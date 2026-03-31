package liteq

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	migrationCreateSchema   = "001_create_schema.up.sql"
	migrationCreateQueue    = "002_create_queue_table.up.sql"
	migrationQueueConfigs   = "003_queue_configs.up.sql"
	migrationCreateIndexes  = "004_indexes.up.sql"
	migrationSchemaVersions = "005_schema_versions.up.sql"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

const maxRollbackSteps = int(^uint(0) >> 1)

type QueueDefinition struct {
	Name    string
	DLQName string
}

type SchemaManager struct {
	pool         *pgxpool.Pool
	schema       string
	dryRun       bool
	dryRunWriter io.Writer
}

type schemaManagerConfig struct {
	schema       string
	dryRun       bool
	dryRunWriter io.Writer
}

type SchemaManagerOption func(*schemaManagerConfig)

func WithSchemaManagerSchema(schema string) SchemaManagerOption {
	return func(c *schemaManagerConfig) {
		c.schema = schema
	}
}

func WithDryRun(w io.Writer) SchemaManagerOption {
	return func(c *schemaManagerConfig) {
		c.dryRun = true
		if w == nil {
			w = io.Discard
		}
		c.dryRunWriter = w
	}
}

func NewSchemaManager(pool *pgxpool.Pool, opts ...SchemaManagerOption) *SchemaManager {
	cfg := schemaManagerConfig{
		schema:       defaultSchema,
		dryRunWriter: io.Discard,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return &SchemaManager{
		pool:         pool,
		schema:       cfg.schema,
		dryRun:       cfg.dryRun,
		dryRunWriter: cfg.dryRunWriter,
	}
}

func (sm *SchemaManager) Migrate(ctx context.Context, queues []QueueDefinition) error {
	if err := sm.validate(ctx); err != nil {
		return err
	}

	targets, err := sm.normalizeQueueTargets(queues)
	if err != nil {
		return err
	}

	steps := sm.migrationSteps(targets)
	if sm.dryRun {
		return sm.writeDryRunPlan(steps)
	}
	if sm.pool == nil {
		return fmt.Errorf("schema manager: pool must not be nil")
	}

	tx, err := sm.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("schema manager: begin migration tx: %w", err)
	}
	defer rollback(tx)

	applied, versionsExist, err := sm.loadAppliedVersions(ctx, tx)
	if err != nil {
		return err
	}

	pending := make([]versionRecord, 0, len(steps))

	for idx, step := range steps {
		key := step.versionKey()
		if versionsExist {
			if _, ok := applied[key]; ok {
				continue
			}
		}

		sql, skip, err := sm.renderMigration(step.file, step.queueName)
		if err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
		}
		if skip || strings.TrimSpace(sql) == "" {
			continue
		}

		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
		}

		record := versionRecord{Version: step.file, QueueName: step.queueName}
		if versionsExist {
			if err := sm.recordVersion(ctx, tx, record); err != nil {
				return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
			}
			applied[key] = struct{}{}
			continue
		}

		pending = append(pending, record)
		if step.file == migrationSchemaVersions {
			versionsExist = true
			for _, pendingRecord := range pending {
				if err := sm.recordVersion(ctx, tx, pendingRecord); err != nil {
					return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
				}
				applied[pendingRecord.versionKey()] = struct{}{}
			}
			pending = pending[:0]
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("schema manager: commit migration tx: %w", err)
	}
	return nil
}

func (sm *SchemaManager) MigrateDown(ctx context.Context, steps int) error {
	if err := sm.validate(ctx); err != nil {
		return err
	}
	if steps <= 0 {
		return fmt.Errorf("schema manager: steps must be greater than zero")
	}
	if sm.pool == nil {
		if sm.dryRun {
			return fmt.Errorf("schema manager: pool must not be nil for migrate-down dry-run; applied migrations must be discovered from schema_versions")
		}
		return fmt.Errorf("schema manager: pool must not be nil")
	}

	tx, err := sm.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("schema manager: begin rollback tx: %w", err)
	}
	defer rollback(tx)

	applied, versionsExist, err := sm.loadAppliedVersionRecords(ctx, tx)
	if err != nil {
		return err
	}
	if !versionsExist || len(applied) == 0 {
		if sm.dryRun {
			return nil
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("schema manager: commit rollback tx: %w", err)
		}
		return nil
	}

	plan := selectRollbackRecords(applied, steps)
	versionsTableDropped := false
	for idx, record := range plan {
		file, err := downMigrationFile(record.Version)
		if err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", record.Version, idx+1, err)
		}

		sql, skip, err := sm.renderMigration(file, record.QueueName)
		if err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", file, idx+1, err)
		}
		if skip || strings.TrimSpace(sql) == "" {
			continue
		}

		if sm.dryRun {
			if err := sm.writeDryRunMigration(file, sql); err != nil {
				return fmt.Errorf("schema manager: migration %s step %d: %w", file, idx+1, err)
			}
			continue
		}

		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", file, idx+1, err)
		}
		if record.Version == migrationSchemaVersions {
			versionsTableDropped = true
			continue
		}
		if versionsTableDropped {
			continue
		}
		if err := sm.deleteVersion(ctx, tx, record); err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", file, idx+1, err)
		}
	}

	if sm.dryRun {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("schema manager: commit rollback tx: %w", err)
	}
	return nil
}

func (sm *SchemaManager) MigrateDownAll(ctx context.Context) error {
	return sm.MigrateDown(ctx, maxRollbackSteps)
}

func (sm *SchemaManager) EnsureSchema(ctx context.Context) error {
	return sm.Migrate(ctx, nil)
}

func (sm *SchemaManager) EnsureQueue(ctx context.Context, name, dlqName string) error {
	return sm.Migrate(ctx, []QueueDefinition{{Name: name, DLQName: dlqName}})
}

func (sm *SchemaManager) migrationSteps(targets []string) []migrationStep {
	steps := []migrationStep{{file: migrationCreateSchema}}
	for _, target := range targets {
		steps = append(steps, migrationStep{file: migrationCreateQueue, queueName: target})
	}
	steps = append(steps, migrationStep{file: migrationQueueConfigs})
	for _, target := range targets {
		steps = append(steps, migrationStep{file: migrationCreateIndexes, queueName: target})
	}
	steps = append(steps, migrationStep{file: migrationSchemaVersions})
	return steps
}

func (sm *SchemaManager) normalizeQueueTargets(queues []QueueDefinition) ([]string, error) {
	seen := make(map[string]struct{})
	targets := make([]string, 0, len(queues)*2)

	for _, queue := range queues {
		if queue.Name == "" {
			continue
		}
		if err := validateIdentifier("queue name", queue.Name); err != nil {
			return nil, fmt.Errorf("schema manager: %w", err)
		}

		dlqName := queue.DLQName
		if dlqName == "" {
			dlqName = defaultDeadLetterQueueName(queue.Name)
		}
		if dlqName != "" {
			if err := validateIdentifier("dlq name", dlqName); err != nil {
				return nil, fmt.Errorf("schema manager: %w", err)
			}
			if dlqName == queue.Name {
				return nil, fmt.Errorf("schema manager: dlq name %q must differ from queue name", dlqName)
			}
		}

		for _, name := range []string{queue.Name, dlqName} {
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			targets = append(targets, name)
		}
	}

	return targets, nil
}

func (sm *SchemaManager) renderMigration(file, queueName string) (string, bool, error) {
	raw, err := schemaFS.ReadFile(filepath.ToSlash(filepath.Join("schema", file)))
	if err != nil {
		return "", false, fmt.Errorf("read embedded SQL %s: %w", file, err)
	}
	if sm.schema == "" && strings.HasPrefix(file, "001_create_schema.") {
		return "", true, nil
	}

	replacer := strings.NewReplacer(
		"{{schema_name}}", sm.schema,
		"{{queue_name}}", queueName,
		"{{qualified_queue_name}}", qualifyIdentifier(sm.schema, queueName),
		"{{qualified_queue_configs_name}}", qualifyIdentifier(sm.schema, "queue_configs"),
		"{{qualified_schema_versions_name}}", qualifyIdentifier(sm.schema, "schema_versions"),
		"{{qualified_index_created_at_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_created_at"),
		"{{qualified_index_deleted_at_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_deleted_at"),
		"{{qualified_index_dequeue_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_dequeue"),
	)
	return replacer.Replace(string(raw)), false, nil
}

func (sm *SchemaManager) loadAppliedVersions(ctx context.Context, tx pgx.Tx) (map[string]struct{}, bool, error) {
	records, exists, err := sm.loadAppliedVersionRecords(ctx, tx)
	if err != nil {
		return nil, false, err
	}

	applied := make(map[string]struct{}, len(records))
	for _, record := range records {
		applied[record.versionKey()] = struct{}{}
	}
	return applied, exists, nil
}

func (sm *SchemaManager) loadAppliedVersionRecords(ctx context.Context, tx pgx.Tx) ([]versionRecord, bool, error) {
	var relation *string
	if err := tx.QueryRow(ctx, "SELECT to_regclass($1)", qualifyIdentifier(sm.schema, "schema_versions")).Scan(&relation); err != nil {
		return nil, false, fmt.Errorf("schema manager: inspect schema_versions: %w", err)
	}
	if relation == nil {
		return nil, false, nil
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT version, queue_name FROM %s ORDER BY applied_at DESC, version DESC, queue_name DESC",
		qualifyIdentifier(sm.schema, "schema_versions"),
	))
	if err != nil {
		return nil, false, fmt.Errorf("schema manager: load applied versions: %w", err)
	}
	defer rows.Close()

	records := make([]versionRecord, 0)
	for rows.Next() {
		var version, queueName string
		if err := rows.Scan(&version, &queueName); err != nil {
			return nil, false, fmt.Errorf("schema manager: scan applied version: %w", err)
		}
		records = append(records, versionRecord{Version: version, QueueName: queueName})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("schema manager: iterate applied versions: %w", err)
	}

	return records, true, nil
}

func (sm *SchemaManager) recordVersion(ctx context.Context, tx pgx.Tx, record versionRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf("INSERT INTO %s (version, queue_name) VALUES ($1, $2) ON CONFLICT (version, queue_name) DO NOTHING",
			qualifyIdentifier(sm.schema, "schema_versions")),
		record.Version,
		record.QueueName,
	)
	if err != nil {
		return fmt.Errorf("record applied migration %s: %w", record.Version, err)
	}
	return nil
}

func (sm *SchemaManager) deleteVersion(ctx context.Context, tx pgx.Tx, record versionRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf("DELETE FROM %s WHERE version = $1 AND queue_name = $2",
			qualifyIdentifier(sm.schema, "schema_versions")),
		record.Version,
		record.QueueName,
	)
	if err != nil {
		return fmt.Errorf("delete applied migration %s: %w", record.Version, err)
	}
	return nil
}

func (sm *SchemaManager) validate(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("schema manager: ctx must not be nil")
	}
	if sm.schema != "" {
		if err := validateIdentifier("schema", sm.schema); err != nil {
			return fmt.Errorf("schema manager: %w", err)
		}
	}
	return nil
}

func (sm *SchemaManager) writeDryRunPlan(steps []migrationStep) error {
	for idx, step := range steps {
		sql, skip, err := sm.renderMigration(step.file, step.queueName)
		if err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
		}
		if skip || strings.TrimSpace(sql) == "" {
			continue
		}
		if err := sm.writeDryRunMigration(step.file, sql); err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
		}
	}
	return nil
}

func (sm *SchemaManager) writeDryRunMigration(file, sql string) error {
	writer := sm.dryRunWriter
	if writer == nil {
		writer = io.Discard
	}
	if !strings.HasSuffix(sql, "\n") {
		sql += "\n"
	}
	if _, err := fmt.Fprintf(writer, "-- Migration: %s\n%s\n", file, sql); err != nil {
		return fmt.Errorf("write dry-run migration %s: %w", file, err)
	}
	return nil
}

func downMigrationFile(version string) (string, error) {
	if !strings.HasSuffix(version, ".up.sql") {
		return "", fmt.Errorf("migration %q does not have .up.sql suffix", version)
	}
	return strings.TrimSuffix(version, ".up.sql") + ".down.sql", nil
}

func selectRollbackRecords(applied []versionRecord, steps int) []versionRecord {
	if steps <= 0 || len(applied) == 0 {
		return nil
	}

	// Separate tracking (schema_versions) from non-tracking records,
	// preserving the original reverse-apply order.
	nonTracking := make([]versionRecord, 0, len(applied))
	for _, record := range applied {
		if record.Version != migrationSchemaVersions {
			nonTracking = append(nonTracking, record)
		}
	}

	// Only tracking records exist.
	if len(nonTracking) == 0 {
		if steps > len(applied) {
			steps = len(applied)
		}
		return append([]versionRecord(nil), applied[:steps]...)
	}

	// Partial rollback: only revert non-tracking records so the
	// schema_versions table stays intact for future operations.
	if steps < len(nonTracking) {
		return append([]versionRecord(nil), nonTracking[:steps]...)
	}

	// Full rollback: return all records in their original reverse-apply
	// order.  schema_versions was applied last so it appears first in the
	// list and gets dropped before the objects it tracks.
	return append([]versionRecord(nil), applied...)
}

func defaultDeadLetterQueueName(name string) string {
	if strings.HasSuffix(name, "_dead_letter") {
		return ""
	}
	return name + "_dead_letter"
}

func qualifyIdentifier(schema, name string) string {
	if schema == "" {
		return name
	}
	if name == "" {
		return schema
	}
	return schema + "." + name
}

func validateIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q contains invalid characters; only letters, numbers, and underscores are allowed", kind, value)
	}
	return nil
}

type migrationStep struct {
	file      string
	queueName string
}

func (m migrationStep) versionKey() string {
	return versionRecord{Version: m.file, QueueName: m.queueName}.versionKey()
}

type versionRecord struct {
	Version   string
	QueueName string
}

func (v versionRecord) versionKey() string {
	return v.Version + "|" + v.QueueName
}
