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
	migrationFoundation  = "000_create_schema.v1.up.sql"
	migrationCreateQueue = "001_create_queue_table.v1.up.sql"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

const maxRollbackSteps = int(^uint(0) >> 1)

// QueueDefinition specifies a queue and its optional dead-letter queue for migrations.
type QueueDefinition struct {
	Name    string
	DLQName string
}

// SchemaManager handles database migrations for liteq queue tables.
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

// SchemaManagerOption configures a SchemaManager at construction time.
type SchemaManagerOption func(*schemaManagerConfig)

// WithSchemaManagerSchema sets the target Postgres schema for migrations.
func WithSchemaManagerSchema(schema string) SchemaManagerOption {
	return func(c *schemaManagerConfig) {
		c.schema = schema
	}
}

// WithDryRun enables dry-run mode, writing SQL to w instead of executing it.
func WithDryRun(w io.Writer) SchemaManagerOption {
	return func(c *schemaManagerConfig) {
		c.dryRun = true
		if w == nil {
			w = io.Discard
		}
		c.dryRunWriter = w
	}
}

// NewSchemaManager creates a SchemaManager with the given pool and options.
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

// Migrate applies all pending up-migrations for the given queue definitions.
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

	applied, migrationsExist, err := sm.loadAppliedMigrations(ctx, tx)
	if err != nil {
		return err
	}

	batch := 1
	if migrationsExist {
		batch, err = sm.nextBatch(ctx, tx)
		if err != nil {
			return err
		}
	}

	pending := make([]migrationRecord, 0, len(steps))

	for idx, step := range steps {
		key := step.migrationKey()
		alreadyApplied := false
		if migrationsExist {
			_, alreadyApplied = applied[key]
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

		record := migrationRecord{
			Name:      step.name,
			Version:   step.version,
			QueueName: step.queueName,
			Batch:     batch,
		}
		if migrationsExist {
			if alreadyApplied {
				continue
			}
			if err := sm.recordMigration(ctx, tx, record); err != nil {
				return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
			}
			if err := sm.recordSchemaVersion(ctx, tx, record); err != nil {
				return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
			}
			applied[key] = struct{}{}
			continue
		}

		pending = append(pending, record)
		if step.file == migrationFoundation {
			migrationsExist = true
			for _, pendingRecord := range pending {
				if err := sm.recordMigration(ctx, tx, pendingRecord); err != nil {
					return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
				}
				if err := sm.recordSchemaVersion(ctx, tx, pendingRecord); err != nil {
					return fmt.Errorf("schema manager: migration %s step %d: %w", step.file, idx+1, err)
				}
				applied[pendingRecord.migrationKey()] = struct{}{}
			}
			pending = pending[:0]
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("schema manager: commit migration tx: %w", err)
	}
	return nil
}

// MigrateDown rolls back the given number of migration batches.
func (sm *SchemaManager) MigrateDown(ctx context.Context, steps int) error {
	if err := sm.validate(ctx); err != nil {
		return err
	}
	if steps <= 0 {
		return fmt.Errorf("schema manager: steps must be greater than zero")
	}
	if sm.pool == nil {
		if sm.dryRun {
			return fmt.Errorf("schema manager: pool must not be nil for migrate-down dry-run; applied migrations must be discovered from the migrations table")
		}
		return fmt.Errorf("schema manager: pool must not be nil")
	}

	tx, err := sm.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("schema manager: begin rollback tx: %w", err)
	}
	defer rollback(tx)

	records, migrationsExist, err := sm.loadAppliedMigrationRecords(ctx, tx)
	if err != nil {
		return err
	}
	if !migrationsExist || len(records) == 0 {
		if sm.dryRun {
			return nil
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("schema manager: commit rollback tx: %w", err)
		}
		return nil
	}

	plan := selectRollbackBatches(records, steps)
	migrationsTableDropped := false
	for idx, record := range plan {
		file := record.downFile()

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
		if record.Name == "000_create_schema" {
			migrationsTableDropped = true
			continue
		}
		if migrationsTableDropped {
			continue
		}
		if err := sm.deleteMigration(ctx, tx, record); err != nil {
			return fmt.Errorf("schema manager: migration %s step %d: %w", file, idx+1, err)
		}
		if err := sm.deleteSchemaVersion(ctx, tx, record); err != nil {
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

// MigrateDownAll rolls back all applied migration batches.
func (sm *SchemaManager) MigrateDownAll(ctx context.Context) error {
	return sm.MigrateDown(ctx, maxRollbackSteps)
}

// EnsureSchema applies only the foundation migration (schema + migrations table).
func (sm *SchemaManager) EnsureSchema(ctx context.Context) error {
	return sm.Migrate(ctx, nil)
}

// EnsureQueue applies migrations for a single queue and its optional DLQ.
func (sm *SchemaManager) EnsureQueue(ctx context.Context, name, dlqName string) error {
	return sm.Migrate(ctx, []QueueDefinition{{Name: name, DLQName: dlqName}})
}

func (sm *SchemaManager) migrationSteps(targets []string) []migrationStep {
	foundationName, foundationVersion, _ := parseMigrationFile(migrationFoundation)
	steps := make([]migrationStep, 0, 1+len(targets))
	steps = append(steps, migrationStep{
		file: migrationFoundation, name: foundationName, version: foundationVersion,
	})

	queueName, queueVersion, _ := parseMigrationFile(migrationCreateQueue)
	for _, target := range targets {
		steps = append(steps, migrationStep{
			file: migrationCreateQueue, name: queueName, version: queueVersion, queueName: target,
		})
	}
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

func (sm *SchemaManager) renderMigration(file, queueName string) (sql string, skip bool, err error) {
	raw, err := schemaFS.ReadFile(filepath.ToSlash(filepath.Join("schema", file)))
	if err != nil {
		return "", false, fmt.Errorf("read embedded SQL %s: %w", file, err)
	}
	if sm.schema == "" && strings.HasPrefix(file, "000_create_schema.") {
		return "", true, nil
	}

	replacer := strings.NewReplacer(
		"{{schema_name}}", sm.schema,
		"{{queue_name}}", queueName,
		"{{qualified_queue_name}}", qualifyIdentifier(sm.schema, queueName),
		"{{qualified_queue_meta_name}}", qualifyIdentifier(sm.schema, "queue_meta"),
		"{{qualified_queue_states_name}}", qualifyIdentifier(sm.schema, "queue_states"),
		"{{qualified_migrations_name}}", qualifyIdentifier(sm.schema, "migrations"),
		"{{qualified_schema_versions_name}}", qualifyIdentifier(sm.schema, "schema_versions"),
		"{{qualified_index_created_at_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_created_at"),
		"{{qualified_index_deleted_at_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_deleted_at"),
		"{{qualified_index_dequeue_name}}", qualifyIdentifier(sm.schema, "idx_"+queueName+"_dequeue"),
	)
	return replacer.Replace(string(raw)), false, nil
}

// loadAppliedMigrations returns a set of migration keys for dedup during Migrate.
func (sm *SchemaManager) loadAppliedMigrations(ctx context.Context, tx pgx.Tx) (applied map[string]struct{}, tableExists bool, err error) {
	records, exists, err := sm.loadAppliedMigrationRecords(ctx, tx)
	if err != nil {
		return nil, false, err
	}

	applied = make(map[string]struct{}, len(records))
	for _, record := range records {
		applied[record.migrationKey()] = struct{}{}
	}
	return applied, exists, nil
}

// loadAppliedMigrationRecords reads all rows from the migrations table
// ordered for rollback (highest batch first, then reverse name/queue order).
func (sm *SchemaManager) loadAppliedMigrationRecords(ctx context.Context, tx pgx.Tx) ([]migrationRecord, bool, error) {
	var relation *string
	if err := tx.QueryRow(ctx, "SELECT to_regclass($1)", qualifyIdentifier(sm.schema, "migrations")).Scan(&relation); err != nil {
		return nil, false, fmt.Errorf("schema manager: inspect migrations: %w", err)
	}
	if relation == nil {
		return nil, false, nil
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT name, version, queue_name, batch FROM %s ORDER BY batch DESC, name DESC, queue_name DESC",
		qualifyIdentifier(sm.schema, "migrations"),
	))
	if err != nil {
		return nil, false, fmt.Errorf("schema manager: load applied migrations: %w", err)
	}
	defer rows.Close()

	records := make([]migrationRecord, 0)
	for rows.Next() {
		var r migrationRecord
		if err := rows.Scan(&r.Name, &r.Version, &r.QueueName, &r.Batch); err != nil {
			return nil, false, fmt.Errorf("schema manager: scan applied migration: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("schema manager: iterate applied migrations: %w", err)
	}

	return records, true, nil
}

func (sm *SchemaManager) nextBatch(ctx context.Context, tx pgx.Tx) (int, error) {
	var maxBatch *int
	err := tx.QueryRow(ctx, fmt.Sprintf(
		"SELECT MAX(batch) FROM %s",
		qualifyIdentifier(sm.schema, "migrations"),
	)).Scan(&maxBatch)
	if err != nil {
		return 0, fmt.Errorf("schema manager: read max batch: %w", err)
	}
	if maxBatch == nil {
		return 1, nil
	}
	return *maxBatch + 1, nil
}

func (sm *SchemaManager) recordMigration(ctx context.Context, tx pgx.Tx, record migrationRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (name, version, queue_name, batch) VALUES ($1, $2, $3, $4) ON CONFLICT (name, version, queue_name) DO NOTHING",
			qualifyIdentifier(sm.schema, "migrations"),
		),
		record.Name, record.Version, record.QueueName, record.Batch,
	)
	if err != nil {
		return fmt.Errorf("record migration %s.%s: %w", record.Name, record.Version, err)
	}
	return nil
}

func (sm *SchemaManager) recordSchemaVersion(ctx context.Context, tx pgx.Tx, record migrationRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (queue_name, version) VALUES ($1, $2) ON CONFLICT (queue_name) DO UPDATE SET version = EXCLUDED.version, applied_at = NOW()",
			qualifyIdentifier(sm.schema, "schema_versions"),
		),
		record.QueueName, record.Version,
	)
	if err != nil {
		return fmt.Errorf("record schema version %s for %q: %w", record.Version, record.QueueName, err)
	}
	return nil
}

func (sm *SchemaManager) deleteMigration(ctx context.Context, tx pgx.Tx, record migrationRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf("DELETE FROM %s WHERE name = $1 AND version = $2 AND queue_name = $3",
			qualifyIdentifier(sm.schema, "migrations")),
		record.Name, record.Version, record.QueueName,
	)
	if err != nil {
		return fmt.Errorf("delete migration %s.%s: %w", record.Name, record.Version, err)
	}
	return nil
}

func (sm *SchemaManager) deleteSchemaVersion(ctx context.Context, tx pgx.Tx, record migrationRecord) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf("DELETE FROM %s WHERE queue_name = $1",
			qualifyIdentifier(sm.schema, "schema_versions")),
		record.QueueName,
	)
	if err != nil {
		return fmt.Errorf("delete schema version for %q: %w", record.QueueName, err)
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

// parseMigrationFile extracts the name and version from a migration filename.
// "000_create_schema.v1.up.sql" → ("000_create_schema", "v1")
func parseMigrationFile(file string) (name, version string, err error) {
	base := file
	switch {
	case strings.HasSuffix(base, ".up.sql"):
		base = strings.TrimSuffix(base, ".up.sql")
	case strings.HasSuffix(base, ".down.sql"):
		base = strings.TrimSuffix(base, ".down.sql")
	default:
		return "", "", fmt.Errorf("migration file %q has no recognized suffix", file)
	}

	lastDot := strings.LastIndex(base, ".")
	if lastDot < 0 {
		return "", "", fmt.Errorf("migration file %q has no version segment", file)
	}

	name = base[:lastDot]
	version = base[lastDot+1:]
	if name == "" || version == "" {
		return "", "", fmt.Errorf("migration file %q has empty name or version", file)
	}

	return name, version, nil
}

// selectRollbackBatches selects migration records to roll back, grouped by batch.
// steps is the number of batches to roll back. Records are returned in
// reverse-apply order (highest batch first). The foundation migration
// (000_create_schema) is only included in a full rollback.
func selectRollbackBatches(applied []migrationRecord, steps int) []migrationRecord {
	if steps <= 0 || len(applied) == 0 {
		return nil
	}

	// Collect distinct batches in descending order (applied is already sorted batch DESC).
	seen := make(map[int]struct{})
	batches := make([]int, 0)
	for _, r := range applied {
		if _, ok := seen[r.Batch]; !ok {
			seen[r.Batch] = struct{}{}
			batches = append(batches, r.Batch)
		}
	}

	// Determine which batches to include.
	batchCount := steps
	if batchCount > len(batches) {
		batchCount = len(batches)
	}
	rollbackBatches := make(map[int]struct{}, batchCount)
	for _, b := range batches[:batchCount] {
		rollbackBatches[b] = struct{}{}
	}

	// Separate foundation from non-foundation records.
	var foundation []migrationRecord
	nonFoundation := make([]migrationRecord, 0, len(applied))
	for _, r := range applied {
		if _, ok := rollbackBatches[r.Batch]; !ok {
			continue
		}
		if r.Name == "000_create_schema" {
			foundation = append(foundation, r)
		} else {
			nonFoundation = append(nonFoundation, r)
		}
	}

	// Partial rollback: skip foundation so the migrations table stays intact.
	if batchCount < len(batches) {
		return nonFoundation
	}

	// Full rollback: non-foundation first, then foundation last.
	return append(nonFoundation, foundation...)
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
	name      string
	version   string
	queueName string
}

func (m migrationStep) migrationKey() string {
	return m.name + "|" + m.version + "|" + m.queueName
}

type migrationRecord struct {
	Name      string
	Version   string
	QueueName string
	Batch     int
}

func (r migrationRecord) migrationKey() string {
	return r.Name + "|" + r.Version + "|" + r.QueueName
}

func (r migrationRecord) downFile() string {
	return r.Name + "." + r.Version + ".down.sql"
}
