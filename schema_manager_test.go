package liteq

import (
	"context"
	"strings"
	"testing"
)

func TestNewSchemaManager_DefaultSchema(t *testing.T) {
	sm := NewSchemaManager(nil)
	if sm.schema != defaultSchema {
		t.Fatalf("schema = %q, want %q", sm.schema, defaultSchema)
	}
}

func TestSchemaManager_RenderMigration_QualifiedQueue(t *testing.T) {
	sm := NewSchemaManager(nil)

	sql, skip, err := sm.renderMigration(migrationCreateQueue, "queue_tasks")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if skip {
		t.Fatal("renderMigration unexpectedly skipped queue migration")
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks",
		"id UUID PRIMARY KEY DEFAULT uuid_generate_v4()",
		"data JSONB",
		"status TEXT NOT NULL DEFAULT 'PENDING'",
		"is_retry BOOLEAN NOT NULL DEFAULT false",
		"retry_policy JSONB",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_created_at",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_dequeue",
		"WITH inserted_queue_meta AS (",
		"INSERT INTO liteq.queue_meta (queue_name)",
		"RETURNING queue_name",
		"INSERT INTO liteq.queue_states (queue_name, state, reason)",
		"SELECT queue_name, 'active', 'queue created'",
		"FROM inserted_queue_meta;",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestSchemaManager_RenderMigration_FoundationTables(t *testing.T) {
	sm := NewSchemaManager(nil)

	sql, skip, err := sm.renderMigration(migrationFoundation, "")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if skip {
		t.Fatal("renderMigration unexpectedly skipped foundation migration")
	}
	for _, want := range []string{
		"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
		"CREATE TABLE IF NOT EXISTS liteq.queue_meta",
		"CREATE TABLE IF NOT EXISTS liteq.queue_states",
		"CREATE TABLE IF NOT EXISTS liteq.migrations",
		"CREATE TABLE IF NOT EXISTS liteq.schema_versions",
		"batch INTEGER NOT NULL",
		"CHECK (state IN ('active', 'paused', 'draining'))",
		"BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
		"CREATE INDEX IF NOT EXISTS idx_queue_states_queue",
		"ON liteq.queue_states (queue_name, changed_at DESC);",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestSchemaManager_RenderMigration_QueueStatePlaceholders(t *testing.T) {
	tests := []struct {
		name         string
		schema       string
		file         string
		queueName    string
		upWants      []string
		downWants    []string
		expectUpSkip bool
	}{
		{
			name:      "qualified foundation",
			schema:    "custom",
			file:      migrationFoundation,
			queueName: "",
			upWants: []string{
				"custom.queue_meta",
				"custom.queue_states",
			},
		},
		{
			name:      "unqualified queue migration",
			schema:    "",
			file:      migrationCreateQueue,
			queueName: "queue_tasks",
			upWants: []string{
				"WITH inserted_queue_meta AS (",
				"INSERT INTO queue_meta (queue_name)",
				"INSERT INTO queue_states (queue_name, state, reason)",
				"FROM inserted_queue_meta;",
			},
			downWants: []string{
				"DELETE FROM queue_states WHERE queue_name = 'queue_tasks';",
				"DELETE FROM queue_meta WHERE queue_name = 'queue_tasks';",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewSchemaManager(nil, WithSchemaManagerSchema(tt.schema))

			sql, skip, err := sm.renderMigration(tt.file, tt.queueName)
			if err != nil {
				t.Fatalf("renderMigration returned error: %v", err)
			}
			if skip != tt.expectUpSkip {
				t.Fatalf("renderMigration skip = %v, want %v", skip, tt.expectUpSkip)
			}
			for _, want := range tt.upWants {
				if !strings.Contains(sql, want) {
					t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
				}
			}

			if len(tt.downWants) == 0 {
				return
			}

			downSQL, downSkip, err := sm.renderMigration("001_create_queue_table.v1.down.sql", tt.queueName)
			if err != nil {
				t.Fatalf("renderMigration down returned error: %v", err)
			}
			if downSkip {
				t.Fatal("renderMigration unexpectedly skipped queue down migration")
			}
			for _, want := range tt.downWants {
				if !strings.Contains(downSQL, want) {
					t.Fatalf("rendered down SQL missing %q in:\n%s", want, downSQL)
				}
			}
		})
	}
}

func TestSchemaManager_RenderMigration_SkipsFoundationWhenSchemaEmpty(t *testing.T) {
	sm := NewSchemaManager(nil, WithSchemaManagerSchema(""))

	sql, skip, err := sm.renderMigration(migrationFoundation, "")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if !skip {
		t.Fatal("expected foundation migration to be skipped")
	}
	if sql != "" {
		t.Fatalf("expected empty SQL when skipped, got %q", sql)
	}
}

func TestSchemaManager_RenderMigration_SkipsSchemaDropWhenDisabled(t *testing.T) {
	sm := NewSchemaManager(nil, WithSchemaManagerSchema(""))

	sql, skip, err := sm.renderMigration("000_create_schema.v1.down.sql", "")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if !skip {
		t.Fatal("expected drop schema migration to be skipped")
	}
	if sql != "" {
		t.Fatalf("expected empty SQL when skipped, got %q", sql)
	}
}

func TestSchemaManager_RenderMigration_QualifiedQueueDrop(t *testing.T) {
	sm := NewSchemaManager(nil)

	sql, skip, err := sm.renderMigration("001_create_queue_table.v1.down.sql", "queue_tasks")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if skip {
		t.Fatal("renderMigration unexpectedly skipped queue drop migration")
	}
	for _, want := range []string{
		"DELETE FROM liteq.queue_states WHERE queue_name = 'queue_tasks';",
		"DELETE FROM liteq.queue_meta WHERE queue_name = 'queue_tasks';",
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_created_at;",
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_deleted_at;",
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_dequeue;",
		"DROP TABLE IF EXISTS liteq.queue_tasks;",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestSchemaManager_MigrateDryRun_WritesSQLWithoutPool(t *testing.T) {
	var output strings.Builder
	sm := NewSchemaManager(nil, WithDryRun(&output))

	err := sm.Migrate(context.Background(), []QueueDefinition{{
		Name:    "queue_tasks",
		DLQName: "queue_tasks_dead_letter",
	}})
	if err != nil {
		t.Fatalf("Migrate returned error: %v", err)
	}

	dryRun := output.String()
	for _, want := range []string{
		"-- Migration: 000_create_schema.v1.up.sql",
		"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
		"CREATE SCHEMA IF NOT EXISTS liteq;",
		"CREATE TABLE IF NOT EXISTS liteq.migrations",
		"CREATE TABLE IF NOT EXISTS liteq.schema_versions",
		"CREATE TABLE IF NOT EXISTS liteq.queue_meta",
		"CREATE TABLE IF NOT EXISTS liteq.queue_states",
		"CHECK (state IN ('active', 'paused', 'draining'))",
		"CREATE INDEX IF NOT EXISTS idx_queue_states_queue",
		"-- Migration: 001_create_queue_table.v1.up.sql",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks",
		"id UUID PRIMARY KEY DEFAULT uuid_generate_v4()",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks_dead_letter",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_dequeue",
		"WITH inserted_queue_meta AS (",
		"INSERT INTO liteq.queue_meta (queue_name)",
		"ON CONFLICT (queue_name) DO NOTHING",
		"INSERT INTO liteq.queue_states (queue_name, state, reason)",
		"SELECT queue_name, 'active', 'queue created'",
		"FROM inserted_queue_meta;",
	} {
		if !strings.Contains(dryRun, want) {
			t.Fatalf("dry-run output missing %q in:\n%s", want, dryRun)
		}
	}
}

func TestSchemaManager_MigrateDown_DryRunNilPoolRequiresDatabaseState(t *testing.T) {
	var output strings.Builder
	sm := NewSchemaManager(nil, WithDryRun(&output))

	err := sm.MigrateDown(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for migrate-down dry-run without pool")
	}
	if !strings.Contains(err.Error(), "pool must not be nil for migrate-down dry-run") {
		t.Fatalf("error = %v", err)
	}
}

func TestSchemaManager_MigrateDownAll_NilPool(t *testing.T) {
	sm := NewSchemaManager(nil)
	if err := sm.MigrateDownAll(context.Background()); err == nil {
		t.Fatal("expected error for nil pool")
	}
}

func TestParseMigrationFile(t *testing.T) {
	tests := []struct {
		file        string
		wantName    string
		wantVersion string
		wantErr     bool
	}{
		{"000_create_schema.v1.up.sql", "000_create_schema", "v1", false},
		{"001_create_queue_table.v1.up.sql", "001_create_queue_table", "v1", false},
		{"001_create_queue_table.v2.down.sql", "001_create_queue_table", "v2", false},
		{"no_suffix.sql", "", "", true},
		{"no_version.up.sql", "", "", true},
	}
	for _, tt := range tests {
		name, version, err := parseMigrationFile(tt.file)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseMigrationFile(%q) err = %v, wantErr = %v", tt.file, err, tt.wantErr)
		}
		if name != tt.wantName || version != tt.wantVersion {
			t.Fatalf("parseMigrationFile(%q) = (%q, %q), want (%q, %q)", tt.file, name, version, tt.wantName, tt.wantVersion)
		}
	}
}

func TestMigrationRecord_DownFile(t *testing.T) {
	r := migrationRecord{Name: "001_create_queue_table", Version: "v1"}
	if got := r.downFile(); got != "001_create_queue_table.v1.down.sql" {
		t.Fatalf("downFile() = %q, want %q", got, "001_create_queue_table.v1.down.sql")
	}
}

func TestSelectRollbackBatches_PartialSkipsFoundation(t *testing.T) {
	applied := []migrationRecord{
		{Name: "001_create_queue_table", Version: "v1", QueueName: "tasks_dead_letter", Batch: 1},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "tasks", Batch: 1},
		{Name: "000_create_schema", Version: "v1", QueueName: "", Batch: 1},
	}

	got := selectRollbackBatches(applied, 1)
	if len(got) != 3 {
		t.Fatalf("rollback count = %d, want 3", len(got))
	}
	if got[2].Name != "000_create_schema" {
		t.Fatalf("last = %#v, want foundation last", got[2])
	}
}

func TestSelectRollbackBatches_MultipleBatches(t *testing.T) {
	applied := []migrationRecord{
		{Name: "001_create_queue_table", Version: "v1", QueueName: "events", Batch: 2},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "tasks", Batch: 1},
		{Name: "000_create_schema", Version: "v1", QueueName: "", Batch: 1},
	}

	got := selectRollbackBatches(applied, 1)
	if len(got) != 1 {
		t.Fatalf("rollback count = %d, want 1", len(got))
	}
	if got[0].QueueName != "events" {
		t.Fatalf("got[0] = %#v, want events queue", got[0])
	}

	got = selectRollbackBatches(applied, 2)
	if len(got) != 3 {
		t.Fatalf("rollback count = %d, want 3", len(got))
	}
	if got[2].Name != "000_create_schema" {
		t.Fatalf("last = %#v, want foundation last", got[2])
	}
}

func TestSelectQueueRollbackBatches_ExcludesFoundationAndKeepsBatchPeers(t *testing.T) {
	applied := []migrationRecord{
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q3_dead_letter", Batch: 4},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q3", Batch: 4},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q2_dead_letter", Batch: 3},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q2", Batch: 3},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q1_dead_letter", Batch: 2},
		{Name: "001_create_queue_table", Version: "v1", QueueName: "q1", Batch: 2},
		{Name: "000_create_schema", Version: "v1", QueueName: "", Batch: 1},
	}

	got := selectQueueRollbackBatches(applied, "q1", 1)
	if len(got) != 2 {
		t.Fatalf("rollback count = %d, want 2", len(got))
	}
	for _, record := range got {
		if record.Batch != 2 {
			t.Fatalf("record = %#v, want batch 2", record)
		}
		if record.Name == "000_create_schema" {
			t.Fatalf("record = %#v, foundation should not be included", record)
		}
	}
}

func TestSchemaManager_NormalizeQueueTargets_DisableDLQSkipsDefault(t *testing.T) {
	sm := NewSchemaManager(nil)

	targets, err := sm.normalizeQueueTargets([]QueueDefinition{{
		Name:       "queue_tasks",
		DisableDLQ: true,
	}})
	if err != nil {
		t.Fatalf("normalizeQueueTargets returned error: %v", err)
	}

	if len(targets) != 1 || targets[0] != "queue_tasks" {
		t.Fatalf("targets = %#v, want only queue_tasks", targets)
	}
}

func TestSchemaManager_NormalizeQueueTargets_RejectsDisableDLQWithExplicitDLQ(t *testing.T) {
	sm := NewSchemaManager(nil)

	_, err := sm.normalizeQueueTargets([]QueueDefinition{{
		Name:       "queue_tasks",
		DLQName:    "queue_tasks_dlq",
		DisableDLQ: true,
	}})
	if err == nil {
		t.Fatal("expected error for disable dlq with explicit dlq")
	}
	if !strings.Contains(err.Error(), "disable dlq cannot both be set") {
		t.Fatalf("error = %v", err)
	}
}

func TestSchemaManager_MigrateDownQueueAll_PreservesFoundationAndOtherQueues(t *testing.T) {
	pool, schema := newIntegrationTestPool(t)
	ctx := context.Background()
	manager := NewSchemaManager(pool, WithSchemaManagerSchema(schema))

	if err := manager.EnsureQueue(ctx, "q1", ""); err != nil {
		t.Fatalf("EnsureQueue(q1): %v", err)
	}
	if err := manager.EnsureQueue(ctx, "q2", ""); err != nil {
		t.Fatalf("EnsureQueue(q2): %v", err)
	}

	if err := manager.MigrateDownQueueAll(ctx, "q1"); err != nil {
		t.Fatalf("MigrateDownQueueAll(q1): %v", err)
	}

	for _, relation := range []string{
		qualifyIdentifier(schema, "queue_meta"),
		qualifyIdentifier(schema, "queue_states"),
		qualifyIdentifier(schema, "migrations"),
		qualifyIdentifier(schema, "schema_versions"),
		qualifyIdentifier(schema, "q2"),
	} {
		var regclass *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", relation).Scan(&regclass); err != nil {
			t.Fatalf("to_regclass(%q): %v", relation, err)
		}
		if regclass == nil {
			t.Fatalf("relation %q should still exist", relation)
		}
	}

	for _, relation := range []string{
		qualifyIdentifier(schema, "q1"),
		qualifyIdentifier(schema, "q1_dead_letter"),
	} {
		var regclass *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", relation).Scan(&regclass); err != nil {
			t.Fatalf("to_regclass(%q): %v", relation, err)
		}
		if regclass != nil {
			t.Fatalf("relation %q should have been removed, got %q", relation, *regclass)
		}
	}

	if got := countRowsForQueue(t, pool, qualifyIdentifier(schema, "queue_meta"), "q2"); got != 1 {
		t.Fatalf("queue_meta rows for q2 = %d, want 1", got)
	}
}

func TestSchemaManager_EnsureSchema_NilPool(t *testing.T) {
	sm := NewSchemaManager(nil)
	err := sm.EnsureSchema(context.Background())
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}
