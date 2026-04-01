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
		"data JSONB",
		"status TEXT NOT NULL DEFAULT 'PENDING'",
		"is_retry BOOLEAN NOT NULL DEFAULT false",
		"retry_policy JSONB",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_created_at",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_dequeue",
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
		"CREATE TABLE IF NOT EXISTS liteq.queue_configs",
		"CREATE TABLE IF NOT EXISTS liteq.migrations",
		"CREATE TABLE IF NOT EXISTS liteq.schema_versions",
		"batch INTEGER NOT NULL",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
		}
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

	for _, want := range []string{
		"-- Migration: 000_create_schema.v1.up.sql",
		"CREATE SCHEMA IF NOT EXISTS liteq;",
		"CREATE TABLE IF NOT EXISTS liteq.migrations",
		"CREATE TABLE IF NOT EXISTS liteq.schema_versions",
		"CREATE TABLE IF NOT EXISTS liteq.queue_configs",
		"-- Migration: 001_create_queue_table.v1.up.sql",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks_dead_letter",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_dequeue",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("dry-run output missing %q in:\n%s", want, output.String())
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
	// Full rollback of the only batch includes everything.
	if len(got) != 3 {
		t.Fatalf("rollback count = %d, want 3", len(got))
	}
	// Foundation should be last.
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

	// Roll back 1 batch = batch 2 only (partial, foundation stays).
	got := selectRollbackBatches(applied, 1)
	if len(got) != 1 {
		t.Fatalf("rollback count = %d, want 1", len(got))
	}
	if got[0].QueueName != "events" {
		t.Fatalf("got[0] = %#v, want events queue", got[0])
	}

	// Roll back 2 batches = everything (foundation last).
	got = selectRollbackBatches(applied, 2)
	if len(got) != 3 {
		t.Fatalf("rollback count = %d, want 3", len(got))
	}
	if got[2].Name != "000_create_schema" {
		t.Fatalf("last = %#v, want foundation last", got[2])
	}
}

func TestSchemaManager_EnsureSchema_NilPool(t *testing.T) {
	sm := NewSchemaManager(nil)
	err := sm.EnsureSchema(context.Background())
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}
