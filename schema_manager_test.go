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
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("rendered SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestSchemaManager_RenderMigration_UnqualifiedSchema(t *testing.T) {
	sm := NewSchemaManager(nil, WithSchemaManagerSchema(""))

	sql, skip, err := sm.renderMigration(migrationQueueConfigs, "")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if skip {
		t.Fatal("renderMigration unexpectedly skipped queue_configs migration")
	}
	if strings.Contains(sql, "liteq.") {
		t.Fatalf("expected unqualified SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS queue_configs") {
		t.Fatalf("expected queue_configs table in SQL, got:\n%s", sql)
	}
}

func TestSchemaManager_RenderMigration_SkipsSchemaCreateWhenDisabled(t *testing.T) {
	sm := NewSchemaManager(nil, WithSchemaManagerSchema(""))

	sql, skip, err := sm.renderMigration(migrationCreateSchema, "")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if !skip {
		t.Fatal("expected create schema migration to be skipped")
	}
	if sql != "" {
		t.Fatalf("expected empty SQL when skipped, got %q", sql)
	}
}

func TestSchemaManager_RenderMigration_SkipsSchemaDropWhenDisabled(t *testing.T) {
	sm := NewSchemaManager(nil, WithSchemaManagerSchema(""))

	sql, skip, err := sm.renderMigration("001_create_schema.down.sql", "")
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

func TestSchemaManager_RenderMigration_QualifiedIndexDrop(t *testing.T) {
	sm := NewSchemaManager(nil)

	sql, skip, err := sm.renderMigration("004_indexes.down.sql", "queue_tasks")
	if err != nil {
		t.Fatalf("renderMigration returned error: %v", err)
	}
	if skip {
		t.Fatal("renderMigration unexpectedly skipped index drop migration")
	}
	for _, want := range []string{
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_created_at;",
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_deleted_at;",
		"DROP INDEX IF EXISTS liteq.idx_queue_tasks_dequeue;",
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
		"-- Migration: 001_create_schema.up.sql",
		"CREATE SCHEMA IF NOT EXISTS liteq;",
		"-- Migration: 002_create_queue_table.up.sql",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks",
		"CREATE TABLE IF NOT EXISTS liteq.queue_tasks_dead_letter",
		"-- Migration: 004_indexes.up.sql",
		"CREATE INDEX IF NOT EXISTS idx_queue_tasks_dequeue",
		"-- Migration: 005_schema_versions.up.sql",
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

func TestDownMigrationFile(t *testing.T) {
	file, err := downMigrationFile(migrationCreateIndexes)
	if err != nil {
		t.Fatalf("downMigrationFile returned error: %v", err)
	}
	if file != "004_indexes.down.sql" {
		t.Fatalf("file = %q, want %q", file, "004_indexes.down.sql")
	}
}

func TestSelectRollbackRecords_SkipsTrackingForPartialRollback(t *testing.T) {
	applied := []versionRecord{
		{Version: migrationSchemaVersions},
		{Version: migrationCreateIndexes, QueueName: "tasks_dead_letter"},
		{Version: migrationCreateIndexes, QueueName: "tasks"},
		{Version: migrationQueueConfigs},
		{Version: migrationCreateQueue, QueueName: "tasks_dead_letter"},
		{Version: migrationCreateQueue, QueueName: "tasks"},
		{Version: migrationCreateSchema},
	}

	got := selectRollbackRecords(applied, 1)
	if len(got) != 1 {
		t.Fatalf("rollback count = %d, want 1", len(got))
	}
	if got[0].Version != migrationCreateIndexes || got[0].QueueName != "tasks_dead_letter" {
		t.Fatalf("got[0] = %#v", got[0])
	}
}

func TestSelectRollbackRecords_FullRollbackPreservesOrder(t *testing.T) {
	applied := []versionRecord{
		{Version: migrationSchemaVersions},
		{Version: migrationCreateIndexes, QueueName: "tasks"},
		{Version: migrationQueueConfigs},
	}

	got := selectRollbackRecords(applied, 10)
	if len(got) != 3 {
		t.Fatalf("rollback count = %d, want 3", len(got))
	}
	if got[0].Version != migrationSchemaVersions {
		t.Fatalf("first rollback = %#v, want schema_versions first (reverse-apply order)", got[0])
	}
	if got[2].Version != migrationQueueConfigs {
		t.Fatalf("last rollback = %#v, want queue_configs last", got[2])
	}
}

func TestSchemaManager_EnsureSchema_NilPool(t *testing.T) {
	sm := NewSchemaManager(nil)
	err := sm.EnsureSchema(context.Background())
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}
