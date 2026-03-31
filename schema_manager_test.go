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
		"meta JSONB",
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

func TestSchemaManager_EnsureSchema_NilPool(t *testing.T) {
	sm := NewSchemaManager(nil)
	err := sm.EnsureSchema(context.Background())
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}
