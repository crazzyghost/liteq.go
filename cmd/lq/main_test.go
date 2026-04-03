package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	liteq "liteq.go"
)

type stubMigrator struct {
	migrateFn        func(context.Context, []liteq.QueueDefinition) error
	migrateDownFn    func(context.Context, int) error
	migrateDownAllFn func(context.Context) error
}

func (s *stubMigrator) Migrate(ctx context.Context, queues []liteq.QueueDefinition) error {
	if s.migrateFn != nil {
		return s.migrateFn(ctx, queues)
	}

	return nil
}

func (s *stubMigrator) MigrateDown(ctx context.Context, steps int) error {
	if s.migrateDownFn != nil {
		return s.migrateDownFn(ctx, steps)
	}

	return nil
}

func (s *stubMigrator) MigrateDownAll(ctx context.Context) error {
	if s.migrateDownAllFn != nil {
		return s.migrateDownAllFn(ctx)
	}

	return nil
}

func TestVersion_SubcommandVersion(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"version"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	want := "lq dev\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestVersion_FlagVersion(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--version"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	want := "lq dev\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestVersion_InjectedVersion(t *testing.T) {
	orig := version
	version = "v1.2.3"
	t.Cleanup(func() { version = orig })

	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"version"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	want := "lq v1.2.3\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRun_MigrateUpDryRunDoesNotRequireDatabaseURL(t *testing.T) {
	origNewPool := newPool
	origNewSchemaMigrator := newSchemaMigrator

	t.Cleanup(func() {
		newPool = origNewPool
		newSchemaMigrator = origNewSchemaMigrator
	})

	newPoolCalled := false
	newPool = func(context.Context, string) (*pgxpool.Pool, error) {
		newPoolCalled = true
		return nil, nil
	}

	var gotQueues []liteq.QueueDefinition

	var gotOpts int

	newSchemaMigrator = func(pool *pgxpool.Pool, opts ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil pool for dry-run migrate-up, got %#v", pool)
		}

		gotOpts = len(opts)

		return &stubMigrator{migrateFn: func(_ context.Context, queues []liteq.QueueDefinition) error {
			gotQueues = queues
			return nil
		}}
	}

	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"migrate-up", "--dry-run", "--queues", "queue_tasks:queue_tasks_dead_letter"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	if newPoolCalled {
		t.Fatal("expected migrate-up dry-run to avoid creating a pool")
	}

	if gotOpts != 2 {
		t.Fatalf("option count = %d, want 2", gotOpts)
	}

	if len(gotQueues) != 1 {
		t.Fatalf("queue count = %d, want 1", len(gotQueues))
	}

	if gotQueues[0].Name != "queue_tasks" || gotQueues[0].DLQName != "queue_tasks_dead_letter" {
		t.Fatalf("queues = %#v, want queue_tasks:queue_tasks_dead_letter", gotQueues)
	}
}

func TestRun_MigrateDownDryRunRequiresDatabaseURL(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"migrate-down", "--dry-run"}, &stdout, &stderr)

	if exitCode == 0 {
		t.Fatal("expected non-zero exit code")
	}

	if !strings.Contains(stderr.String(), "migrate-down dry-run requires a database connection") {
		t.Fatalf("stderr = %q, want migrate-down dry-run guidance", stderr.String())
	}
}

func TestRun_MigrateDownAllUsesEnvironmentDatabaseURL(t *testing.T) {
	origNewPool := newPool
	origNewSchemaMigrator := newSchemaMigrator

	t.Cleanup(func() {
		newPool = origNewPool
		newSchemaMigrator = origNewSchemaMigrator
	})

	t.Setenv("DATABASE_URL", "postgres://env-user:env-pass@localhost:5432/liteq")

	var gotDatabaseURL string

	newPool = func(_ context.Context, databaseURL string) (*pgxpool.Pool, error) {
		gotDatabaseURL = databaseURL
		return nil, nil
	}

	calledDownAll := false

	newSchemaMigrator = func(pool *pgxpool.Pool, opts ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		if len(opts) != 1 {
			t.Fatalf("option count = %d, want 1", len(opts))
		}

		return &stubMigrator{migrateDownAllFn: func(context.Context) error {
			calledDownAll = true
			return nil
		}}
	}

	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"migrate-down", "--steps", "all"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	if gotDatabaseURL != "postgres://env-user:env-pass@localhost:5432/liteq" {
		t.Fatalf("database url = %q", gotDatabaseURL)
	}

	if !calledDownAll {
		t.Fatal("expected MigrateDownAll to be called")
	}
}

func TestParseQueueDefinitions(t *testing.T) {
	queues, err := parseQueueDefinitions("queue_tasks:queue_tasks_dead_letter, queue_scheduled:queue_scheduled_dead_letter")
	if err != nil {
		t.Fatalf("parseQueueDefinitions returned error: %v", err)
	}

	if len(queues) != 2 {
		t.Fatalf("queue count = %d, want 2", len(queues))
	}

	if queues[1].Name != "queue_scheduled" || queues[1].DLQName != "queue_scheduled_dead_letter" {
		t.Fatalf("queues[1] = %#v", queues[1])
	}
}

func TestParseQueueDefinitions_DefaultDLQ(t *testing.T) {
	queues, err := parseQueueDefinitions("tasks")
	if err != nil {
		t.Fatalf("parseQueueDefinitions returned error: %v", err)
	}

	if len(queues) != 1 {
		t.Fatalf("queue count = %d, want 1", len(queues))
	}

	if queues[0].Name != "tasks" {
		t.Fatalf("queue name = %q, want %q", queues[0].Name, "tasks")
	}

	if queues[0].DLQName != "" {
		t.Fatalf("dlq name = %q, want empty for defaulting in SchemaManager", queues[0].DLQName)
	}
}

func TestParseQueueDefinitions_RejectsEmptyQueueName(t *testing.T) {
	_, err := parseQueueDefinitions(":tasks_dead_letter")
	if err == nil {
		t.Fatal("expected error for empty queue name")
	}

	if !strings.Contains(err.Error(), "queue name must not be empty") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseStepsRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "-1", "abc"} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			_, _, err := parseSteps(value)
			if err == nil {
				t.Fatalf("parseSteps(%q) returned nil error", value)
			}
		})
	}
}
