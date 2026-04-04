package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	liteq "liteq.go"
)

func restoreCLIHooks(t *testing.T) {
	t.Helper()

	origNewPool := newPool
	origNewSchemaMigrator := newSchemaMigrator
	origQueryQueueStateRows := queryQueueStateRows
	origRemovalPromptReader := removalPromptReader
	origRemovalPromptIsInteractive := removalPromptIsInteractive

	t.Cleanup(func() {
		newPool = origNewPool
		newSchemaMigrator = origNewSchemaMigrator
		queryQueueStateRows = origQueryQueueStateRows
		removalPromptReader = origRemovalPromptReader
		removalPromptIsInteractive = origRemovalPromptIsInteractive
	})
}

func TestRun_Queue_RequiresSubcommand(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue subcommand required") {
		t.Fatalf("stderr = %q, want queue subcommand error", stderr.String())
	}
}

func TestRun_Queue_UnknownSubcommand(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "unknown"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown queue subcommand: unknown") {
		t.Fatalf("stderr = %q, want unknown queue subcommand error", stderr.String())
	}
}

func TestRun_Queue_HelpFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "--help"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Commands:") {
		t.Fatalf("stdout = %q, want commands section", stdout.String())
	}
}

func TestRun_Queue_HelpSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "help"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Fatalf("stdout = %q, want usage text", stdout.String())
	}
}

func TestRun_Queue_ListIsUnknown(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "list"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown queue subcommand: list") {
		t.Fatalf("stderr = %q, want queue list unknown error", stderr.String())
	}
}

func TestRun_QueueCreate_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "create"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue create <queue_name>") {
		t.Fatalf("stderr = %q, want queue create usage hint", stderr.String())
	}
}

func TestRun_QueueCreate_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "create", "queue_tasks"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueCreate_DryRunDoesNotRequireDatabaseURL(t *testing.T) {
	restoreCLIHooks(t)

	newPoolCalled := false
	newPool = func(context.Context, string) (*pgxpool.Pool, error) {
		newPoolCalled = true
		return nil, nil
	}

	var gotQueues []liteq.QueueDefinition
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil pool for dry-run queue create, got %#v", pool)
		}

		return &stubMigrator{
			migrateFn: func(_ context.Context, queues []liteq.QueueDefinition) error {
				gotQueues = queues
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--dry-run", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if newPoolCalled {
		t.Fatal("expected dry-run queue create to avoid creating a pool")
	}
	if len(gotQueues) != 1 || gotQueues[0].Name != "queue_tasks" || gotQueues[0].DLQName != "" {
		t.Fatalf("got queues = %#v, want queue_tasks without dlq", gotQueues)
	}
}

func TestRun_QueueCreate_ValidatesQueueName(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "create", "--dry-run", "bad name!"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid characters error", stderr.String())
	}
}

func TestRun_QueueCreate_ValidatesDLQName(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--dry-run", "--dlq", "bad name!", "queue_tasks"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid characters error", stderr.String())
	}
}

func TestRun_QueueCreate_RejectsDLQAndNoDLQTogether(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--dry-run", "--dlq", "queue_tasks_dlq", "--no-dlq", "queue_tasks"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "--dlq and --no-dlq are mutually exclusive") {
		t.Fatalf("stderr = %q, want mutual exclusivity error", stderr.String())
	}
}

func TestRun_QueueCreate_CallsMigrateWithQueueDefinition(t *testing.T) {
	restoreCLIHooks(t)

	var gotDatabaseURL string
	newPool = func(_ context.Context, databaseURL string) (*pgxpool.Pool, error) {
		gotDatabaseURL = databaseURL
		return nil, nil
	}

	var gotQueues []liteq.QueueDefinition
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateFn: func(_ context.Context, queues []liteq.QueueDefinition) error {
				gotQueues = queues
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--database-url", "postgres://flag", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if gotDatabaseURL != "postgres://flag" {
		t.Fatalf("database url = %q, want %q", gotDatabaseURL, "postgres://flag")
	}
	if len(gotQueues) != 1 || gotQueues[0].Name != "queue_tasks" || gotQueues[0].DLQName != "" {
		t.Fatalf("got queues = %#v, want queue_tasks without dlq", gotQueues)
	}
	if !strings.Contains(stdout.String(), `created queue "queue_tasks"`) {
		t.Fatalf("stdout = %q, want create message", stdout.String())
	}
}

func TestRun_QueueCreate_CallsMigrateWithDLQ(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	var gotQueues []liteq.QueueDefinition
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateFn: func(_ context.Context, queues []liteq.QueueDefinition) error {
				gotQueues = queues
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--database-url", "postgres://flag", "--dlq", "queue_tasks_dlq", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(gotQueues) != 1 || gotQueues[0].Name != "queue_tasks" || gotQueues[0].DLQName != "queue_tasks_dlq" {
		t.Fatalf("got queues = %#v, want queue_tasks with dlq", gotQueues)
	}
	if !strings.Contains(stdout.String(), `created queue "queue_tasks" with dlq "queue_tasks_dlq"`) {
		t.Fatalf("stdout = %q, want create with dlq message", stdout.String())
	}
}

func TestRun_QueueCreate_NoDLQDisablesDefaultDLQ(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	var gotQueues []liteq.QueueDefinition
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateWithStatusFn: func(_ context.Context, queues []liteq.QueueDefinition) (bool, error) {
				gotQueues = queues
				return true, nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--database-url", "postgres://flag", "--no-dlq", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(gotQueues) != 1 || gotQueues[0].Name != "queue_tasks" || gotQueues[0].DLQName != "" || !gotQueues[0].DisableDLQ {
		t.Fatalf("got queues = %#v, want queue_tasks with DisableDLQ", gotQueues)
	}
	if !strings.Contains(stdout.String(), `created queue "queue_tasks"`) {
		t.Fatalf("stdout = %q, want create message", stdout.String())
	}
	if strings.Contains(stdout.String(), "with dlq") {
		t.Fatalf("stdout = %q, should not mention dlq", stdout.String())
	}
}

func TestRun_QueueCreate_UpToDateMessage(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateWithStatusFn: func(_ context.Context, queues []liteq.QueueDefinition) (bool, error) {
				if len(queues) != 1 || queues[0].Name != "queue_tasks" || queues[0].DLQName != "" {
					t.Fatalf("queues = %#v, want queue_tasks without dlq", queues)
				}
				return false, nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--database-url", "postgres://flag", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `queue "queue_tasks" is up to date`) {
		t.Fatalf("stdout = %q, want up-to-date message", stdout.String())
	}
}

func TestRun_QueueCreate_RejectsUnexpectedArgs(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "--dry-run", "queue_tasks", "extra"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected arguments for queue create: extra") {
		t.Fatalf("stderr = %q, want unexpected arguments error", stderr.String())
	}
}

func TestRun_QueueCreate_AcceptsFlagsAfterQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPoolCalled := false
	newPool = func(context.Context, string) (*pgxpool.Pool, error) {
		newPoolCalled = true
		return nil, nil
	}

	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil pool for dry-run queue create, got %#v", pool)
		}
		return &stubMigrator{}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "create", "queue_tasks", "--dry-run"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if newPoolCalled {
		t.Fatal("expected flags after queue name to still avoid creating a pool")
	}
}

func TestRun_QueueCreate_HelpFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "create", "--help"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "lq queue create [flags] <queue_name>") {
		t.Fatalf("stdout = %q, want queue create usage", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--database-url") {
		t.Fatalf("stdout = %q, want queue create flags", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--no-dlq") {
		t.Fatalf("stdout = %q, want --no-dlq flag", stdout.String())
	}
}

func TestRun_QueueRm_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "rm"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue rm <queue_name>") {
		t.Fatalf("stderr = %q, want queue rm usage hint", stderr.String())
	}
}

func TestRun_QueueRm_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "rm", "--force", "queue_tasks"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueRm_DryRunRequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "rm", "--dry-run", "queue_tasks"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue rm dry-run requires a database connection") {
		t.Fatalf("stderr = %q, want queue rm dry-run guidance", stderr.String())
	}
}

func TestRun_QueueRm_ForceSkipsConfirmation(t *testing.T) {
	restoreCLIHooks(t)

	removalPromptIsInteractive = func(io.Reader) bool {
		t.Fatal("unexpected confirmation prompt")
		return false
	}

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	calledDownQueueAll := false
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateDownQueueAllFn: func(_ context.Context, queueName string) error {
				if queueName != "queue_tasks" {
					t.Fatalf("queueName = %q, want %q", queueName, "queue_tasks")
				}
				calledDownQueueAll = true
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "--force", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !calledDownQueueAll {
		t.Fatal("expected MigrateDownQueueAll to be called")
	}
}

func TestRun_QueueRm_DefaultStepsIsAll(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	calledDownQueueAll := false
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateDownQueueAllFn: func(_ context.Context, queueName string) error {
				if queueName != "queue_tasks" {
					t.Fatalf("queueName = %q, want %q", queueName, "queue_tasks")
				}
				calledDownQueueAll = true
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "--force", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !calledDownQueueAll {
		t.Fatal("expected MigrateDownQueueAll to be called by default")
	}
}

func TestRun_QueueRm_StepsCallsMigrateDown(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	gotSteps := 0
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateDownQueueFn: func(_ context.Context, queueName string, steps int) error {
				if queueName != "queue_tasks" {
					t.Fatalf("queueName = %q, want %q", queueName, "queue_tasks")
				}
				gotSteps = steps
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "--force", "--steps", "2", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if gotSteps != 2 {
		t.Fatalf("steps = %d, want 2", gotSteps)
	}
}

func TestRun_QueueRm_ReturnsErrorWhenQueueMissing(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateDownQueueAllFn: func(_ context.Context, queueName string) error {
				return fmt.Errorf("queue %q not found", queueName)
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "--force", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), `queue "queue_tasks" not found`) {
		t.Fatalf("stderr = %q, want not found error", stderr.String())
	}
	if strings.Contains(stdout.String(), `removed queue "queue_tasks"`) {
		t.Fatalf("stdout = %q, should not report removal", stdout.String())
	}
}

func TestRun_QueueRm_AbortsOnPipedStdin(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		t.Fatal("unexpected database connection on aborted confirmation")
		return nil, nil
	}

	removalPromptReader = strings.NewReader("")
	removalPromptIsInteractive = func(io.Reader) bool { return false }

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "aborted") {
		t.Fatalf("stdout = %q, want aborted message", stdout.String())
	}
	if !strings.Contains(stderr.String(), `remove queue "queue_tasks"?`) {
		t.Fatalf("stderr = %q, want confirmation prompt", stderr.String())
	}
}

func TestRun_QueueRm_InteractiveYesCallsMigrator(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}

	removalPromptReader = strings.NewReader("y\n")
	removalPromptIsInteractive = func(io.Reader) bool { return true }

	calledDownQueueAll := false
	newSchemaMigrator = func(pool *pgxpool.Pool, _ ...liteq.SchemaManagerOption) schemaMigrator {
		if pool != nil {
			t.Fatalf("expected nil test pool, got %#v", pool)
		}

		return &stubMigrator{
			migrateDownQueueAllFn: func(_ context.Context, queueName string) error {
				if queueName != "queue_tasks" {
					t.Fatalf("queueName = %q, want %q", queueName, "queue_tasks")
				}
				calledDownQueueAll = true
				return nil
			},
		}
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "rm", "--database-url", "postgres://flag", "queue_tasks"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !calledDownQueueAll {
		t.Fatal("expected MigrateDownQueueAll to be called after confirmation")
	}
}

func TestRun_QueueLs_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "ls"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueLs_RejectsInvalidSchema(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "ls", "--schema", "bad schema!"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid schema error", stderr.String())
	}
}

func TestRun_QueueLs_RejectsUnexpectedArguments(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "ls", "extra"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected arguments for queue ls: extra") {
		t.Fatalf("stderr = %q, want unexpected arguments error", stderr.String())
	}
}

func TestRun_QueueLs_JSONOutput(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}
	queryQueueStateRows = func(context.Context, *pgxpool.Pool, string) ([]queueStateRow, error) {
		return []queueStateRow{
			{
				QueueName: "queue_tasks",
				State:     "active",
				Since:     time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
				Reason:    "queue created",
			},
		}, nil
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "ls", "--database-url", "postgres://flag", "--output", "json"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}

	var got []map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout = %q, unmarshal error = %v", stdout.String(), err)
	}
	if len(got) != 1 {
		t.Fatalf("json row count = %d, want 1", len(got))
	}
	if got[0]["queue"] != "queue_tasks" || got[0]["state"] != "active" || got[0]["reason"] != "queue created" {
		t.Fatalf("json row = %#v", got[0])
	}
}

func TestRun_QueueLs_JSONOutputEmpty(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, nil
	}
	queryQueueStateRows = func(context.Context, *pgxpool.Pool, string) ([]queueStateRow, error) {
		return nil, nil
	}

	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "ls", "--database-url", "postgres://flag", "--output", "json"},
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "[]" {
		t.Fatalf("stdout = %q, want []", stdout.String())
	}
}

func TestRun_QueueLs_InvalidOutputFormat(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		t.Fatal("unexpected database connection for invalid output format")
		return nil, nil
	}

	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "ls", "--output", "invalid"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), `invalid output format "invalid": must be "table" or "json"`) {
		t.Fatalf("stderr = %q, want invalid output format error", stderr.String())
	}
}

func TestRun_QueuePause_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "pause"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue pause <queue_name>") {
		t.Fatalf("stderr = %q, want queue pause usage hint", stderr.String())
	}
}

func TestRun_QueuePause_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "pause", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueuePause_RejectsUnexpectedArguments(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "postgres://env")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "pause", "my_queue", "extra"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected arguments for queue pause: extra") {
		t.Fatalf("stderr = %q, want unexpected arguments error", stderr.String())
	}
}

func TestRun_QueueResume_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "resume"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue resume <queue_name>") {
		t.Fatalf("stderr = %q, want queue resume usage hint", stderr.String())
	}
}

func TestRun_QueueResume_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "resume", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueHistory_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "history"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue history <queue_name>") {
		t.Fatalf("stderr = %q, want queue history usage hint", stderr.String())
	}
}

func TestRun_QueueHistory_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "history", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueHistory_RejectsNegativeLimit(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "history", "--limit", "-1", "my_queue"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid --limit value -1: must be >= 0") {
		t.Fatalf("stderr = %q, want negative limit error", stderr.String())
	}
}

func TestRun_QueueHistory_AcceptsFlagsAfterQueueName(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "history", "my_queue", "--limit", "-1"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid --limit value -1: must be >= 0") {
		t.Fatalf("stderr = %q, want parsed --limit after queue name", stderr.String())
	}
}

func TestRun_QueueDrain_RequiresQueueName(t *testing.T) {
	restoreCLIHooks(t)

	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "drain"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue name is required: lq queue drain <queue_name>") {
		t.Fatalf("stderr = %q, want queue drain usage hint", stderr.String())
	}
}

func TestRun_QueueDrain_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "drain", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want database url required error", stderr.String())
	}
}

func TestRun_QueueDrain_RejectsInvalidQueueName(t *testing.T) {
	var stderr bytes.Buffer
	code := run(
		context.Background(),
		[]string{"queue", "drain", "--database-url", "postgres://flag", "bad name!"},
		nil,
		&stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid queue name error", stderr.String())
	}
}

func TestPrintQueueTable_HeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	printQueueTable(&buf, nil)

	out := buf.String()
	for _, want := range []string{"QUEUE", "STATE", "SINCE", "REASON"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output = %q, want header %q", out, want)
		}
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("output = %q, want exactly 1 line", out)
	}
}

func TestQueueListQuery_ContainsSelectAndTables(t *testing.T) {
	query := queueListQuery("liteq.queue_meta", "liteq.queue_states")
	trimmed := strings.TrimSpace(query)

	if !strings.HasPrefix(trimmed, "SELECT") {
		t.Fatalf("query = %q, want SELECT prefix", trimmed)
	}
	if !strings.Contains(query, "FROM liteq.queue_meta s") {
		t.Fatalf("query = %q, want queue_meta source", query)
	}
	if !strings.Contains(query, "FROM liteq.queue_states") {
		t.Fatalf("query = %q, want queue_states source", query)
	}
}

func TestPrintQueueTable_WithRows(t *testing.T) {
	ts := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	rows := []queueStateRow{
		{QueueName: "queue_tasks", State: "active", Since: ts, Reason: "queue created"},
		{QueueName: "queue_scheduled", State: "paused", Since: ts, Reason: "maintenance"},
	}

	var buf bytes.Buffer
	printQueueTable(&buf, rows)

	out := buf.String()
	for _, want := range []string{"queue_tasks", "queue_scheduled", "paused", "maintenance"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output = %q, want %q", out, want)
		}
	}
	if strings.Count(out, "\n") != 3 {
		t.Fatalf("output = %q, want 3 lines", out)
	}
}

func TestPrintQueueJSON(t *testing.T) {
	rows := []queueStateRow{
		{
			QueueName: "queue_tasks",
			State:     "active",
			Since:     time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
			Reason:    "queue created",
		},
	}

	var buf bytes.Buffer
	if err := printQueueJSON(&buf, rows); err != nil {
		t.Fatalf("printQueueJSON() error = %v", err)
	}

	var got []map[string]string
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(got) != 1 || got[0]["queue"] != "queue_tasks" {
		t.Fatalf("json output = %#v", got)
	}
}

func TestPrintQueueHistory_HeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	printQueueHistory(&buf, nil)

	out := buf.String()
	for _, want := range []string{"STATE", "CHANGED AT", "REASON"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output = %q, want header %q", out, want)
		}
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("output = %q, want exactly 1 line", out)
	}
}

func TestPrintQueueHistory_WithRows(t *testing.T) {
	ts := time.Date(2026, 4, 3, 15, 30, 0, 0, time.UTC)
	rows := []queueEventRow{
		{State: "paused", ChangedAt: ts, Reason: "maintenance window"},
		{State: "active", ChangedAt: ts.Add(-90 * time.Minute), Reason: ""},
	}

	var buf bytes.Buffer
	printQueueHistory(&buf, rows)

	out := buf.String()
	for _, want := range []string{"paused", "maintenance window"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output = %q, want %q", out, want)
		}
	}
	if strings.Count(out, "\n") != 3 {
		t.Fatalf("output = %q, want 3 lines", out)
	}
}

func TestCLIValidateIdentifier(t *testing.T) {
	cases := []struct {
		value   string
		wantErr bool
	}{
		{"liteq", false},
		{"queue_tasks", false},
		{"QueueTasks123", false},
		{"", true},
		{"bad schema!", true},
		{"bad-name", true},
		{"has space", true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.value, func(t *testing.T) {
			err := cliValidateIdentifier("test", tc.value)
			if tc.wantErr && err == nil {
				t.Fatalf("cliValidateIdentifier(%q) = nil, want error", tc.value)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("cliValidateIdentifier(%q) = %v, want nil", tc.value, err)
			}
		})
	}
}

func TestCLIQualifyIdentifier(t *testing.T) {
	cases := []struct {
		schema string
		name   string
		want   string
	}{
		{"liteq", "queue_meta", "liteq.queue_meta"},
		{"", "queue_meta", "queue_meta"},
		{"liteq", "", "liteq"},
		{"", "", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.schema+"."+tc.name, func(t *testing.T) {
			got := cliQualifyIdentifier(tc.schema, tc.name)
			if got != tc.want {
				t.Fatalf("cliQualifyIdentifier(%q, %q) = %q, want %q", tc.schema, tc.name, got, tc.want)
			}
		})
	}
}

func TestParseStepsRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"0", "-1", "abc"} {
		value := value
		t.Run(value, func(t *testing.T) {
			_, _, err := parseSteps(value)
			if err == nil {
				t.Fatalf("parseSteps(%q) returned nil error", value)
			}
		})
	}
}
