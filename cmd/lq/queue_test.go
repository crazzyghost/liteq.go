package main

// queue_test.go contains unit tests for the queue subcommand dispatcher and
// its individual handlers. All tests use the newPool override pattern to avoid
// requiring a real Postgres connection.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Dispatcher tests ────────────────────────────────────────────────────────

func TestRun_Queue_RequiresSubcommand(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue subcommand required") {
		t.Fatalf("stderr = %q, want 'queue subcommand required'", stderr.String())
	}
}

func TestRun_Queue_UnknownSubcommand(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "unknown"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown queue subcommand: unknown") {
		t.Fatalf("stderr = %q, want 'unknown queue subcommand: unknown'", stderr.String())
	}
}

func TestRun_RootUsage_IncludesQueue(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "queue") {
		t.Fatalf("stderr = %q, want root usage to mention 'queue'", stderr.String())
	}
}

// ─── queue list ──────────────────────────────────────────────────────────────

func TestRun_QueueList_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("LITEQ_SCHEMA", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "list"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want 'database url is required'", stderr.String())
	}
}

func TestRun_QueueList_RejectsInvalidSchema(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "list", "--schema", "bad schema!"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid characters error", stderr.String())
	}
}

func TestRun_QueueList_RejectsUnexpectedArguments(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "list", "extra"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected arguments for queue list: extra") {
		t.Fatalf("stderr = %q, want unexpected arguments error", stderr.String())
	}
}

// ─── queue pause ─────────────────────────────────────────────────────────────

func TestRun_QueuePause_RequiresQueueName(t *testing.T) {
	// Override newPool so the test fails fast at the validation check before any
	// real connection attempt. The missing queue-name check is intentionally
	// tested before pool creation.
	orig := newPool
	t.Cleanup(func() { newPool = orig })
	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

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
	t.Setenv("DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "pause", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want 'database url is required'", stderr.String())
	}
}

func TestRun_QueuePause_RejectsUnexpectedArguments(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "pause", "my_queue", "extra"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected arguments for queue pause: extra") {
		t.Fatalf("stderr = %q, want unexpected arguments error", stderr.String())
	}
}

// ─── queue resume ────────────────────────────────────────────────────────────

func TestRun_QueueResume_RequiresQueueName(t *testing.T) {
	orig := newPool
	t.Cleanup(func() { newPool = orig })
	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

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
	t.Setenv("DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "resume", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want 'database url is required'", stderr.String())
	}
}

// ─── queue history ───────────────────────────────────────────────────────────

func TestRun_QueueHistory_RequiresQueueName(t *testing.T) {
	orig := newPool
	t.Cleanup(func() { newPool = orig })
	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

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
	t.Setenv("DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "history", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want 'database url is required'", stderr.String())
	}
}

func TestRun_QueueHistory_RejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "history", "--limit", "-1", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid --limit value -1: must be >= 0") {
		t.Fatalf("stderr = %q, want negative limit error", stderr.String())
	}
}

// ─── queue drain ─────────────────────────────────────────────────────────────

func TestRun_QueueDrain_RequiresQueueName(t *testing.T) {
	orig := newPool
	t.Cleanup(func() { newPool = orig })
	newPool = func(_ context.Context, _ string) (*pgxpool.Pool, error) {
		return nil, fmt.Errorf("should not connect in this test")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

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
	t.Setenv("DATABASE_URL", "")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"queue", "drain", "my_queue"}, nil, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database url is required") {
		t.Fatalf("stderr = %q, want 'database url is required'", stderr.String())
	}
}

func TestRun_QueueDrain_RejectsInvalidQueueName(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	// A queue name with a space would be interpolated into a DELETE — must be rejected.
	code := run(context.Background(),
		[]string{"queue", "drain", "--database-url", "postgres://localhost/test", "bad name!"},
		nil, &stderr,
	)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid characters") {
		t.Fatalf("stderr = %q, want invalid characters error", stderr.String())
	}
}

// ─── Output formatting unit tests ────────────────────────────────────────────

func TestPrintQueueTable_HeaderOnly(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	printQueueTable(&buf, nil)

	out := buf.String()
	if !strings.Contains(out, "QUEUE") {
		t.Fatalf("output = %q, want header with QUEUE", out)
	}
	if !strings.Contains(out, "STATE") {
		t.Fatalf("output = %q, want header with STATE", out)
	}
	if !strings.Contains(out, "SINCE") {
		t.Fatalf("output = %q, want header with SINCE", out)
	}
	if !strings.Contains(out, "REASON") {
		t.Fatalf("output = %q, want header with REASON", out)
	}
	// Only the header line — no queue data rows.
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("output = %q, want exactly 1 line (header only)", out)
	}
}

func TestPrintQueueTable_WithRows(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	rows := []queueStateRow{
		{QueueName: "queue_tasks", State: "active", Since: ts, Reason: "queue created"},
		{QueueName: "queue_scheduled", State: "paused", Since: ts, Reason: "maintenance"},
	}

	var buf bytes.Buffer
	printQueueTable(&buf, rows)

	out := buf.String()
	if !strings.Contains(out, "queue_tasks") {
		t.Fatalf("output = %q, want queue_tasks row", out)
	}
	if !strings.Contains(out, "queue_scheduled") {
		t.Fatalf("output = %q, want queue_scheduled row", out)
	}
	if !strings.Contains(out, "paused") {
		t.Fatalf("output = %q, want paused state", out)
	}
	if !strings.Contains(out, "maintenance") {
		t.Fatalf("output = %q, want reason 'maintenance'", out)
	}
	// Header + 2 data rows.
	if strings.Count(out, "\n") != 3 {
		t.Fatalf("output = %q, want 3 lines (header + 2 rows)", out)
	}
}

func TestPrintQueueHistory_HeaderOnly(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	printQueueHistory(&buf, nil)

	out := buf.String()
	if !strings.Contains(out, "STATE") {
		t.Fatalf("output = %q, want header with STATE", out)
	}
	if !strings.Contains(out, "CHANGED AT") {
		t.Fatalf("output = %q, want header with CHANGED AT", out)
	}
	if !strings.Contains(out, "REASON") {
		t.Fatalf("output = %q, want header with REASON", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("output = %q, want exactly 1 line (header only)", out)
	}
}

func TestPrintQueueHistory_WithRows(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 3, 15, 30, 0, 0, time.UTC)
	rows := []queueEventRow{
		{State: "paused", ChangedAt: ts, Reason: "maintenance window"},
		{State: "active", ChangedAt: ts.Add(-90 * time.Minute), Reason: ""},
	}

	var buf bytes.Buffer
	printQueueHistory(&buf, rows)

	out := buf.String()
	if !strings.Contains(out, "paused") {
		t.Fatalf("output = %q, want paused row", out)
	}
	if !strings.Contains(out, "maintenance window") {
		t.Fatalf("output = %q, want reason 'maintenance window'", out)
	}
	// Header + 2 rows.
	if strings.Count(out, "\n") != 3 {
		t.Fatalf("output = %q, want 3 lines (header + 2 rows)", out)
	}
}

// ─── Identifier helpers ──────────────────────────────────────────────────────

func TestCLIValidateIdentifier(t *testing.T) {
	t.Parallel()

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
			t.Parallel()
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
	t.Parallel()

	cases := []struct {
		schema, name, want string
	}{
		{"liteq", "queue_meta", "liteq.queue_meta"},
		{"", "queue_meta", "queue_meta"},
		{"liteq", "", "liteq"},
		{"", "", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.schema+"."+tc.name, func(t *testing.T) {
			t.Parallel()
			got := cliQualifyIdentifier(tc.schema, tc.name)
			if got != tc.want {
				t.Fatalf("cliQualifyIdentifier(%q, %q) = %q, want %q",
					tc.schema, tc.name, got, tc.want)
			}
		})
	}
}
