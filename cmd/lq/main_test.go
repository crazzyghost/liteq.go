package main

import (
	"context"
	"strings"
	"testing"

	liteq "liteq.go"
)

type stubMigrator struct {
	migrateFn             func(context.Context, []liteq.QueueDefinition) error
	migrateWithStatusFn   func(context.Context, []liteq.QueueDefinition) (bool, error)
	migrateDownFn         func(context.Context, int) error
	migrateDownAllFn      func(context.Context) error
	migrateDownQueueFn    func(context.Context, string, int) error
	migrateDownQueueAllFn func(context.Context, string) error
}

func (s *stubMigrator) Migrate(ctx context.Context, queues []liteq.QueueDefinition) error {
	if s.migrateFn != nil {
		return s.migrateFn(ctx, queues)
	}

	return nil
}

func (s *stubMigrator) MigrateWithStatus(ctx context.Context, queues []liteq.QueueDefinition) (bool, error) {
	if s.migrateWithStatusFn != nil {
		return s.migrateWithStatusFn(ctx, queues)
	}
	if s.migrateFn != nil {
		return true, s.migrateFn(ctx, queues)
	}

	return true, nil
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

func (s *stubMigrator) MigrateDownQueue(ctx context.Context, queueName string, steps int) error {
	if s.migrateDownQueueFn != nil {
		return s.migrateDownQueueFn(ctx, queueName, steps)
	}

	return nil
}

func (s *stubMigrator) MigrateDownQueueAll(ctx context.Context, queueName string) error {
	if s.migrateDownQueueAllFn != nil {
		return s.migrateDownQueueAllFn(ctx, queueName)
	}

	return nil
}

func TestVersionFlag(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--version"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	if stdout.String() != "lq dev\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "lq dev\n")
	}
}

func TestVersionFlag_InjectedVersion(t *testing.T) {
	orig := version
	version = "v1.2.3"
	t.Cleanup(func() { version = orig })

	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--version"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	if stdout.String() != "lq v1.2.3\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "lq v1.2.3\n")
	}
}

func TestRun_VersionSubcommand_IsUnknown(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"version"}, &stdout, &stderr)

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "unknown command: version") {
		t.Fatalf("stderr = %q, want unknown version command error", stderr.String())
	}
}

func TestRun_MigrateUp_IsUnknown(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"migrate-up"}, &stdout, &stderr)

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "unknown command: migrate-up") {
		t.Fatalf("stderr = %q, want unknown migrate-up command error", stderr.String())
	}
}

func TestRun_MigrateDown_IsUnknown(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"migrate-down"}, &stdout, &stderr)

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "unknown command: migrate-down") {
		t.Fatalf("stderr = %q, want unknown migrate-down command error", stderr.String())
	}
}

func TestRun_NoArgs_PrintsUsage(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), nil, &stdout, &stderr)

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr = %q, want usage text", stderr.String())
	}
	if !strings.Contains(stderr.String(), "error: command is required") {
		t.Fatalf("stderr = %q, want command is required error", stderr.String())
	}
}

func TestRun_Help_Flag(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--help"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if !strings.Contains(stdout.String(), "lq — Postgres-backed queue management CLI") {
		t.Fatalf("stdout = %q, want root help", stdout.String())
	}
}

func TestRun_Help_ShortFlag(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"-h"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Fatalf("stdout = %q, want usage text", stdout.String())
	}
}

func TestRun_Help_Subcommand(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"help"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Commands:") {
		t.Fatalf("stdout = %q, want commands section", stdout.String())
	}
}

func TestRun_Help_ContainsAllCommands(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--help"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	for _, want := range []string{"create", "rm", "ls", "pause", "resume", "drain", "history"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout = %q, want command %q", stdout.String(), want)
		}
	}
}

func TestRun_Help_ContainsEnvVars(t *testing.T) {
	var stdout, stderr strings.Builder
	exitCode := run(context.Background(), []string{"--help"}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}

	for _, want := range []string{"LITEQ_DATABASE_URL", "LITEQ_SCHEMA"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout = %q, want env var %q", stdout.String(), want)
		}
	}
}

func TestResolveDatabaseURL_PrefersFlag(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "postgres://env")

	got := resolveDatabaseURL("postgres://flag")

	if got != "postgres://flag" {
		t.Fatalf("resolveDatabaseURL() = %q, want %q", got, "postgres://flag")
	}
}

func TestResolveDatabaseURL_FallsBackToEnv(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "postgres://env")

	got := resolveDatabaseURL("")

	if got != "postgres://env" {
		t.Fatalf("resolveDatabaseURL() = %q, want %q", got, "postgres://env")
	}
}

func TestResolveDatabaseURL_ReturnsEmptyWhenNoneSet(t *testing.T) {
	t.Setenv("LITEQ_DATABASE_URL", "")
	t.Setenv("DATABASE_URL", "postgres://legacy")

	got := resolveDatabaseURL("")

	if got != "" {
		t.Fatalf("resolveDatabaseURL() = %q, want empty", got)
	}
}

func TestResolveSchema_PrefersFlag(t *testing.T) {
	t.Setenv("LITEQ_SCHEMA", "env_schema")

	got := resolveSchema("flag_schema")

	if got != "flag_schema" {
		t.Fatalf("resolveSchema() = %q, want %q", got, "flag_schema")
	}
}

func TestResolveSchema_FallsBackToEnv(t *testing.T) {
	t.Setenv("LITEQ_SCHEMA", "env_schema")

	got := resolveSchema("")

	if got != "env_schema" {
		t.Fatalf("resolveSchema() = %q, want %q", got, "env_schema")
	}
}

func TestResolveSchema_DefaultsToLiteq(t *testing.T) {
	t.Setenv("LITEQ_SCHEMA", "")

	got := resolveSchema("")

	if got != "liteq" {
		t.Fatalf("resolveSchema() = %q, want %q", got, "liteq")
	}
}
