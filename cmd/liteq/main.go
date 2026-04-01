// Package main implements the liteq CLI for database migrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	liteq "liteq.go"
)

type schemaMigrator interface {
	Migrate(context.Context, []liteq.QueueDefinition) error
	MigrateDown(context.Context, int) error
	MigrateDownAll(context.Context) error
}

var newPool = func(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

var newSchemaMigrator = func(pool *pgxpool.Pool, opts ...liteq.SchemaManagerOption) schemaMigrator {
	return liteq.NewSchemaManager(pool, opts...)
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if err := runCommand(ctx, args, stdout, stderr); err != nil {
		if stderr != nil {
			_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
		}
		return 1
	}
	return 0
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printRootUsage(stderr)
		return fmt.Errorf("command is required")
	}

	switch args[0] {
	case "migrate-up":
		return runMigrateUp(ctx, args[1:], stdout, stderr)
	case "migrate-down":
		return runMigrateDown(ctx, args[1:], stdout, stderr)
	default:
		printRootUsage(stderr)
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func runMigrateUp(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("migrate-up", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		if stderr != nil {
			_, _ = fmt.Fprintln(stderr, "usage: liteq migrate-up [--database-url] [--schema] [--queues] [--dry-run]")
		}
	}

	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema")
	queuesFlag := fs.String("queues", "", "Comma-separated queue[:dlq] entries")
	dryRunFlag := fs.Bool("dry-run", false, "Print SQL without executing")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse migrate-up flags: %w", err)
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("unexpected arguments for migrate-up: %s", strings.Join(fs.Args(), " "))
	}

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")
	queuesRaw := firstNonEmpty(*queuesFlag, os.Getenv("LITEQ_QUEUES"))

	queues, err := parseQueueDefinitions(queuesRaw)
	if err != nil {
		return err
	}

	opts := []liteq.SchemaManagerOption{liteq.WithSchemaManagerSchema(schema)}
	if *dryRunFlag {
		opts = append(opts, liteq.WithDryRun(stdout))
	}

	var pool *pgxpool.Pool
	if !*dryRunFlag {
		pool, err = requirePool(ctx, databaseURL)
		if err != nil {
			return err
		}
		if pool != nil {
			defer pool.Close()
		}
	}

	return newSchemaMigrator(pool, opts...).Migrate(ctx, queues)
}

func runMigrateDown(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("migrate-down", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		if stderr != nil {
			_, _ = fmt.Fprintln(stderr, "usage: liteq migrate-down [--database-url] [--schema] [--steps] [--dry-run]")
		}
	}

	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema")
	stepsFlag := fs.String("steps", "1", "Number of migrations to roll back, or 'all'")
	dryRunFlag := fs.Bool("dry-run", false, "Print SQL without executing")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse migrate-down flags: %w", err)
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("unexpected arguments for migrate-down: %s", strings.Join(fs.Args(), " "))
	}

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")
	rollbackAll, steps, err := parseSteps(*stepsFlag)
	if err != nil {
		return err
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		if *dryRunFlag {
			return fmt.Errorf("migrate-down dry-run requires a database connection to inspect applied migrations: %w", err)
		}
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	opts := []liteq.SchemaManagerOption{liteq.WithSchemaManagerSchema(schema)}
	if *dryRunFlag {
		opts = append(opts, liteq.WithDryRun(stdout))
	}

	migrator := newSchemaMigrator(pool, opts...)
	if rollbackAll {
		return migrator.MigrateDownAll(ctx)
	}
	return migrator.MigrateDown(ctx, steps)
}

func requirePool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("database url is required; use --database-url or DATABASE_URL")
	}
	pool, err := newPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func parseQueueDefinitions(value string) ([]liteq.QueueDefinition, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	parts := strings.Split(value, ",")
	queues := make([]liteq.QueueDefinition, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		pair := strings.SplitN(part, ":", 2)
		queue := liteq.QueueDefinition{Name: strings.TrimSpace(pair[0])}
		if queue.Name == "" {
			return nil, fmt.Errorf("invalid queue definition %q: queue name must not be empty", part)
		}
		if len(pair) == 2 {
			queue.DLQName = strings.TrimSpace(pair[1])
		}
		queues = append(queues, queue)
	}

	return queues, nil
}

func parseSteps(value string) (all bool, steps int, err error) {
	trimmed := strings.TrimSpace(value)
	if strings.EqualFold(trimmed, "all") {
		return true, 0, nil
	}

	steps, err = strconv.Atoi(trimmed)
	if err != nil {
		return false, 0, fmt.Errorf("invalid --steps value %q: must be a positive integer or 'all'", value)
	}
	if steps <= 0 {
		return false, 0, fmt.Errorf("invalid --steps value %q: must be a positive integer or 'all'", value)
	}
	return false, steps, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func printRootUsage(w io.Writer) {
	if w == nil {
		return
	}
	_, _ = fmt.Fprintln(w, "usage: liteq <migrate-up|migrate-down> [flags]")
}
