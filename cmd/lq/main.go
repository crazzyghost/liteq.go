// Package main implements the lq CLI for queue administration.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
	liteq "github.com/crazzyghost/liteq.go"
)

// version is the binary version, injected at build time via -X main.version=<tag>.
var version = "dev"

type schemaMigrator interface {
	Migrate(context.Context, []liteq.QueueDefinition) error
	MigrateWithStatus(context.Context, []liteq.QueueDefinition) (bool, error)
	MigrateDown(context.Context, int) error
	MigrateDownAll(context.Context) error
	MigrateDownQueue(context.Context, string, int) error
	MigrateDownQueueAll(context.Context, string) error
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
	rootFS := pflag.NewFlagSet("lq", pflag.ContinueOnError)
	rootFS.SetInterspersed(false)
	rootFS.SetOutput(stderr)

	versionFlag := rootFS.Bool("version", false, "Print version and exit")
	helpFlag := rootFS.BoolP("help", "h", false, "Print help and exit")

	if err := rootFS.Parse(args); err != nil {
		return fmt.Errorf("parse root flags: %w", err)
	}

	if *versionFlag {
		_, _ = fmt.Fprintf(stdout, "lq %s\n", version)
		return nil
	}

	remaining := rootFS.Args()
	if *helpFlag || (len(remaining) > 0 && remaining[0] == "help") {
		printHelp(stdout)
		return nil
	}

	if len(remaining) == 0 {
		printHelp(stderr)
		return fmt.Errorf("command is required")
	}

	switch remaining[0] {
	case "queue":
		return runQueue(ctx, remaining[1:], stdout, stderr)
	default:
		printHelp(stderr)
		return fmt.Errorf("unknown command: %s", remaining[0])
	}
}

func requirePool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("database url is required; use --database-url or LITEQ_DATABASE_URL")
	}

	pool, err := newPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func resolveDatabaseURL(flagValue string) string {
	if value := strings.TrimSpace(flagValue); value != "" {
		return value
	}

	return strings.TrimSpace(os.Getenv("LITEQ_DATABASE_URL"))
}

func resolveSchema(flagValue string) string {
	if value := strings.TrimSpace(flagValue); value != "" {
		return value
	}

	if value := strings.TrimSpace(os.Getenv("LITEQ_SCHEMA")); value != "" {
		return value
	}

	return "liteq"
}

func printHelp(w io.Writer) {
	if w == nil {
		return
	}

	_, _ = fmt.Fprintln(w, `lq — Postgres-backed queue management CLI

Usage:
  lq queue <command> [flags]
  lq --version
  lq --help

Commands:
  create   Create a queue (apply migrations)
  rm       Remove a queue (rollback migrations)
  ls       List all queues and their state
  pause    Pause a queue
  resume   Resume a paused queue
  drain    Drain and pause a queue
  history  Show queue state change history

Environment Variables:
  LITEQ_DATABASE_URL   Postgres connection string (--database-url)
  LITEQ_SCHEMA         Target schema (--schema, default: liteq)

Run 'lq queue <command> --help' for details on a specific command.`)
}
