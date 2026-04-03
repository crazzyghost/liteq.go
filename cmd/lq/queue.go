package main

// queue.go contains the CLI handlers for queue management subcommands:
// list, pause, resume, history, and drain.
//
// These commands operate directly on the queue_meta and queue_states tables
// created by Phase 013 migrations. They do not instantiate full PgQueue
// objects — the CLI is a thin administrative tool.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// cliIdentifierPattern mirrors schema_manager.identifierPattern. It is
// duplicated here because qualifyIdentifier is unexported in the liteq package.
// Any identifier interpolated into SQL (schema, table names) must match.
var cliIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// cliQualifyIdentifier returns "schema.name", "name" (if schema is empty), or
// "schema" (if name is empty).
func cliQualifyIdentifier(schema, name string) string {
	if schema == "" {
		return name
	}
	if name == "" {
		return schema
	}
	return schema + "." + name
}

// cliValidateIdentifier returns an error if value is empty or contains
// characters outside [A-Za-z0-9_]. kind is used in the error message.
func cliValidateIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	if !cliIdentifierPattern.MatchString(value) {
		return fmt.Errorf(
			"%s %q contains invalid characters; only letters, numbers, and underscores are allowed",
			kind, value,
		)
	}
	return nil
}

func unexpectedArgsError(command string, args []string) error {
	return fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(args, " "))
}

func normalizeInterspersedFlags(args []string, valueFlags ...string) []string {
	if len(args) == 0 {
		return nil
	}

	valueFlagSet := make(map[string]struct{}, len(valueFlags))
	for _, name := range valueFlags {
		valueFlagSet[name] = struct{}{}
	}

	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	expectValue := false
	afterDoubleDash := false

	for _, arg := range args {
		switch {
		case afterDoubleDash:
			positionals = append(positionals, arg)
		case expectValue:
			flags = append(flags, arg)
			expectValue = false
		case arg == "--":
			flags = append(flags, arg)
			afterDoubleDash = true
		case strings.HasPrefix(arg, "--"):
			flags = append(flags, arg)
			name := strings.TrimPrefix(arg, "--")
			if cut := strings.IndexByte(name, '='); cut >= 0 {
				continue
			}
			if _, ok := valueFlagSet[name]; ok {
				expectValue = true
			}
		case strings.HasPrefix(arg, "-"):
			flags = append(flags, arg)
		default:
			positionals = append(positionals, arg)
		}
	}

	return append(flags, positionals...)
}

func optionalReason(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	return &value
}

func rollbackTx(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return nil
	}

	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return nil
	}

	return fmt.Errorf("rollback transaction: %w", rollbackErr)
}

// ─── Queue dispatcher ────────────────────────────────────────────────────────

func runQueue(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printQueueUsage(stderr)
		return fmt.Errorf("queue subcommand required: list, pause, resume, drain, history")
	}

	switch args[0] {
	case "list":
		return runQueueList(ctx, args[1:], stdout, stderr)
	case "pause":
		return runQueuePause(ctx, args[1:], stdout, stderr)
	case "resume":
		return runQueueResume(ctx, args[1:], stdout, stderr)
	case "history":
		return runQueueHistory(ctx, args[1:], stdout, stderr)
	case "drain":
		return runQueueDrain(ctx, args[1:], stdout, stderr)
	default:
		printQueueUsage(stderr)
		return fmt.Errorf("unknown queue subcommand: %s", args[0])
	}
}

func printQueueUsage(w io.Writer) {
	if w == nil {
		return
	}
	_, _ = fmt.Fprintln(w, "usage: lq queue <list|pause|resume|drain|history> [flags]")
}

// ─── queue list ──────────────────────────────────────────────────────────────

// queueStateRow holds one row returned by the queue list query.
type queueStateRow struct {
	QueueName string
	State     string
	Since     time.Time
	Reason    string
}

func runQueueList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("queue list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue list flags: %w", err)
	}
	if len(fs.Args()) != 0 {
		return unexpectedArgsError("queue list", fs.Args())
	}

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	metaTable := cliQualifyIdentifier(schema, "queue_meta")
	statesTable := cliQualifyIdentifier(schema, "queue_states")

	query := queueListQuery(metaTable, statesTable)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query queue states: %w", err)
	}
	defer rows.Close()

	var results []queueStateRow
	for rows.Next() {
		var r queueStateRow
		if err := rows.Scan(&r.QueueName, &r.State, &r.Since, &r.Reason); err != nil {
			return fmt.Errorf("scan queue state row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate queue state rows: %w", err)
	}

	printQueueTable(stdout, results)
	return nil
}

func queueListQuery(metaTable, statesTable string) string {
	return fmt.Sprintf(`
SELECT
    s.queue_name,
    s.state,
    COALESCE(e.changed_at, s.updated_at) AS since,
    COALESCE(e.reason, '') AS reason
FROM %s s
LEFT JOIN LATERAL (
    SELECT changed_at, reason
    FROM %s
    WHERE queue_name = s.queue_name
    ORDER BY changed_at DESC
    LIMIT 1
) e ON true
ORDER BY s.queue_name
`, metaTable, statesTable)
}

func printQueueTable(w io.Writer, rows []queueStateRow) {
	_, _ = fmt.Fprintf(w, "%-30s %-10s %-25s %s\n", "QUEUE", "STATE", "SINCE", "REASON")
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "%-30s %-10s %-25s %s\n",
			r.QueueName,
			r.State,
			r.Since.UTC().Format(time.RFC3339),
			r.Reason,
		)
	}
}

// ─── queue pause / resume (shared helper) ────────────────────────────────────

func runQueuePause(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runQueueStateChange(ctx, args, stdout, stderr, "pause", "paused", "paused")
}

func runQueueResume(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runQueueStateChange(ctx, args, stdout, stderr, "resume", "active", "resumed")
}

// runQueueStateChange is the shared implementation for pause and resume. It
// updates queue_meta.state and inserts a queue_states event in one transaction
// only when the queue is transitioning to a new state.
func runQueueStateChange(
	ctx context.Context,
	args []string,
	stdout, stderr io.Writer,
	command, targetState, verb string,
) (err error) {
	cmdName := "queue " + command
	fs := flag.NewFlagSet(cmdName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq)")
	reasonFlag := fs.String("reason", "", "Reason for the state change (recorded in history)")

	if err := fs.Parse(normalizeInterspersedFlags(args, "database-url", "schema", "reason")); err != nil {
		return fmt.Errorf("parse %s flags: %w", cmdName, err)
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue %s <queue_name>", command)
	}
	if len(positional) > 1 {
		return unexpectedArgsError(cmdName, positional[1:])
	}
	queueName := positional[0]

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := rollbackTx(ctx, tx); rollbackErr != nil {
			if err == nil {
				err = rollbackErr
				return
			}

			err = errors.Join(err, rollbackErr)
		}
	}()

	metaTable := cliQualifyIdentifier(schema, "queue_meta")
	statesTable := cliQualifyIdentifier(schema, "queue_states")

	selectSQL := fmt.Sprintf(
		`SELECT state FROM %s WHERE queue_name = $1 FOR UPDATE`,
		metaTable,
	)
	var currentState string
	if err = tx.QueryRow(ctx, selectSQL, queueName).Scan(&currentState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("queue %q not found", queueName)
		}
		return fmt.Errorf("query %s queue state: %w", command, err)
	}

	if currentState == targetState {
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", command, err)
		}
		_, _ = fmt.Fprintf(stdout, "%s queue %q\n", verb, queueName)
		return nil
	}

	updateSQL := fmt.Sprintf(
		`UPDATE %s SET state = $1, updated_at = NOW() WHERE queue_name = $2`,
		metaTable,
	)
	tag, err := tx.Exec(ctx, updateSQL, targetState, queueName)
	if err != nil {
		return fmt.Errorf("%s queue: %w", command, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s queue %q: expected 1 updated row, got %d", command, queueName, tag.RowsAffected())
	}

	reason := optionalReason(*reasonFlag)

	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (queue_name, state, reason) VALUES ($1, $2, $3)`,
		statesTable,
	)
	if _, err = tx.Exec(ctx, insertSQL, queueName, targetState, reason); err != nil {
		return fmt.Errorf("record %s event: %w", command, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", command, err)
	}

	_, _ = fmt.Fprintf(stdout, "%s queue %q\n", verb, queueName)
	return nil
}

// ─── queue history ───────────────────────────────────────────────────────────

// queueEventRow holds one row returned by the queue history query.
type queueEventRow struct {
	State     string
	ChangedAt time.Time
	Reason    string
}

func runQueueHistory(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("queue history", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq)")
	limitFlag := fs.Int("limit", 20, "Maximum number of events to display")

	if err := fs.Parse(normalizeInterspersedFlags(args, "database-url", "schema", "limit")); err != nil {
		return fmt.Errorf("parse queue history flags: %w", err)
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue history <queue_name>")
	}
	if len(positional) > 1 {
		return unexpectedArgsError("queue history", positional[1:])
	}
	if *limitFlag < 0 {
		return fmt.Errorf("invalid --limit value %d: must be >= 0", *limitFlag)
	}
	queueName := positional[0]

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	statesTable := cliQualifyIdentifier(schema, "queue_states")

	query := fmt.Sprintf(
		`SELECT state, changed_at, COALESCE(reason, '') AS reason
         FROM %s
         WHERE queue_name = $1
         ORDER BY changed_at DESC
         LIMIT $2`,
		statesTable,
	)

	rows, err := pool.Query(ctx, query, queueName, *limitFlag)
	if err != nil {
		return fmt.Errorf("query queue history: %w", err)
	}
	defer rows.Close()

	var results []queueEventRow
	for rows.Next() {
		var r queueEventRow
		if err := rows.Scan(&r.State, &r.ChangedAt, &r.Reason); err != nil {
			return fmt.Errorf("scan queue history row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate queue history rows: %w", err)
	}

	printQueueHistory(stdout, results)
	return nil
}

func printQueueHistory(w io.Writer, rows []queueEventRow) {
	_, _ = fmt.Fprintf(w, "%-10s %-25s %s\n", "STATE", "CHANGED AT", "REASON")
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "%-10s %-25s %s\n",
			r.State,
			r.ChangedAt.UTC().Format(time.RFC3339),
			r.Reason,
		)
	}
}

// ─── queue drain ─────────────────────────────────────────────────────────────

func runQueueDrain(ctx context.Context, args []string, stdout, stderr io.Writer) (err error) {
	fs := flag.NewFlagSet("queue drain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq)")
	reasonFlag := fs.String("reason", "", "Reason for draining (recorded in event history)")

	if err := fs.Parse(normalizeInterspersedFlags(args, "database-url", "schema", "reason")); err != nil {
		return fmt.Errorf("parse queue drain flags: %w", err)
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue drain <queue_name>")
	}
	if len(positional) > 1 {
		return unexpectedArgsError("queue drain", positional[1:])
	}
	queueName := positional[0]

	databaseURL := firstNonEmpty(*databaseURLFlag, os.Getenv("DATABASE_URL"))
	schema := firstNonEmpty(*schemaFlag, os.Getenv("LITEQ_SCHEMA"), "liteq")

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}
	// Queue name is used as a table name in the DELETE, so it must also pass
	// identifier validation to prevent SQL injection.
	if err := cliValidateIdentifier("queue name", queueName); err != nil {
		return err
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := rollbackTx(ctx, tx); rollbackErr != nil {
			if err == nil {
				err = rollbackErr
				return
			}

			err = errors.Join(err, rollbackErr)
		}
	}()

	metaTable := cliQualifyIdentifier(schema, "queue_meta")
	statesTable := cliQualifyIdentifier(schema, "queue_states")
	queueTable := cliQualifyIdentifier(schema, queueName)

	// 1. Set state to draining.
	updateDrainingSQL := fmt.Sprintf(
		`UPDATE %s SET state = 'draining', updated_at = NOW() WHERE queue_name = $1`,
		metaTable,
	)
	tag, err := tx.Exec(ctx, updateDrainingSQL, queueName)
	if err != nil {
		return fmt.Errorf("set queue draining: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("queue %q not found", queueName)
	}

	// 2. Record draining event.
	reason := optionalReason(*reasonFlag)

	insertEventSQL := fmt.Sprintf(
		`INSERT INTO %s (queue_name, state, reason) VALUES ($1, $2, $3)`,
		statesTable,
	)
	if _, err = tx.Exec(ctx, insertEventSQL, queueName, "draining", reason); err != nil {
		return fmt.Errorf("record draining event: %w", err)
	}

	// 3. Delete all entries from the queue table.
	deleteSQL := fmt.Sprintf(`DELETE FROM %s`, queueTable)
	deleteTag, err := tx.Exec(ctx, deleteSQL)
	if err != nil {
		return fmt.Errorf("drain queue entries: %w", err)
	}
	count := deleteTag.RowsAffected()

	// 4. Set state to paused.
	updatePausedSQL := fmt.Sprintf(
		`UPDATE %s SET state = 'paused', updated_at = NOW() WHERE queue_name = $1`,
		metaTable,
	)
	if _, err = tx.Exec(ctx, updatePausedSQL, queueName); err != nil {
		return fmt.Errorf("set queue paused: %w", err)
	}

	// 5. Record paused event with "drain complete" reason.
	drainCompleteReason := "drain complete"
	if _, err = tx.Exec(ctx, insertEventSQL, queueName, "paused", drainCompleteReason); err != nil {
		return fmt.Errorf("record drain complete event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit drain: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "drained queue %q (%d entries removed)\n", queueName, count)
	return nil
}
