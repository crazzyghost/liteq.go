package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
	liteq "liteq.go"
)

// cliIdentifierPattern mirrors schema_manager.identifierPattern. It is
// duplicated here because qualifyIdentifier is unexported in the liteq package.
// Any identifier interpolated into SQL (schema, table names) must match.
var cliIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

var queryQueueStateRows = loadQueueStateRows

var removalPromptReader io.Reader = os.Stdin

var removalPromptIsInteractive = func(r io.Reader) bool {
	file, ok := r.(*os.File)
	if !ok {
		return false
	}

	info, err := file.Stat()
	if err != nil {
		return false
	}

	return info.Mode()&os.ModeCharDevice != 0
}

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

func newCommandFlagSet(
	name string,
	parseOutput, usageOutput io.Writer,
	description, usage string,
) (flagSet *pflag.FlagSet, helpFlag *bool) {
	flagSet = pflag.NewFlagSet(name, pflag.ContinueOnError)
	flagSet.SetOutput(parseOutput)
	helpFlag = flagSet.BoolP("help", "h", false, "Print help and exit")
	flagSet.Usage = func() {
		if usageOutput == nil {
			return
		}

		_, _ = fmt.Fprintln(usageOutput, description)
		_, _ = fmt.Fprintln(usageOutput)
		_, _ = fmt.Fprintln(usageOutput, "Usage:")
		_, _ = fmt.Fprintf(usageOutput, "  %s\n", usage)
		_, _ = fmt.Fprintln(usageOutput)
		_, _ = fmt.Fprintln(usageOutput, "Flags:")

		prevOutput := flagSet.Output()
		flagSet.SetOutput(usageOutput)
		flagSet.PrintDefaults()
		flagSet.SetOutput(prevOutput)
	}

	return flagSet, helpFlag
}

func runQueue(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := pflag.NewFlagSet("queue", pflag.ContinueOnError)
	fs.SetInterspersed(false)
	fs.SetOutput(stderr)
	helpFlag := fs.BoolP("help", "h", false, "Print help and exit")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue flags: %w", err)
	}

	remaining := fs.Args()
	if *helpFlag || (len(remaining) > 0 && remaining[0] == "help") {
		printHelp(stdout)
		return nil
	}

	if len(remaining) == 0 {
		printHelp(stderr)
		return fmt.Errorf("queue subcommand required: create, rm, ls, pause, resume, drain, history")
	}

	switch remaining[0] {
	case "create":
		return runQueueCreate(ctx, remaining[1:], stdout, stderr)
	case "rm":
		return runQueueRm(ctx, remaining[1:], stdout, stderr)
	case "ls":
		return runQueueList(ctx, remaining[1:], stdout, stderr)
	case "pause":
		return runQueuePause(ctx, remaining[1:], stdout, stderr)
	case "resume":
		return runQueueResume(ctx, remaining[1:], stdout, stderr)
	case "history":
		return runQueueHistory(ctx, remaining[1:], stdout, stderr)
	case "drain":
		return runQueueDrain(ctx, remaining[1:], stdout, stderr)
	default:
		printQueueUsage(stderr)
		return fmt.Errorf("unknown queue subcommand: %s", remaining[0])
	}
}

func printQueueUsage(w io.Writer) {
	if w == nil {
		return
	}

	_, _ = fmt.Fprintln(w, "usage: lq queue <create|rm|ls|pause|resume|drain|history> [flags]")
}

func runQueueCreate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, helpFlag := newCommandFlagSet(
		"queue create",
		stderr,
		stdout,
		"Create a queue by applying pending schema migrations.",
		"lq queue create [flags] <queue_name>",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	dlqFlag := fs.String("dlq", "", "Dead-letter queue name")
	noDLQFlag := fs.Bool("no-dlq", false, "Do not create a dead-letter queue")
	dryRunFlag := fs.Bool("dry-run", false, "Print SQL without executing")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue create flags: %w", err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue create <queue_name>")
	}
	if len(positional) > 1 {
		return unexpectedArgsError("queue create", positional[1:])
	}
	queueName := positional[0]

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)
	dlqName := strings.TrimSpace(*dlqFlag)

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}
	if err := cliValidateIdentifier("queue name", queueName); err != nil {
		return err
	}
	if *noDLQFlag && dlqName != "" {
		return fmt.Errorf("--dlq and --no-dlq are mutually exclusive")
	}
	if dlqName != "" {
		if err := cliValidateIdentifier("dlq name", dlqName); err != nil {
			return err
		}
	}

	opts := []liteq.SchemaManagerOption{liteq.WithSchemaManagerSchema(schema)}
	if *dryRunFlag {
		opts = append(opts, liteq.WithDryRun(stdout))
	}

	var pool *pgxpool.Pool
	if !*dryRunFlag {
		var err error
		pool, err = requirePool(ctx, databaseURL)
		if err != nil {
			return err
		}
		if pool != nil {
			defer pool.Close()
		}
	}

	queues := []liteq.QueueDefinition{{Name: queueName, DLQName: dlqName, DisableDLQ: *noDLQFlag}}
	migrator := newSchemaMigrator(pool, opts...)
	if *dryRunFlag {
		if err := migrator.Migrate(ctx, queues); err != nil {
			return err
		}
	} else {
		appliedAny, err := migrator.MigrateWithStatus(ctx, queues)
		if err != nil {
			return err
		}
		if !appliedAny {
			_, _ = fmt.Fprintf(stdout, "queue %q is up to date\n", queueName)
			return nil
		}
	}

	if dlqName != "" {
		_, _ = fmt.Fprintf(stdout, "created queue %q with dlq %q\n", queueName, dlqName)
		return nil
	}

	_, _ = fmt.Fprintf(stdout, "created queue %q\n", queueName)
	return nil
}

func runQueueRm(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, helpFlag := newCommandFlagSet(
		"queue rm",
		stderr,
		stdout,
		"Remove a queue by rolling back applied schema migrations.",
		"lq queue rm [flags] <queue_name>",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	stepsFlag := fs.String("steps", "all", "Migrations to roll back, or 'all'")
	dryRunFlag := fs.Bool("dry-run", false, "Print SQL without executing")
	forceFlag := fs.Bool("force", false, "Skip confirmation prompt")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue rm flags: %w", err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue rm <queue_name>")
	}
	if len(positional) > 1 {
		return unexpectedArgsError("queue rm", positional[1:])
	}
	queueName := positional[0]

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}
	if err := cliValidateIdentifier("queue name", queueName); err != nil {
		return err
	}

	rollbackAll, steps, err := parseSteps(*stepsFlag)
	if err != nil {
		return err
	}

	if !*dryRunFlag && !*forceFlag {
		if !confirmRemoval(stderr, removalPromptReader, queueName) {
			_, _ = fmt.Fprintln(stdout, "aborted")
			return nil
		}
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		if *dryRunFlag {
			return fmt.Errorf("queue rm dry-run requires a database connection to inspect applied migrations: %w", err)
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
		if err := migrator.MigrateDownQueueAll(ctx, queueName); err != nil {
			return err
		}
	} else {
		if err := migrator.MigrateDownQueue(ctx, queueName, steps); err != nil {
			return err
		}
	}

	_, _ = fmt.Fprintf(stdout, "removed queue %q\n", queueName)
	return nil
}

func confirmRemoval(w io.Writer, r io.Reader, queueName string) bool {
	if w != nil {
		_, _ = fmt.Fprintf(w, "remove queue %q? This will drop the table and all data. [y/N] ", queueName)
	}

	if !removalPromptIsInteractive(r) {
		return false
	}

	var answer string
	if _, err := fmt.Fscanln(r, &answer); err != nil {
		return false
	}

	return strings.EqualFold(strings.TrimSpace(answer), "y")
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

// queueStateRow holds one row returned by the queue list query.
type queueStateRow struct {
	QueueName string
	State     string
	Since     time.Time
	Reason    string
}

func runQueueList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, helpFlag := newCommandFlagSet(
		"queue ls",
		stderr,
		stdout,
		"List all queues and their current state.",
		"lq queue ls [flags]",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	outputFlag := fs.String("output", "table", "Output format: table or json")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue ls flags: %w", err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}
	if len(fs.Args()) != 0 {
		return unexpectedArgsError("queue ls", fs.Args())
	}

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)

	if err := cliValidateIdentifier("schema", schema); err != nil {
		return err
	}

	switch *outputFlag {
	case "table", "json":
	default:
		return fmt.Errorf("invalid output format %q: must be \"table\" or \"json\"", *outputFlag)
	}

	pool, err := requirePool(ctx, databaseURL)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}

	results, err := queryQueueStateRows(ctx, pool, schema)
	if err != nil {
		return err
	}

	switch *outputFlag {
	case "table":
		printQueueTable(stdout, results)
	default:
		if err := printQueueJSON(stdout, results); err != nil {
			return err
		}
	}

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

func loadQueueStateRows(ctx context.Context, pool *pgxpool.Pool, schema string) ([]queueStateRow, error) {
	metaTable := cliQualifyIdentifier(schema, "queue_meta")
	statesTable := cliQualifyIdentifier(schema, "queue_states")

	rows, err := pool.Query(ctx, queueListQuery(metaTable, statesTable))
	if err != nil {
		return nil, fmt.Errorf("query queue states: %w", err)
	}
	defer rows.Close()

	var results []queueStateRow
	for rows.Next() {
		var row queueStateRow
		if err := rows.Scan(&row.QueueName, &row.State, &row.Since, &row.Reason); err != nil {
			return nil, fmt.Errorf("scan queue state row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queue state rows: %w", err)
	}

	return results, nil
}

func printQueueTable(w io.Writer, rows []queueStateRow) {
	_, _ = fmt.Fprintf(w, "%-30s %-10s %-25s %s\n", "QUEUE", "STATE", "SINCE", "REASON")
	for _, row := range rows {
		_, _ = fmt.Fprintf(
			w,
			"%-30s %-10s %-25s %s\n",
			row.QueueName,
			row.State,
			row.Since.UTC().Format(time.RFC3339),
			row.Reason,
		)
	}
}

func printQueueJSON(w io.Writer, rows []queueStateRow) error {
	type jsonRow struct {
		Queue  string `json:"queue"`
		State  string `json:"state"`
		Since  string `json:"since"`
		Reason string `json:"reason"`
	}

	out := make([]jsonRow, len(rows))
	for i, row := range rows {
		out[i] = jsonRow{
			Queue:  row.QueueName,
			State:  row.State,
			Since:  row.Since.UTC().Format(time.RFC3339),
			Reason: row.Reason,
		}
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		return fmt.Errorf("encode queue list json: %w", err)
	}

	return nil
}

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
	fs, helpFlag := newCommandFlagSet(
		cmdName,
		stderr,
		stdout,
		strings.ToUpper(command[:1])+command[1:]+" a queue.",
		"lq "+cmdName+" [flags] <queue_name>",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	reasonFlag := fs.String("reason", "", "Reason for the state change (recorded in history)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse %s flags: %w", cmdName, err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue %s <queue_name>", command)
	}
	if len(positional) > 1 {
		return unexpectedArgsError(cmdName, positional[1:])
	}
	queueName := positional[0]

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)

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

	selectSQL := fmt.Sprintf(`SELECT state FROM %s WHERE queue_name = $1 FOR UPDATE`, metaTable)

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

	updateSQL := fmt.Sprintf(`UPDATE %s SET state = $1, updated_at = NOW() WHERE queue_name = $2`, metaTable)
	tag, err := tx.Exec(ctx, updateSQL, targetState, queueName)
	if err != nil {
		return fmt.Errorf("%s queue: %w", command, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s queue %q: expected 1 updated row, got %d", command, queueName, tag.RowsAffected())
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (queue_name, state, reason) VALUES ($1, $2, $3)`, statesTable)
	if _, err = tx.Exec(ctx, insertSQL, queueName, targetState, optionalReason(*reasonFlag)); err != nil {
		return fmt.Errorf("record %s event: %w", command, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", command, err)
	}

	_, _ = fmt.Fprintf(stdout, "%s queue %q\n", verb, queueName)
	return nil
}

// queueEventRow holds one row returned by the queue history query.
type queueEventRow struct {
	State     string
	ChangedAt time.Time
	Reason    string
}

func runQueueHistory(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, helpFlag := newCommandFlagSet(
		"queue history",
		stderr,
		stdout,
		"Show queue state change history.",
		"lq queue history [flags] <queue_name>",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	limitFlag := fs.Int("limit", 20, "Maximum number of events to display")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue history flags: %w", err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
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

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)

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
		var row queueEventRow
		if err := rows.Scan(&row.State, &row.ChangedAt, &row.Reason); err != nil {
			return fmt.Errorf("scan queue history row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate queue history rows: %w", err)
	}

	printQueueHistory(stdout, results)
	return nil
}

func printQueueHistory(w io.Writer, rows []queueEventRow) {
	_, _ = fmt.Fprintf(w, "%-10s %-25s %s\n", "STATE", "CHANGED AT", "REASON")
	for _, row := range rows {
		_, _ = fmt.Fprintf(
			w,
			"%-10s %-25s %s\n",
			row.State,
			row.ChangedAt.UTC().Format(time.RFC3339),
			row.Reason,
		)
	}
}

func runQueueDrain(ctx context.Context, args []string, stdout, stderr io.Writer) (err error) {
	fs, helpFlag := newCommandFlagSet(
		"queue drain",
		stderr,
		stdout,
		"Drain and pause a queue.",
		"lq queue drain [flags] <queue_name>",
	)
	databaseURLFlag := fs.String("database-url", "", "Postgres connection string (env: LITEQ_DATABASE_URL)")
	schemaFlag := fs.String("schema", "", "Target Postgres schema (default: liteq, env: LITEQ_SCHEMA)")
	reasonFlag := fs.String("reason", "", "Reason for draining (recorded in event history)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse queue drain flags: %w", err)
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	positional := fs.Args()
	if len(positional) == 0 {
		return fmt.Errorf("queue name is required: lq queue drain <queue_name>")
	}
	if len(positional) > 1 {
		return unexpectedArgsError("queue drain", positional[1:])
	}
	queueName := positional[0]

	databaseURL := resolveDatabaseURL(*databaseURLFlag)
	schema := resolveSchema(*schemaFlag)

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

	insertEventSQL := fmt.Sprintf(
		`INSERT INTO %s (queue_name, state, reason) VALUES ($1, $2, $3)`,
		statesTable,
	)
	if _, err = tx.Exec(ctx, insertEventSQL, queueName, "draining", optionalReason(*reasonFlag)); err != nil {
		return fmt.Errorf("record draining event: %w", err)
	}

	deleteSQL := fmt.Sprintf(`DELETE FROM %s`, queueTable)
	deleteTag, err := tx.Exec(ctx, deleteSQL)
	if err != nil {
		return fmt.Errorf("drain queue entries: %w", err)
	}
	count := deleteTag.RowsAffected()

	updatePausedSQL := fmt.Sprintf(
		`UPDATE %s SET state = 'paused', updated_at = NOW() WHERE queue_name = $1`,
		metaTable,
	)
	if _, err = tx.Exec(ctx, updatePausedSQL, queueName); err != nil {
		return fmt.Errorf("set queue paused: %w", err)
	}

	if _, err = tx.Exec(ctx, insertEventSQL, queueName, "paused", "drain complete"); err != nil {
		return fmt.Errorf("record drain complete event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit drain: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "drained queue %q (%d entries removed)\n", queueName, count)
	return nil
}
