# liteq

[![CI](https://github.com/crazzyghost/liteq.go/actions/workflows/ci.yml/badge.svg)](https://github.com/crazzyghost/liteq.go/actions/workflows/ci.yml)

**A PostgreSQL-backed task queue for Go that eliminates extra infrastructure.**

If your app already runs on Postgres, liteq gives you background jobs, retries, dead-letter queues, and operational controls—without Redis, brokers, or coordination services.

## Why liteq

**Fewer moving parts**  
No Redis cluster to maintain, no message broker to monitor. One database, one dependency.

**Simple mental model**  
Create a queue → enqueue tasks → run workers. The entire API fits on one screen.

**Production-ready defaults**  
Exponential backoff retries, dead-letter queues, transactional enqueue, worker timeouts, pause/resume/drain—already built in.

**Debuggable when things go wrong**  
Tasks are rows in Postgres. Query them with SQL. Replay failed jobs from the DLQ. No black boxes.

**CLI for operations**  
Create queues, pause traffic, drain backlogs, inspect history—all from your terminal with `lq`.

## When to use liteq

**Good fit:**
- You're already on PostgreSQL and want to avoid adding Redis/RabbitMQ
- You need reliable background jobs with retries and dead-letter handling
- You want transactional task enqueue (task commits only if your transaction commits)
- You value operational simplicity and SQL-based debugging
- You need cross-language support (any language with a Postgres driver can implement the [protocol spec](spec/))

**Not a fit:**
- You need advanced routing patterns (topic exchanges, fanout, priority queues)
- You're pushing extreme throughput and latency is critical (sub-millisecond response times)
- You already have a mature Redis/SQS setup with no operational pain

## How it works

```
Producer → Enqueue(task) → PostgreSQL table → Worker polls → Consume(task)
                                  ↓
                            Retry on error
                                  ↓
                            Max retries → Dead Letter Queue
```

Tasks are rows in a Postgres table. Workers poll for available tasks, process them with your `Consumer`, and handle retries/failures automatically. Everything uses transactions—enqueue is atomic with your business logic.

## Install

```bash
go get github.com/crazzyghost/liteq.go@latest
```

**CLI:**

Download pre-built binary from [GitHub Releases](https://github.com/crazzyghost/liteq.go/releases):

```bash
# macOS (Apple Silicon)
curl -sL https://github.com/crazzyghost/liteq.go/releases/download/<version>/lq_darwin_arm64.tar.gz | tar xz
sudo mv lq /usr/local/bin/

# macOS (Intel)
curl -sL https://github.com/crazzyghost/liteq.go/releases/download/<version>/lq_darwin_amd64.tar.gz | tar xz
sudo mv lq /usr/local/bin/

# Linux (x86_64)
curl -sL https://github.com/crazzyghost/liteq.go/releases/download/<version>/lq_linux_amd64.tar.gz | tar xz
sudo mv lq /usr/local/bin/
```

Or install from source:
```bash
go install github.com/crazzyghost/liteq.go/cmd/lq@latest
```

**Requirements:** Go 1.25+ • PostgreSQL 12+

## Quick start

**1. Setup**
```bash
export LITEQ_DATABASE_URL="postgres://localhost:5432/app?sslmode=disable"
lq queue create email_jobs  # Creates email_jobs + email_jobs_dead_letter
```

**2. Enqueue a task**
```go
import lq "github.com/crazzyghost/liteq.go"

pool, _ := pgxpool.New(ctx, os.Getenv("LITEQ_DATABASE_URL"))
jobs, _ := lq.NewPgQueue(ctx, pool, "email_jobs", &lq.RetryPolicy{
    Strategy: lq.StrategyExponential, MaxRetries: 3,
}, lq.WithSchema("liteq"))

tx, txCtx, cancel, _ := jobs.BeginTx(ctx)
defer cancel()

jobs.Enqueue(lq.Task{
    // ID is optional. PostgreSQL generates a UUID when it is omitted.
    Data: map[string]any{"email": "ada@example.com"},
}, tx)

tx.Commit(txCtx)
```

**3. Process tasks**
```go
type EmailConsumer struct{}

func (EmailConsumer) Consume(ctx context.Context, task lq.Task) error {
    // Send email using task.Data
    return nil  // or return error to retry
}

worker, _ := lq.NewWorker(ctx, &lq.WorkerConfig{
    TaskQueue:       jobs,
    DeadLetterQueue: dlq,
    GetTaskRetryPolicy: func(t lq.Task) (*lq.RetryPolicy, error) {
        return t.RetryPolicy, nil
    },
})

worker.Run(ctx, func() lq.Consumer { return EmailConsumer{} }, 2*time.Second)
```

That's it. Worker polls every 2 seconds, processes tasks concurrently, retries on failure, and moves exhausted tasks to the DLQ.

## Key features

### Transactional enqueue
Tasks commit only if your business transaction commits. No orphaned jobs.
```go
tx, _ := pool.Begin(ctx)
saveUser(tx)  // Your business logic
jobs.Enqueue(task, tx)  // Enqueued atomically
tx.Commit(ctx)
```

### Smart retries
Return an error → task retries with exponential backoff. Return `ConsumerError{IsNonTransient: true}` → straight to DLQ.
```go
func (c Consumer) Consume(ctx context.Context, task lq.Task) error {
    if missingData(task) {
        return &lq.ConsumerError{IsNonTransient: true}  // Skip retries
    }
    return doWork(task)  // Retries on error
}
```

### Operational controls
```bash
lq queue pause email_jobs --reason "maintenance"
lq queue drain email_jobs --reason "clear backlog"
lq queue resume email_jobs
lq queue history email_jobs
```

### Auto-migrate option
Skip the CLI—let the app create its own queues:
```go
jobs, _ := lq.NewPgQueue(ctx, pool, "email_jobs", policy, 
    lq.WithAutoMigrate())
```

### Observability hooks
```go
Hooks: lq.SlogHooks{Logger: slog.Default()}  // Structured logs
```
Or build custom hooks for metrics/tracing by embedding `BaseHooks`.

### Sane defaults
| Setting | Default | Tweak when |
|---------|---------|------------|
| `TaskBatchSize` | 10 | You want bigger/smaller poll batches |
| `MaxConcurrency` | `runtime.NumCPU()` | Downstream has rate limits |
| `TaskTimeout` | 30s | Tasks are faster/slower |

## CLI reference

| Command | Purpose |
|---------|---------|
| `lq queue create <name>` | Create queue + DLQ |
| `lq queue ls` | List all queues |
| `lq queue pause <name> --reason <msg>` | Stop processing |
| `lq queue resume <name>` | Re-enable queue |
| `lq queue drain <name> --reason <msg>` | Delete pending tasks |
| `lq queue history <name>` | Show state changes |
| `lq queue rm <name> --force` | Delete queue |

**Flags:** `--database-url`, `--schema`, `--dry-run`

Reads from `LITEQ_DATABASE_URL` and `LITEQ_SCHEMA` env vars by default.

## Core API

**Queue setup**
```go
lq.NewPgQueue(ctx, pool, queueName, retryPolicy, options...)
```
Options: `WithSchema(name)`, `WithAutoMigrate()`, `WithDeadLetterQueue(dlqName)`

**Worker setup**
```go
lq.NewWorker(ctx, &WorkerConfig{
    TaskQueue, DeadLetterQueue, GetTaskRetryPolicy, Hooks,
    TaskBatchSize, MaxConcurrency, TaskTimeout,
})
worker.Run(ctx, consumerFactory, pollInterval)
```

**Consumer interface**
```go
type Consumer interface {
    Consume(ctx context.Context, task Task) error
}
```

**Task struct**
```go
type Task struct {
    ID          string
    Data        map[string]any  // JSON-friendly data
    Status      string
    RetryPolicy *RetryPolicy
    // ... metadata fields
}
```

**Retry policy**
```go
type RetryPolicy struct {
    Strategy     RetryStrategy  // StrategyExponential | StrategyLinear
    MaxRetries   int
    RetryDelayMs int64
    MaxDelayMs   int64
}
```

**Queue operations**
```go
queue.Enqueue(task, tx)
queue.Dequeue(ctx, limit, lockFor, tx)
queue.Pause(ctx)
queue.Resume(ctx)
queue.Drain(ctx)
```

**Hooks**
- `SlogHooks{Logger}` - Structured logging via slog
- `BaseHooks{}` - Embed and override for custom metrics/tracing

## Important notes

- **Task.Data must be JSON-serializable** - Use `map[string]any` or structs, not channels/functions
- **Workers need both queues** - Pass the main queue and DLQ to `WorkerConfig`
- **Names are snake_case** - Queue/schema names: letters, numbers, underscores only
- **Transactional safety** - Always enqueue within a transaction for atomicity

## Comparison to alternatives

| | liteq | Sidekiq/BullMQ | AWS SQS |
|---|---|---|---|
| **Infrastructure** | PostgreSQL only | Redis required | Managed service |
| **Language support** | Any (protocol spec) | Ruby/Node/multi | Any (HTTP API) |
| **Transactional enqueue** | ✅ Yes | ❌ No | ❌ No |
| **Query tasks with SQL** | ✅ Yes | ❌ No | ❌ No |
| **Operational complexity** | Low (1 DB) | Medium (DB + Redis) | Low (managed) |
| **Cost model** | DB only | DB + Redis infra | Per-request pricing |
| **Best for** | Transactional jobs | High throughput | Decoupled services |

Choose liteq when you value **operational simplicity** and **transactional guarantees**. Implement clients in any language by following the [SQL operation contracts](spec/).
