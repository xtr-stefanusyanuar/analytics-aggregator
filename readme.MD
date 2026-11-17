# High-Performance Go Ingestion Pipeline

## Overview

This project is a highly concurrent, resilient, and backpressure-aware event ingestion pipeline written in Go. It is built using **Hexagonal Architecture** (Ports and Adapters) to strictly decouple the core domain logic from external infrastructure (HTTP handlers, PostgreSQL, and 3rd-party APIs).

The pipeline is designed to absorb massive HTTP traffic spikes, asynchronously enrich data via external APIs with exponential backoff, and batch-insert the results into a database—all while guaranteeing **zero data loss** during rolling deployments or unexpected pod terminations.

## Core Architectural Patterns

- **Hexagonal Architecture:** The `domain.Event` remains pure. Infrastructure concerns are isolated to outer adapters.
- **Asynchronous Fire-and-Forget:** HTTP handlers immediately return `202 Accepted` after safely handing events off to the internal pipeline, preventing slow downstream APIs from holding HTTP connections hostage.
- **Fan-Out / Fan-In:** A single dispatcher routes traffic to a pool of concurrent `EnricherWorker` goroutines (Fan-Out), which then push enriched data to a single `batchInsert` worker (Fan-In) for optimized database writes.

## Advanced Go Concurrency Learnings

Building this pipeline required navigating several complex edge cases in Go's concurrency model:

### 1. Graceful Shutdown & The "Dying Breath" Pattern

- **Tiered Shutdowns:** Using a single global context for graceful shutdown causes a "Simultaneous Shutdown Deadlock" where workers try to flush data to channels that have already been closed by other shutting-down workers. **Solution:** Split the shutdown into tiers—stop HTTP ingress first, wait for in-flight requests to drain, and _then_ signal the internal workers to exit.
- **The "Last Gasp" Context:** When a worker detects a shutdown (`<-ctx.Done()`), the parent context is already dead. To make one final network or channel call (like saving an event to a Dead Letter Queue), a fresh `context.WithTimeout(context.Background(), 1*time.Second)` must be created to give the operation a strict window to succeed.
- **Pseudo-Random Select:** If multiple channels in a `select` block are ready simultaneously, Go picks one at random. Relying on channel buffers during shutdown without strict context management results in a 50/50 coin toss of losing data.

### 2. Channel Safety & Backpressure

- **Naked Sends are Deadlocks:** Sending data to a channel (`myChan <- data`) without wrapping it in a `select` block that also listens for `ctx.Done()` is a critical risk. If the consumer on the other side crashes or shuts down, the producer will block forever, causing a goroutine leak.
- **True Backpressure:** Simply using `time.After` in a `select` block with a channel send does not create backoff; it creates a constant-pressure loop. True backpressure requires separating the send attempt (with a short timeout to handle micro-bursts) from the backoff sleep (yielding the CPU when the system is genuinely saturated).

### 3. Context Management & Timers

- **Context Immutability:** A `context.Context` cannot be reset. Once it times out, any standard library function (`http.Client`, `sql.DB`) that receives it will instantly fail. Retries must always generate a _fresh_ context per attempt.
- **The Undead Context:** Using `context.WithoutCancel` blindly strips all cancellation awareness, making goroutines deaf to OS-level `SIGTERM` signals and preventing the application from shutting down.
- **Memory-Safe Batching:** Modern Go slice reuse (`clear(events)` followed by `events = events[:0]`) prevents the Garbage Collector from trashing memory during high-volume batch processing.
- **Ticker Leaks:** `time.NewTicker` must always be paired with `defer ticker.Stop()` to prevent background timers from leaking when the goroutine exits. Ticker channels must also be actively drained if work takes longer than the tick interval to prevent stale phantom ticks.

## System Design: In-Memory Pipeline vs. Message Broker

While this in-memory pipeline acts similarly to an external message broker (like Kafka or RabbitMQ), it optimizes for different constraints:

**Why use this Go Pipeline?**

- **Ultra-Low Latency:** Nanosecond handoffs between stages in memory.
- **Cost & Simplicity:** Zero external infrastructure to deploy, monitor, or pay for.
- **Intra-Service Decoupling:** Perfect for isolating slow I/O (API calls) from fast I/O (HTTP ingestion) within a single bounded context.

**When to migrate to a Message Broker:**

- **Fan-Out Ratio > 1:** When multiple distinct microservices need to react to the exact same event.
- **OOM Kills:** If the channel buffers require so much memory during traffic spikes that the Kubernetes pod runs out of RAM.
- **Shutdown Thresholds:** If the "dying breath" pipeline drain consistently takes longer than Kubernetes' graceful termination grace period (e.g., >30 seconds), risking data destruction via `SIGKILL`.

## Project Structure (Hexagonal Architecture)

The codebase is strictly organized following Ports and Adapters to ensure the core domain remains completely agnostic of the transport layer (HTTP) and infrastructure (PostgreSQL, External APIs).

```text
.
├── cmd/
│   └── api/
│       └── main.go           # The Composition Root. Initializes contexts, wires adapters to ports, and orchestrates tiered graceful shutdown.
│
├── internal/
│   ├── config/               # Environment variables and application-wide configurations (e.g., WorkerPoolSize, BatchSize).
│   │
│   ├── core/                 # The absolute center of the hexagon. No external dependencies allowed here.
│   │   ├── domain/           # Core business entities (e.g., `Event` struct) and domain-specific errors.
│   │   ├── port/             # Interfaces defining what the core needs (e.g., `TxManager`, `DataEnricher`, `PipelineRepository`).
│   │   └── service/          # Business logic and use cases. Contains `PipelineService`, `EnricherWorker`, the dispatcher, and batching engines.
│   │
│   └── adapter/              # The outer ring. Implementations of the ports.
│       ├── handler/          # Driving Adapters (Left Side): HTTP REST handlers and routers.
│       ├── extapi/           # Driven Adapters (Right Side): External HTTP clients (e.g., IP Enrichment API).
│       ├── repository/       # Driven Adapters (Right Side): Concrete database logic and Unit of Work (UoW) implementation.
│       └── db/               # Infrastructure setup: PostgreSQL connection pooling and configuration.
│
├── db/
│   ├── migrations/           # SQL schema (run 001_init.up.sql once to create the tables).
│   └── queries/              # SQL queries compiled by sqlc into internal/adapter/repository/sqlc.
│
├── api-collection/           # Bruno collection for sending test events.
├── .env.example              # Template for your local .env file.
├── go.mod
└── go.sum
```

## Getting Started (Local, without Docker)

You only need Go, PostgreSQL and (optionally) pgAdmin 4. Docker is **not** required; `docker-compose.yaml` is just an alternative.

### 1. Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| [Go](https://go.dev/dl/) | 1.24+ | Check with `go version`. |
| [PostgreSQL](https://www.postgresql.org/download/) | 16 recommended | The Windows/macOS installers can also install pgAdmin 4. Remember the password you set for the `postgres` superuser. |
| [pgAdmin 4](https://www.pgadmin.org/download/) | any | Optional GUI for managing the database. |

Make sure the PostgreSQL service is running (Windows: *Services → postgresql-x64-16*; macOS: `brew services start postgresql@16`; Linux: `sudo systemctl start postgresql`).

### 2. Create the database

Create a login role `analytics` and a database `analytics_aggregator` owned by it. Pick **one** of the two options.

**Option A: pgAdmin 4**

1. Open pgAdmin 4 and connect to your local server (`localhost`, port `5432`, user `postgres`).
2. Right-click **Login/Group Roles → Create → Login/Group Role…**
   - *General* tab → Name: `analytics`
   - *Definition* tab → Password: `analytics` (or your own)
   - *Privileges* tab → enable **Can login?**
   - Save.
3. Right-click **Databases → Create → Database…**
   - Database: `analytics_aggregator`, Owner: `analytics` → Save.
4. Select the new `analytics_aggregator` database, open **Tools → Query Tool**, use **Open File** to load [`db/migrations/001_init.up.sql`](./db/migrations/001_init.up.sql), and press **Execute (F5)**.
5. Refresh **Schemas → public → Tables**. You should see `events` and `dead_letter_events`.

> The tables must be created while connected as `analytics` (or granted to it). If you ran the script as `postgres`, run this in the Query Tool:
> `GRANT ALL ON ALL TABLES IN SCHEMA public TO analytics;`

**Option B: `psql` command line**

```bash
psql -U postgres -h localhost -c "CREATE ROLE analytics WITH LOGIN PASSWORD 'analytics';"
psql -U postgres -h localhost -c "CREATE DATABASE analytics_aggregator OWNER analytics;"
psql -U analytics -h localhost -d analytics_aggregator -f db/migrations/001_init.up.sql
```

To reset the schema, run [`db/migrations/001_init.down.sql`](./db/migrations/001_init.down.sql) and then the `up` script again.

### 3. Configure `.env`

The app reads its configuration from a `.env` file in the **directory you run it from** (loaded with [`godotenv`](https://github.com/joho/godotenv) in [`internal/config/config.go`](./internal/config/config.go)). Variables already set in the OS environment take priority over the file, and any missing key falls back to its default.

`.env` is gitignored, so create yours from the template:

```bash
cp .env.example .env            # macOS / Linux / Git Bash
Copy-Item .env.example .env     # Windows PowerShell
```

Then update `DATABASE_URL` to match the role, password, host and port you used in step 2:

```dotenv
SERVER_PORT=8081
DATABASE_URL=postgres://analytics:analytics@localhost:5432/analytics_aggregator?sslmode=disable
```

| Variable | Default | Description |
| --- | --- | --- |
| `SERVER_PORT` | `8081` | HTTP port the API listens on. |
| `APP_ENV` | `development` | Application environment. |
| `DATABASE_URL` | `postgres://localhost:5432/analytics_aggregator` | PostgreSQL connection string. URL-encode special characters in the password (e.g. `@` → `%40`). |
| `DB_MAX_CONNS` / `DB_MIN_CONNS` | `10` / `5` | Connection pool size. |
| `DB_MAX_IDLE_TIME` / `DB_MAX_LIFE_TIME` | `300` / `1800` | Pool connection idle and max lifetime, in seconds. |
| `DB_HEALTH_CHECK_PERIOD` | `60` | Pool health check interval, in seconds. |
| `PAGING_LIMIT_DEFAULT` / `PAGING_LIMIT_MAX` | `10` / `100` | Default and maximum page size. |
| `GEO_API_BASE_URL` | `http://ip-api.com` | Base URL of the IP enrichment API. |
| `ENRICHER_WORKER_SIZE` | `2` | Number of concurrent enricher workers. |
| `INSERT_BATCH_SIZE` | `5` | Events per database batch insert. |
| `PIPELINE_JOB_SIZE` | `50` | Pipeline channel buffer size. |
| `BACKOFF_MULTIPLIER` | `SECOND` | Retry backoff unit: `SECOND`, `MINUTE`, or anything else for milliseconds. |

> If the log shows `No .env file found, using system environment variables`, you are running the app from a directory that does not contain `.env`. Run it from the project root.

### 4. Run the API

From the project root:

```bash
go mod download
go run ./cmd/api
```

### 5. Send a test event

```bash
curl -X POST http://localhost:8081/event \
  -H "Content-Type: application/json" \
  -d '{"user_id":"usr_12345","event_type":"checkout_completed","client_ip":"192.168.1.55","payload":{"cart_value":151.50,"currency":"USD","items":3}}'
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8081/event -ContentType "application/json" `
  -Body '{"user_id":"usr_12345","event_type":"checkout_completed","client_ip":"192.168.1.55","payload":{"cart_value":151.50,"currency":"USD","items":3}}'
```

You can also open [`api-collection/`](./api-collection) in [Bruno](https://www.usebruno.com/) and use the `local` environment.

The API returns `202 Accepted`. After the batch is flushed, check the results in pgAdmin (right-click `events` → **View/Edit Data → All Rows**) or with:

```sql
SELECT * FROM events ORDER BY created_at DESC;
SELECT * FROM dead_letter_events ORDER BY failed_at DESC;
```

### Alternative: Docker

If you prefer Docker, `docker compose up -d` starts PostgreSQL 16 with the schema already applied. It listens on host port **5433**, so set:

```dotenv
DATABASE_URL=postgres://analytics:analytics@localhost:5433/analytics_aggregator?sslmode=disable
```

You can still connect pgAdmin 4 to it at `localhost:5433`.

## Development & Contributing

Before picking up a ticket or opening a Pull Request, all engineers **must** read and follow our development standards:

- 📖 **[Engineering Standard: Testing Workflow & SDLC](./testing.MD)**: Learn how to write technical ACs, generate test skeletons, and pass our CI pipeline coverage gates.
