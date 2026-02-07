# ClickHouse Dashboard MVP (TCP + SSE)

A minimal ClickHouse dashboard that focuses on **query execution telemetry** (progress + timing + profile events) and streams updates to the browser using **Server-Sent Events** (no browser polling).

This project intentionally keeps the UI small and the backend strict and defensive.

## Why TCP (native protocol) instead of ClickHouse HTTP?

ClickHouse has a binary native protocol that provides **progress packets** and **profile events** while the query is running — this is what `clickhouse-client` relies on. Implementing the binary protocol correctly is non-trivial, so this MVP uses the official Go client library:

- `github.com/ClickHouse/clickhouse-go/v2`

Everything else uses the Go standard library.

## Run

### 1) Set environment variables

```bash
export LISTEN="127.0.0.1:8080"

# ClickHouse native protocol address (TCP) or DSN
export CH_URL="127.0.0.1:9000"
export CH_USER="default"
export CH_PASS=""
export CH_DATABASE="default"

# Optional: admin credentials used for best-effort KILL QUERY fallback
export CH_ADMIN_USER=""
export CH_ADMIN_PASS=""

# Optional: ClickHouse TCP dial timeout
export CH_DIAL_TIMEOUT="5s"

# Default query guardrails
export DEFAULT_MAX_EXECUTION_SECONDS="60"
export DEFAULT_MAX_RESULT_ROWS="5000"
export DEFAULT_MAX_RESULT_BYTES="52428800"
```

`CH_URL` can be either:

- `127.0.0.1:9000` (or multiple: `host1:9000,host2:9000`)
- a DSN supported by `clickhouse-go`, e.g. `clickhouse://host:9000/default`

### 2) Start

```bash
go run .
```

Open:

- `http://127.0.0.1:8080`

## API

### `POST /api/query`

Request body:

```json
{
  "sql": "SELECT ...",
  "database": "optional",
  "settings": {
    "max_execution_time": 10
  }
}
```

Response:

```json
{
  "query_id": "9b6c2b3e-7f5a-4f9f-b0d7-2e1b0e0f9c2a",
  "stream_url": "/api/query/stream?query_id=..."
}
```

Read-only guard (MVP):
- Only statements starting with **SELECT / WITH / SHOW / DESCRIBE / DESC / EXPLAIN** are accepted.
- The server also sets ClickHouse setting `readonly=1` on every query.

### `GET /api/query/stream?query_id=...`

This is an **SSE** endpoint (EventSource). It starts the ClickHouse query on first attach and pushes events.

Event types:

- `meta`
- `progress`
- `resource`
- `log` (extra, best effort)
- `error`
- `done`

#### `meta`

```json
{
  "query_id": "...",
  "status": "connected"
}
```

#### `progress`

```json
{
  "query_id": "...",
  "elapsed_seconds": 12.34,

  "percent": 56.7,
  "percent_known": true,

  "read_rows": 123456,
  "read_bytes": 987654321,
  "total_rows_to_read": 217000,

  "rows_per_second": 10000.2,
  "bytes_per_second": 800000.0,

  "rows_per_second_inst": 12000.0,
  "bytes_per_second_inst": 900000.0
}
```

Progress percent rules:
1. If `total_bytes_to_read > 0`: `read_bytes / total_bytes_to_read * 100`
2. Else if `total_rows_to_read > 0`: `read_rows / total_rows_to_read * 100`
3. Else: `percent_known=false` (indeterminate)

With the native protocol, ClickHouse progress packets typically provide **total rows**, not total bytes.

#### `resource`

```json
{
  "query_id": "...",

  "central_processing_unit_seconds": 1.23,
  "central_processing_unit_core_percent_total": 150.0,
  "central_processing_unit_core_percent_instant": 220.0,

  "memory_current_bytes": 123456789,
  "memory_peak_bytes": 234567890,

  "thread_count_current": 8,
  "thread_count_peak": 12
}
```

Notes:
- CPU percentages can exceed 100% (multi-thread execution).
- Memory + threads are **best effort**. The native protocol does not expose stable per-query "current memory" and "current threads" gauges in all configurations. This MVP tries:
  - CPU: from `ProfileEvents` (`UserTimeMicroseconds` + `SystemTimeMicroseconds`)
  - Memory: from profile events (if present) and/or log parsing ("Peak memory usage")
  - Threads: inferred from active profile-event thread identifiers in a recent time window

#### `log` (best effort)

```json
{
  "query_id": "...",
  "line": "2026-02-06T12:34:56Z [SomeComponent] some log text"
}
```

#### `error`

```json
{
  "query_id": "...",
  "message": "ClickHouse error ...",
  "code": 123
}
```

#### `done`

```json
{
  "query_id": "...",
  "status": "finished",
  "elapsed_seconds": 12.34,
  "read_rows": 123456,
  "read_bytes": 987654321
}
```

### `POST /api/query/cancel`

Request body:

```json
{ "query_id": "..." }
```

Behavior:
- Cancels the local query context (native protocol cancellation).
- Also attempts `KILL QUERY WHERE query_id = '...' SYNC` as a best-effort fallback (requires privileges).

## ClickHouse read-only user (concept)

For a safer setup, create a dedicated read-only user:

- Grant only SELECT (and optionally SHOW/DESCRIBE/EXPLAIN permissions).
- Prefer a separate admin user for `KILL QUERY` (or disable KILL by leaving CH_ADMIN_* empty).

Exact SQL depends on your ClickHouse version and access management configuration.

## Known limitations (MVP)

- No query result rendering (the dashboard drains results to complete the query, but does not display rows).
- Memory and thread metrics are best effort with the native protocol.
- No authentication on the dashboard itself (run behind a reverse proxy if needed).
- Sessions are in-memory and expire automatically after completion.

## Build

```bash
go build -o chdash .
./chdash
```
