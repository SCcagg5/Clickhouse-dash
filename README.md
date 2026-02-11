# ClickHouse Dash

A small, ultra-minimal ClickHouse web dashboard to run SQL queries and stream results + live metrics via **Server-Sent Events (SSE)**.

It’s designed to be fast, simple, and safe-ish by default (timeouts, max rows/bytes), while still allowing “full SQL” depending on your ClickHouse settings.

## Features

* Run ClickHouse queries from a web UI
* Stream results over SSE:

  * `result_meta` (columns + types)
  * `result_rows` (batched rows)
  * `progress` and `resource` metrics
  * `error` and `done`
* Live metrics:

  * elapsed time, progress
  * read rows/bytes + inst. rate
  * CPU (%), memory (inst/max), threads (inst/max)
* Cancel:

  * cancels the running query via context cancellation
  * best-effort fallback `KILL QUERY ... SYNC` using admin credentials
* Built-in guardrails via ClickHouse settings:

  * `max_execution_time`, `max_result_rows`, `max_result_bytes`
* Static frontend embedded into the Go binary (`embed`)

## Requirements

* Go **1.22+**
* A reachable ClickHouse server (native protocol by default)
* Optional: admin credentials if you want `KILL QUERY` to work reliably

## Quick start

```bash
# From repo root (adjust if your main package is under src/)
cd src

go mod download
go run .
```

Then open:

* UI: `http://127.0.0.1:8080/`
* Health: `http://127.0.0.1:8080/healthz`

## Configuration

All runtime configuration is read from environment variables.

### Server

| Variable      |     Default | Description                  |
| ------------- | ----------: | ---------------------------- |
| `LISTEN_HOST` | `127.0.0.1` | Host to bind the HTTP server |
| `LISTEN_PORT` |      `8080` | Port to bind the HTTP server |

### ClickHouse connection

| Variable          |          Default | Description                                                |
| ----------------- | ---------------: | ---------------------------------------------------------- |
| `CH_URL`          | `127.0.0.1:9000` | ClickHouse address or DSN (depending on your driver setup) |
| `CH_USER`         |        `default` | ClickHouse username                                        |
| `CH_PASS`         |        *(empty)* | ClickHouse password                                        |
| `CH_DATABASE`     |        `default` | Default database for queries                               |
| `CH_DIAL_TIMEOUT` |             `5s` | Dial timeout (Go duration, e.g. `250ms`, `5s`, `1m`)       |

### Admin credentials (optional, for `KILL QUERY` fallback)

If not set, admin credentials default to `CH_USER` / `CH_PASS`.

| Variable        |   Default | Description                                   |
| --------------- | --------: | --------------------------------------------- |
| `CH_ADMIN_USER` | `CH_USER` | Admin username used for control-plane actions |
| `CH_ADMIN_PASS` | `CH_PASS` | Admin password used for control-plane actions |

### Default query limits / guardrails

| Variable                        |                   Default | Description                                               |
| ------------------------------- | ------------------------: | --------------------------------------------------------- |
| `DEFAULT_MAX_EXECUTION_SECONDS` |                      `60` | ClickHouse `max_execution_time` (seconds)                 |
| `DEFAULT_MAX_RESULT_ROWS`       |                   `10000` | ClickHouse `max_result_rows`                              |
| `DEFAULT_MAX_RESULT_BYTES`      |                `10485760` | ClickHouse `max_result_bytes` (10 MiB)                    |
| `DEFAULT_RESULT_PREVIEW_ROWS`   | `DEFAULT_MAX_RESULT_ROWS` | Server-side preview limit (UI streaming stops after this) |

### Query session expiration

| Variable                        | Default | Description                                                          |
| ------------------------------- | ------: | -------------------------------------------------------------------- |
| `SESSION_EXPIRE_IF_NOT_STARTED` |    `2m` | Expire query sessions if the client never attaches to the SSE stream |
| `SESSION_EXPIRE_AFTER_FINISH`   |    `5m` | Expire sessions after completion                                     |

### Example

```bash
export LISTEN_HOST=0.0.0.0
export LISTEN_PORT=8080

export CH_URL=clickhouse:9000
export CH_USER=default
export CH_PASS=
export CH_DATABASE=default

export DEFAULT_MAX_EXECUTION_SECONDS=30
export DEFAULT_MAX_RESULT_ROWS=50000
export DEFAULT_MAX_RESULT_BYTES=$((50*1024*1024))
export DEFAULT_RESULT_PREVIEW_ROWS=20000

cd src && go run .
```

## API

### `POST /api/query`

Creates a new query session.

**Request JSON**

```json
{
  "sql": "SELECT 1",
  "database": "optional_db",
  "settings": {
    "max_execution_time": 10
  }
}
```

**Response JSON**

```json
{
  "query_id": "…",
  "stream_url": "/api/query/stream?query_id=…"
}
```

### `GET /api/query/stream?query_id=...`

SSE stream for a query session.

Event types you may receive:

* `meta`
* `progress`
* `resource`
* `result_meta`
* `result_rows`
* `error`
* `done`
* `keepalive`

### `POST /api/query/cancel`

Requests cancellation of a running query.

**Request JSON**

```json
{ "query_id": "…" }
```

**Response JSON**

```json
{ "query_id": "…", "status": "cancel_requested" }
```

## Notes on performance / “client is too slow”

Results are batched server-side and sent as `result_rows` events to avoid flooding the SSE channel and to protect memory usage when the browser cannot render fast enough.

If the client still can’t keep up, the server may cancel the query to avoid unbounded buffering.

## Releases

This repository is set up for tag-based releases (GitHub Actions + GoReleaser).

Create and push a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```
