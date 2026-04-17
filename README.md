<div align="center">

# 🔍 mysql-binlog-monitor

**Real-time MySQL binlog change capture — structured JSON logs + embedded storage + multi-format export**

[![CI](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/ci.yml)
[![Release](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/release.yml/badge.svg)](https://github.com/lihongjie0209/mysql-binlog-monitor/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)

[Installation](#installation) • [Quick Start](#quick-start) • [CLI Reference](#cli-reference) • [Schema](#gluesql-storage-schema) • [Export](#export-file-schema)

</div>

---

## ✨ Features

| | |
|---|---|
| 🚀 **High-performance** | Built in Rust with async I/O via `tokio` + `mysql_async` |
| 📋 **Structured JSON logs** | One JSON object per line, stdout + rotating log file |
| 🔑 **Primary key resolution** | Fetches PK and column names from `information_schema` |
| 🌐 **Wildcard filters** | Filter databases/tables with `*` and `?` patterns |
| 💾 **Embedded persistence** | Optional GlueSQL (sled) storage — id-only or full row |
| 📤 **Export** | Dump stored events to JSON array or CSV with filters |
| 🔄 **Auto-reconnect** | Exponential backoff (1s → 60s) on connection loss |
| 🔐 **Split credentials** | Separate metadata user when replication user lacks `SELECT` |

---

## Installation

### Pre-built binaries

Download the latest release for your platform from the [**Releases**](https://github.com/lihongjie0209/mysql-binlog-monitor/releases) page:

| Platform | File |
|---|---|
| Linux x86-64 (static) | `mysql-binlog-monitor-*-x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM64 | `mysql-binlog-monitor-*-aarch64-unknown-linux-gnu.tar.gz` |
| macOS Intel | `mysql-binlog-monitor-*-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `mysql-binlog-monitor-*-aarch64-apple-darwin.tar.gz` |
| Windows x86-64 | `mysql-binlog-monitor-*-x86_64-pc-windows-msvc.zip` |

### Build from source

```bash
cargo install --git https://github.com/lihongjie0209/mysql-binlog-monitor
```

or clone and build:

```bash
git clone https://github.com/lihongjie0209/mysql-binlog-monitor
cd mysql-binlog-monitor
cargo build --release
# binary at: target/release/mysql-binlog-monitor
```

---

## Quick Start

### 1. MySQL prerequisites

```ini
# my.cnf / my.ini
log-bin          = mysql-bin
binlog-format    = ROW
binlog-row-image = FULL
server-id        = 1
```

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
-- optional: separate read-only user for information_schema
GRANT SELECT ON information_schema.* TO 'meta'@'%';
```

### 2. Monitor binlog changes

```bash
# Watch all databases
mysql-binlog-monitor monitor --password secret

# Watch specific databases (wildcard)
mysql-binlog-monitor monitor \
  --password secret \
  --databases "shop_*,analytics"

# Also persist events for later export
mysql-binlog-monitor monitor \
  --password secret \
  --databases mydb \
  --gluesql-path ./events.db \
  --store-mode full
```

### 3. Export stored events

```bash
# JSON to stdout
mysql-binlog-monitor export --gluesql-path ./events.db

# CSV file, filtered
mysql-binlog-monitor export \
  --gluesql-path ./events.db \
  --format csv \
  --output events.csv \
  --operation INSERT \
  --table-filter orders \
  --limit 10000
```

---

## CLI Reference

### `monitor` subcommand

```
mysql-binlog-monitor monitor [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MySQL host |
| `--port` | `3306` | MySQL port |
| `--user` | `root` | Replication user |
| `--password` | *(required)* | Password — or `MYSQL_PASSWORD` env var |
| `--metadata-user` | same as `--user` | User for `information_schema` queries |
| `--metadata-password` | same as `--password` | Password — or `MYSQL_METADATA_PASSWORD` env var |
| `--server-id` | `100` | Replication server ID (must be unique in the cluster) |
| `--log-file` | `binlog.log` | Output log file (newline-delimited JSON) |
| `--databases` | *(all)* | Comma-separated list; supports `*` / `?` wildcards |
| `--tables` | *(all)* | Comma-separated list; supports `*` / `?` wildcards |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--gluesql-path` | *(disabled)* | Enable GlueSQL persistence at this directory |
| `--store-mode` | `id-only` | `id-only` = metadata only · `full` = include row JSON |

### `export` subcommand

```
mysql-binlog-monitor export --gluesql-path <PATH> [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--gluesql-path` | *(required)* | GlueSQL database directory |
| `--format` | `json` | `json` (array of objects) \| `csv` (header + rows) |
| `--output` | stdout | Write to file instead of stdout |
| `--db-filter` | *(all)* | Exact database name filter |
| `--table-filter` | *(all)* | Exact table name filter |
| `--operation` | *(all)* | `INSERT` \| `UPDATE` \| `DELETE` |
| `--limit` | *(all)* | Maximum rows to export |

---

## Log Output Format

The log file contains **one JSON object per line**. Each entry wraps the event in a log envelope:

<details>
<summary><b>INSERT / DELETE</b></summary>

```json
{
  "time":    "2026-04-17T01:00:00.000Z",
  "level":   "INFO",
  "message": {
    "timestamp":   "2026-04-17T01:00:00Z",
    "event_time":  1713312000,
    "operation":   "INSERT",
    "database":    "mydb",
    "table":       "orders",
    "pk_columns":  ["id"],
    "primary_key": {"id": 42},
    "row": {
      "values": {"id": 42, "user_id": 7, "total": 99.99, "created_at": "2026-04-17"}
    }
  }
}
```

</details>

<details>
<summary><b>UPDATE</b></summary>

```json
{
  "time":    "2026-04-17T01:00:01.000Z",
  "level":   "INFO",
  "message": {
    "timestamp":   "2026-04-17T01:00:01Z",
    "event_time":  1713312001,
    "operation":   "UPDATE",
    "database":    "mydb",
    "table":       "orders",
    "pk_columns":  ["id"],
    "primary_key": {"id": 42},
    "row": {
      "before_values": {"id": 42, "total": 99.99},
      "after_values":  {"id": 42, "total": 129.99}
    }
  }
}
```

</details>

### Event field reference

| Field | Type | Description |
|---|---|---|
| `timestamp` | ISO-8601 string | Event time from the binlog (UTC) |
| `event_time` | integer | Unix epoch seconds |
| `operation` | string | `INSERT`, `UPDATE`, or `DELETE` |
| `database` | string | Source database/schema |
| `table` | string | Source table |
| `pk_columns` | `string[]` \| `null` | Primary key column names; `null` if unknown |
| `primary_key` | `object` \| `null` | `{col: value}` for each PK column |
| `row.values` | object | Full row (INSERT / DELETE) |
| `row.before_values` | object | Row state before change (UPDATE) |
| `row.after_values` | object | Row state after change (UPDATE) |

---

## GlueSQL Storage Schema

When `--gluesql-path` is set, events are appended to an embedded [sled](https://github.com/spacejam/sled)-backed GlueSQL table.

### DDL

```sql
CREATE TABLE IF NOT EXISTS binlog_events (
    id          INTEGER,  -- auto-increment, resumes across restarts
    captured_at TEXT,     -- wall-clock write time (RFC-3339)
    event_time  TEXT,     -- binlog event timestamp (ISO-8601 UTC)
    operation   TEXT,     -- INSERT | UPDATE | DELETE
    db_name     TEXT,     -- source database
    table_name  TEXT,     -- source table
    primary_key TEXT,     -- JSON-encoded PK object, e.g. {"id":42}
    row_data    TEXT      -- NULL (id-only) · JSON row object (full)
)
```

### Column reference

| Column | Example | Notes |
|---|---|---|
| `id` | `1` | Monotonically increasing; survives process restart |
| `captured_at` | `"2026-04-17T01:00:00.123Z"` | When the row was written to GlueSQL |
| `event_time` | `"2026-04-17T01:00:00Z"` | Timestamp from the MySQL binlog |
| `operation` | `"INSERT"` | `INSERT`, `UPDATE`, or `DELETE` |
| `db_name` | `"mydb"` | Source database |
| `table_name` | `"orders"` | Source table |
| `primary_key` | `"{\"id\":42}"` | JSON-encoded PK; `"null"` if no PK detected |
| `row_data` | `"{\"values\":{...}}"` | Row JSON (`full` mode) or `NULL` (`id-only`) |

### `row_data` structure

```jsonc
// INSERT / DELETE (--store-mode full)
{"values": {"id": 42, "total": 99.99, "status": "pending"}}

// UPDATE (--store-mode full)
{"before_values": {"id": 42, "total": 99.99},
 "after_values":  {"id": 42, "total": 129.99}}
```

---

## Export File Schema

### JSON (`--format json`)

A pretty-printed JSON **array of objects**. Each object maps column names to their values.
`NULL` database values become JSON `null`; integers are JSON numbers.

```json
[
  {
    "id":          1,
    "captured_at": "2026-04-17T01:00:00.123Z",
    "event_time":  "2026-04-17T01:00:00Z",
    "operation":   "INSERT",
    "db_name":     "mydb",
    "table_name":  "orders",
    "primary_key": "{\"id\":42}",
    "row_data":    "{\"values\":{\"id\":42,\"total\":99.99}}"
  },
  {
    "id":          2,
    "captured_at": "2026-04-17T01:00:01.456Z",
    "event_time":  "2026-04-17T01:00:01Z",
    "operation":   "UPDATE",
    "db_name":     "mydb",
    "table_name":  "orders",
    "primary_key": "{\"id\":42}",
    "row_data":    null
  }
]
```

### CSV (`--format csv`)

Header row + one data row per event.
Fields containing `,`, `"`, or newlines are quoted; internal `"` are doubled (`""`).
`NULL` values become empty fields.

```csv
id,captured_at,event_time,operation,db_name,table_name,primary_key,row_data
1,2026-04-17T01:00:00.123Z,2026-04-17T01:00:00Z,INSERT,mydb,orders,"{""id"":42}","{""values"":{""id"":42}}"
2,2026-04-17T01:00:01.456Z,2026-04-17T01:00:01Z,UPDATE,mydb,orders,"{""id"":42}",
```

---

## Project Structure

```
mysql-binlog-monitor/
├── .github/
│   └── workflows/
│       ├── ci.yml       # Build + unit tests on every push / PR (Linux, macOS, Windows)
│       └── release.yml  # Cross-compile + publish GitHub Release on `v*` tags
├── src/
│   ├── main.rs          # Entry point — dispatches monitor / export subcommands
│   ├── lib.rs           # Crate root
│   ├── config.rs        # Cli, Args (monitor), ExportArgs; wildcard matching
│   ├── monitor.rs       # Binlog streaming loop with exponential backoff
│   ├── db.rs            # information_schema helpers (column names, PK columns)
│   ├── logger.rs        # JSON log writer
│   ├── storage.rs       # GlueSQL EventStorage — insert + schema management
│   └── export.rs        # Export runner — JSON and CSV writers
├── tests/
│   └── integration.rs   # 7 integration tests against live Docker MySQL
└── Cargo.toml
```

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Run tests: `cargo test -- --test-threads=1`
4. Open a pull request

Integration tests require a MySQL instance at `127.0.0.1:3306` (root/rootpassword). Use the companion `docker-compose.yml` in the `mysql-binlog` directory.

---

## License

MIT © [lihongjie0209](https://github.com/lihongjie0209)


## Features

- Captures **INSERT**, **UPDATE**, and **DELETE** events from MySQL binlog
- Outputs structured **JSON logs** to a file (one JSON object per line)
- Filters by databases and/or tables with **wildcard support** (`*`, `?`)
- Resolves **primary key columns** and **column names** from `information_schema`
- Persists events to an embedded **GlueSQL (sled)** database — id-only or full row
- **Exports** persisted events to **JSON** or **CSV** via the `export` subcommand
- Automatic **exponential backoff** reconnect on connection loss
- Separate `--metadata-user` credential for `information_schema` queries

---

## Quick Start

### 1. Start MySQL (Docker Compose)

```bash
# from the companion mysql-binlog/ Python project directory
docker compose up -d
```

MySQL is pre-configured with:
- `binlog-format=ROW`, `binlog-row-image=FULL`
- A replication user with `REPLICATION SLAVE, REPLICATION CLIENT`

### 2. Build

```bash
cargo build --release
```

### 3. Run the monitor

```bash
# Monitor all databases, log to binlog.log
./target/release/mysql-binlog-monitor monitor --password rootpassword

# Monitor specific databases (wildcard supported)
./target/release/mysql-binlog-monitor monitor \
  --password rootpassword \
  --databases "app_*,legacy"

# Also persist events to GlueSQL (full row data)
./target/release/mysql-binlog-monitor monitor \
  --password rootpassword \
  --databases mydb \
  --gluesql-path ./events.db \
  --store-mode full
```

### 4. Export persisted events

```bash
# Export all events as JSON to stdout
./target/release/mysql-binlog-monitor export \
  --gluesql-path ./events.db

# Export as CSV to a file, filtered by table
./target/release/mysql-binlog-monitor export \
  --gluesql-path ./events.db \
  --format csv \
  --output events.csv \
  --table-filter orders \
  --operation INSERT \
  --limit 1000
```

---

## CLI Reference

### `monitor` subcommand

| Option | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MySQL host |
| `--port` | `3306` | MySQL port |
| `--user` | `root` | MySQL user (needs `REPLICATION SLAVE, REPLICATION CLIENT`) |
| `--password` | *(required)* | MySQL password (or `MYSQL_PASSWORD` env var) |
| `--metadata-user` | *(same as --user)* | User for `information_schema` queries |
| `--metadata-password` | *(same as --password)* | Password for metadata user (or `MYSQL_METADATA_PASSWORD` env var) |
| `--server-id` | `100` | Replication server ID — must be unique in the cluster |
| `--log-file` | `binlog.log` | Output log file path (newline-delimited JSON) |
| `--databases` | *(all)* | Comma-separated databases; supports `*` and `?` wildcards |
| `--tables` | *(all)* | Comma-separated tables; supports `*` and `?` wildcards |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--gluesql-path` | *(disabled)* | Directory for GlueSQL sled storage |
| `--store-mode` | `id-only` | `id-only` stores metadata only; `full` also stores the row JSON |

### `export` subcommand

| Option | Default | Description |
|---|---|---|
| `--gluesql-path` | *(required)* | GlueSQL database directory to read from |
| `--format` | `json` | `json` (array of objects) or `csv` (header + rows) |
| `--output` | *(stdout)* | Write to this file instead of stdout |
| `--db-filter` | *(all)* | Only rows matching this database name |
| `--table-filter` | *(all)* | Only rows matching this table name |
| `--operation` | *(all)* | `INSERT` \| `UPDATE` \| `DELETE` |
| `--limit` | *(all)* | Maximum number of rows to export |

---

## Log Output Format

The log file (`binlog.log`) contains one JSON object per line wrapped in a log envelope.

### INSERT / DELETE event

```json
{
  "time":    "2026-04-17T01:00:00.000Z",
  "level":   "INFO",
  "message": {
    "timestamp":   "2026-04-17T01:00:00Z",
    "event_time":  1713312000,
    "operation":   "INSERT",
    "database":    "mydb",
    "table":       "orders",
    "pk_columns":  ["id"],
    "primary_key": {"id": 42},
    "row": {
      "values": {
        "id": 42, "user_id": 7, "total": 99.99, "created_at": "2026-04-17"
      }
    }
  }
}
```

### UPDATE event

```json
{
  "time":    "2026-04-17T01:00:01.000Z",
  "level":   "INFO",
  "message": {
    "timestamp":   "2026-04-17T01:00:01Z",
    "event_time":  1713312001,
    "operation":   "UPDATE",
    "database":    "mydb",
    "table":       "orders",
    "pk_columns":  ["id"],
    "primary_key": {"id": 42},
    "row": {
      "before_values": {"id": 42, "total": 99.99},
      "after_values":  {"id": 42, "total": 129.99}
    }
  }
}
```

### Field reference

| Field | Type | Description |
|---|---|---|
| `timestamp` | ISO-8601 string | Event timestamp from the binlog (UTC) |
| `event_time` | integer | Unix epoch seconds of the event |
| `operation` | string | `INSERT`, `UPDATE`, or `DELETE` |
| `database` | string | Schema/database name |
| `table` | string | Table name |
| `pk_columns` | array / null | Primary key column name(s), or `null` if unknown |
| `primary_key` | object / null | `{col: value}` pairs for all PK columns |
| `row.values` | object | Full row data (INSERT / DELETE) |
| `row.before_values` | object | Row state before the change (UPDATE) |
| `row.after_values` | object | Row state after the change (UPDATE) |

---

## GlueSQL Storage Schema

When `--gluesql-path` is provided, events are persisted to an embedded sled-backed GlueSQL database in a table named **`binlog_events`**.

### Table definition

```sql
CREATE TABLE IF NOT EXISTS binlog_events (
    id          INTEGER,   -- Auto-incrementing row ID (resumes across restarts)
    captured_at TEXT,      -- Wall-clock time the event was written (RFC-3339)
    event_time  TEXT,      -- Binlog event timestamp (ISO-8601 UTC)
    operation   TEXT,      -- INSERT | UPDATE | DELETE
    db_name     TEXT,      -- Database/schema name
    table_name  TEXT,      -- Table name
    primary_key TEXT,      -- JSON-encoded primary key object, e.g. {"id":42}
    row_data    TEXT       -- NULL (id-only mode) or JSON row data (full mode)
)
```

### Column details

| Column | Example value | Notes |
|---|---|---|
| `id` | `1` | Monotonically increasing; persists across restarts |
| `captured_at` | `"2026-04-17T01:00:00.123456789Z"` | When the row was written to GlueSQL |
| `event_time` | `"2026-04-17T01:00:00Z"` | Timestamp from the MySQL binlog |
| `operation` | `"INSERT"` | One of `INSERT`, `UPDATE`, `DELETE` |
| `db_name` | `"mydb"` | Source database |
| `table_name` | `"orders"` | Source table |
| `primary_key` | `"{\"id\":42}"` | JSON-encoded PK object; `"null"` if no PK detected |
| `row_data` | `"{\"values\":{...}}"` | Full row JSON (`--store-mode full`) or `NULL` (`id-only`) |

### `row_data` structure

In **`full`** mode `row_data` is the JSON-encoded `row` field from the log event:

```json
// INSERT / DELETE
{"values": {"id": 42, "total": 99.99}}

// UPDATE
{"before_values": {"id": 42, "total": 99.99},
 "after_values":  {"id": 42, "total": 129.99}}
```

---

## Export File Schema

### JSON format (`--format json`)

An array of objects where each object represents one row from `binlog_events`.
All GlueSQL column names become JSON object keys.

```json
[
  {
    "id":          1,
    "captured_at": "2026-04-17T01:00:00.123456789Z",
    "event_time":  "2026-04-17T01:00:00Z",
    "operation":   "INSERT",
    "db_name":     "mydb",
    "table_name":  "orders",
    "primary_key": "{\"id\":42}",
    "row_data":    "{\"values\":{\"id\":42,\"total\":99.99}}"
  }
]
```

- Integer columns (`id`) are exported as JSON numbers.
- `NULL` columns (`row_data` in id-only mode) are exported as JSON `null`.
- String columns are exported as JSON strings.

### CSV format (`--format csv`)

Header row followed by one data row per event.
Fields containing commas, double-quotes, or newlines are wrapped in double-quotes with internal quotes doubled (`""`).
`NULL` values are represented as empty fields.

```csv
id,captured_at,event_time,operation,db_name,table_name,primary_key,row_data
1,2026-04-17T01:00:00.123456789Z,2026-04-17T01:00:00Z,INSERT,mydb,orders,"{""id"":42}",
2,2026-04-17T01:00:01.000Z,2026-04-17T01:00:01Z,UPDATE,mydb,orders,"{""id"":42}",
```

---

## MySQL Requirements

```ini
log-bin        = mysql-bin
binlog-format  = ROW
binlog-row-image = FULL
server-id      = 1
```

The replication user needs:

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
```

If the replication user does not have `SELECT` on `information_schema`, provide a separate metadata user:

```sql
GRANT SELECT ON information_schema.* TO 'meta_user'@'%';
```

Then pass `--metadata-user meta_user --metadata-password ...`.

---

## Project Structure

```
mysql-binlog-rs/
├── Cargo.toml
├── src/
│   ├── main.rs       # CLI entry point — dispatches monitor / export subcommands
│   ├── lib.rs        # Crate root — re-exports all modules
│   ├── config.rs     # Cli, Args (monitor), ExportArgs; wildcard matching
│   ├── monitor.rs    # Binlog streaming loop with exponential backoff reconnect
│   ├── db.rs         # information_schema helpers (column names, PK columns)
│   ├── logger.rs     # JSON log writer
│   ├── storage.rs    # GlueSQL EventStorage (insert + schema management)
│   └── export.rs     # Export runner — JSON and CSV writers
└── tests/
    └── integration.rs  # 7 integration tests against live Docker MySQL
```
