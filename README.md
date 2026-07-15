<div align="center">

# 🔍 mysql-binlog-monitor

**Real-time MySQL binlog change capture — structured JSON logs + binlog inspection + time-based seeking**

[![CI](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/ci.yml)
[![Release](https://github.com/lihongjie0209/mysql-binlog-monitor/actions/workflows/release.yml/badge.svg)](https://github.com/lihongjie0209/mysql-binlog-monitor/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)

[Installation](#installation) • [Quick Start](#quick-start) • [CLI Reference](#cli-reference) • [Log Schema](#log-output-format) • [Storage Schema](#gluesql-storage-schema) • [Export Schema](#export-file-schema)

</div>

---

## ✨ Features

| | |
|---|---|
| 🚀 **High-performance** | Built in Rust with async I/O via `tokio` + `mysql_async` |
| 📋 **Structured JSON logs** | One JSON object per line, stdout + optional size-based rotation (`--log-max-bytes`) |
| 🔑 **Primary key resolution** | Fetches PK and column names from `information_schema` |
| 🌐 **Wildcard filters** | Filter databases/tables with `*` and `?` patterns |
| 🔎 **Field filters** | `--field-filter COL=VALUE` (repeatable AND; value supports wildcards) |
| 🕐 **Flexible start position** | Start from current position, file beginning, specific file:offset, or a timestamp |
| ⏱️ **Time-based seek** | `--since` scans binlog files to find the exact replay position for a given datetime |
| 📊 **Binlog inspection** | `binlog-info` lists all binlog files with size and UTC time ranges |
| 💾 **Embedded persistence** | Optional GlueSQL (sled) storage — id-only or full row |
| 📜 **Historical scan** | One-shot `scan` by db/table/time — no continuous follow |
| 📤 **Export** | Dump stored events to JSON array or CSV with filters |
| 🔄 **Auto-reconnect** | Exponential backoff (1 s → 60 s); resumes from last in-memory position |
| 📍 **Checkpoint** | Optional `--checkpoint-path` persists `file:pos` + `gtid_set` for restart resume |
| 🧬 **GTID mode** | `--gtid` uses COM_BINLOG_DUMP_GTID; events include `gtid`; GlueSQL dedupes by GTID |
| 🧱 **DDL-aware metadata** | Clears column/PK cache on `ALTER`/`CREATE`/`DROP`/`RENAME`/`TRUNCATE` |
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

## MySQL Prerequisites

```ini
# my.cnf / my.ini
log-bin          = mysql-bin
binlog-format    = ROW
binlog-row-image = FULL
server-id        = 1
```

### Required privileges

| Privilege | Why |
|---|---|
| `REPLICATION CLIENT` | `SHOW MASTER STATUS` / `SHOW BINARY LOGS` |
| `REPLICATION SLAVE` | Open the binlog dump stream |
| `SELECT` on `information_schema` (optional) | Resolve column names and primary keys; without it, logs use `col_N` / PK falls back to `"id"` |

On startup, `monitor` / `scan` / `binlog-info` check these privileges and print **CREATE USER / GRANT** guidance when something is missing. The same examples appear in `mysql-binlog-monitor --help`.

### Create a new user

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
GRANT SELECT ON information_schema.* TO 'repl'@'%';
FLUSH PRIVILEGES;
```

### Grant to an existing user

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'existing_user'@'%';
GRANT SELECT ON information_schema.* TO 'existing_user'@'%';
FLUSH PRIVILEGES;
```

### Split credentials (replication user without SELECT)

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY 'repl_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';

CREATE USER 'meta'@'%' IDENTIFIED BY 'meta_password';
GRANT SELECT ON information_schema.* TO 'meta'@'%';
FLUSH PRIVILEGES;
```

```bash
mysql-binlog-monitor monitor \
  --user repl --password repl_password \
  --metadata-user meta --metadata-password meta_password
```

---

## Quick Start

### 1. Inspect binlog files

```bash
# List all binlog files with size and time range
mysql-binlog-monitor binlog-info --password secret

# JSON output
mysql-binlog-monitor binlog-info --password secret --format json

# Show only files that cover a specific time window
mysql-binlog-monitor binlog-info --password secret \
  --since "2026-04-17T00:00:00Z" \
  --until "2026-04-17T12:00:00Z"
```

### 2. Monitor binlog changes

```bash
# Start capturing from current position (default)
mysql-binlog-monitor monitor --password secret

# Replay from a specific timestamp
mysql-binlog-monitor monitor --password secret \
  --since "2026-04-17T10:00:00+08:00"

# Start from a known file + offset
mysql-binlog-monitor monitor --password secret \
  --binlog-start "mysql-bin.000003:4096"

# Watch specific databases (wildcard) and persist events + checkpoint
mysql-binlog-monitor monitor --password secret \
  --databases "shop_*,analytics" \
  --tables "orders,products" \
  --gluesql-path ./events.db \
  --store-mode full \
  --checkpoint-path ./checkpoint.json
```

### 3. Historical scan (one-shot)

```bash
# Replay retained binlog for a table/time window, then exit
mysql-binlog-monitor scan --password secret \
  --databases "shop_*" \
  --tables "orders" \
  --field-filter status=paid \
  --field-filter "email=*@example.com" \
  --since "2026-04-17T00:00:00Z" \
  --until "2026-04-17T12:00:00Z" \
  --output orders.jsonl \
  --limit 10000
```

Does **not** keep running. Writes one JSON event per line (NDJSON) to stdout or `--output`. Progress summaries go to **stderr**.

### 4. Export stored events

```bash
# JSON to stdout
mysql-binlog-monitor export --gluesql-path ./events.db

# Filtered CSV to file
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
mysql-binlog-monitor monitor --password <PASSWORD> [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MySQL host |
| `--port` | `3306` | MySQL port |
| `--user` | `root` | Replication user |
| `--password` | *(required)* | Password — or `MYSQL_PASSWORD` env var |
| `--metadata-user` | same as `--user` | User for `information_schema` queries |
| `--metadata-password` | same as `--password` | Password — or `MYSQL_METADATA_PASSWORD` env var |
| `--server-id` | `100` | Replication server ID (must be unique in cluster) |
| `--log-file` | `binlog.log` | Output log file (newline-delimited JSON) |
| `--log-max-bytes` | `0` | Rotate log to `<file>.1` when size would exceed N bytes (`0` = never) |
| `--databases` | *(all)* | Comma-separated list; supports `*` / `?` wildcards |
| `--tables` | *(all)* | Comma-separated list; supports `*` / `?` wildcards |
| `--field-filter` | *(none)* | Repeatable `COL=VALUE` (AND). UPDATE matches before or after. Value may use `*`/`?` |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--gluesql-path` | *(disabled)* | Enable GlueSQL persistence at this directory |
| `--store-mode` | `id-only` | `id-only` = metadata only · `full` = include row JSON |
| `--binlog-start` | `end` | Starting position — see table below |
| `--since` | *(none)* | Seek to this datetime before streaming (overrides `--binlog-start`) |
| `--checkpoint-path` | *(disabled)* | JSON file storing last consumed `file:pos` + `gtid_set`; enables restart resume when `--binlog-start end` and no `--since` |
| `--gtid` | `auto` | `auto` = detect server `gtid_mode` · `on` = prefer GTID · `off` = file:pos only. Bare `--gtid` = `on` |

#### `--binlog-start` values

| Value | Behaviour |
|---|---|
| `end` *(default)* | Start from the current live position (`SHOW MASTER STATUS`) |
| `start` | Replay from position 4 of the current binlog file |
| `<file>:<pos>` | Start from an exact file + byte offset, e.g. `mysql-bin.000003:125638` |

#### `--since` / time formats

Accepted formats (all interpreted as UTC unless a timezone offset is given):

```
2026-04-17T10:00:00Z          # RFC 3339 UTC
2026-04-17T10:00:00+08:00     # RFC 3339 with offset
2026-04-17 10:00:00           # SQL-style datetime (UTC)
```

#### Checkpoint resume

Start-position priority (first match wins):

1. **In-memory resume** (file:pos, or GTID set when `--gtid`)
2. **`--since`** time seek
3. Explicit **`--binlog-start`** (`start` / `file:pos`)
4. **Disk checkpoint** when `--binlog-start end` and no `--since`
   - with `--gtid` + non-empty `gtid_set` → GTID auto-position
   - otherwise file:pos
5. Live end (`SHOW MASTER STATUS`, or `@@gtid_executed` when `--gtid`)

Checkpoint file format:

```json
{
  "file": "mysql-bin.000003",
  "pos": 125638,
  "gtid_set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
  "last_gtid": "3e11fa47-71ca-11e1-9e33-c80aa9429562:5"
}
```

#### GTID / delivery semantics

| Layer | Guarantee | How |
|---|---|---|
| Stream resume (GTID + checkpoint) | **At-least-once** source, no full replay of executed set | `COM_BINLOG_DUMP_GTID` with executed set |
| JSON log | At-least-once (may duplicate if crash after write before checkpoint) | Downstream should key on `gtid` |
| GlueSQL (`--gluesql-path`) | **Effectively exactly-once** per GTID | `processed_gtids` table skips duplicates |

**Auto-detect (default):** at startup the monitor reads `@@GLOBAL.gtid_mode`.

| `--gtid` | Server `gtid_mode=ON` | Behaviour |
|---|---|---|
| `auto` *(default)* | yes | GTID dump + `gtid` on events |
| `auto` | no | file:pos dump (classic) |
| `on` / bare `--gtid` | yes | GTID dump |
| `on` | no | **warn** and fall back to file:pos |
| `off` | * | always file:pos |

MySQL prerequisites for GTID path:

```ini
gtid-mode                = ON
enforce-gtid-consistency = ON
```

```bash
# Auto: enable GTID only when the server supports it
mysql-binlog-monitor monitor --password secret \
  --checkpoint-path ./checkpoint.json \
  --gluesql-path ./events.db

# Force prefer GTID (same as bare --gtid)
mysql-binlog-monitor monitor --password secret --gtid on

# Never use GTID dump
mysql-binlog-monitor monitor --password secret --gtid off
```

---

### `scan` subcommand

```
mysql-binlog-monitor scan --password <PASSWORD> [OPTIONS]
```

One-shot historical dump of ROW events. Uses a **non-blocking** binlog stream so the process exits when retained history is exhausted (or when `--until` / `--limit` is hit).

| Option | Default | Description |
|---|---|---|
| `--host` / `--port` / `--user` / `--password` | same as monitor | Connection |
| `--metadata-user` / `--metadata-password` | same as user | `information_schema` access |
| `--server-id` | `300` | Dump server id (unique per concurrent client) |
| `--databases` | *(all)* | Comma list; `*` / `?` wildcards |
| `--tables` | *(all)* | Comma list; `*` / `?` wildcards |
| `--field-filter` | *(none)* | Repeatable `COL=VALUE` (AND); same semantics as monitor |
| `--since` | *(earliest retained)* | Start time (also seeks binlog position) |
| `--until` | *(EOF)* | Stop after this event time |
| `--binlog-start` | *(first file:4)* | Exact `file:pos` start (optional) |
| `--output` | stdout | NDJSON file path |
| `--limit` | *(all)* | Max matching row events to emit |
| `--gtid` | `auto` | Same semantics as monitor (`auto`/`on`/`off`) |

Start position rules:

1. `--binlog-start file:pos` if set  
2. else `--since` time seek  
3. else first available binary log at position 4  

### `binlog-info` subcommand

```
mysql-binlog-monitor binlog-info --password <PASSWORD> [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MySQL host |
| `--port` | `3306` | MySQL port |
| `--user` | `root` | User (needs `REPLICATION CLIENT`) |
| `--password` | *(required)* | Password — or `MYSQL_PASSWORD` env var |
| `--format` | `table` | `table` (human-readable) \| `json` |
| `--server-id` | `200` | Replication server ID for timestamp scanning |
| `--since` | *(none)* | Show only files covering events at or after this datetime |
| `--until` | *(none)* | Show only files covering events at or before this datetime |

**Table output example:**

```
File                Size (bytes)  Start (UTC)           End (UTC)             Note
mysql-bin.000001            180   2026-04-16 09:54:34   2026-04-16 09:54:44
mysql-bin.000002        2995306   2026-04-16 09:54:44   2026-04-16 09:54:51
mysql-bin.000003         125638   2026-04-16 09:54:51   2026-04-17 02:53:03   current

Current position: mysql-bin.000003:125638
```

**JSON output example:**

```json
{
  "binlog_files": [
    {
      "log_name":   "mysql-bin.000001",
      "file_size":  180,
      "start_time": "2026-04-16T09:54:34Z",
      "end_time":   "2026-04-16T09:54:44Z",
      "is_current": false
    },
    {
      "log_name":   "mysql-bin.000003",
      "file_size":  125638,
      "start_time": "2026-04-16T09:54:51Z",
      "end_time":   "2026-04-17T02:53:03Z",
      "is_current": true
    }
  ],
  "current": {
    "file":          "mysql-bin.000003",
    "pos":           125638,
    "binlog_start":  "mysql-bin.000003:125638"
  }
}
```

> **Time range derivation:** Each file's `end_time` is the `start_time` of the next file (efficient — only reads the first event of each file). The current (active) file's `end_time` is the current wall-clock time.

---

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

The log file contains **one JSON object per line**. Each entry wraps the change event in a log envelope.

<details>
<summary><b>INSERT / DELETE event</b></summary>

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
<summary><b>UPDATE event</b></summary>

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
| `gtid` | string \| omitted | Transaction GTID (`uuid:gno`) when GTID events are present |
| `row.values` | object | Full row (INSERT / DELETE) |
| `row.before_values` | object | Row state before change (UPDATE) |
| `row.after_values` | object | Row state after change (UPDATE) |

---

## GlueSQL Storage Schema

When `--gluesql-path` is set, events are appended to an embedded [sled](https://github.com/spacejam/sled)-backed GlueSQL table.

### DDL

```sql
CREATE TABLE IF NOT EXISTS binlog_events (
    id          INTEGER,   -- auto-increment, resumes across restarts
    captured_at TEXT,      -- wall-clock write time (RFC-3339)
    event_time  TEXT,      -- binlog event timestamp (ISO-8601 UTC)
    operation   TEXT,      -- INSERT | UPDATE | DELETE
    db_name     TEXT,      -- source database
    table_name  TEXT,      -- source table
    primary_key TEXT,      -- JSON-encoded PK object, e.g. {"id":42}
    row_data    TEXT,      -- NULL (id-only) · JSON row object (full)
    gtid        TEXT       -- uuid:gno when GTID is available, else NULL
)

CREATE TABLE IF NOT EXISTS processed_gtids (
    gtid TEXT               -- for exactly-once local sink dedupe
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
│       ├── ci.yml          # Build + unit tests on every push / PR
│       └── release.yml     # Cross-compile + publish GitHub Release on v* tags
├── src/
│   ├── main.rs             # Entry point — monitor / scan / export / binlog-info
│   ├── lib.rs              # Crate root
│   ├── config.rs           # Cli + Monitor/Scan/Export/BinlogInfo args; wildcards
│   ├── monitor.rs          # Continuous binlog streaming + checkpoint
│   ├── scan.rs             # One-shot historical scan
│   ├── event.rs            # Shared row→JSON / event builders
│   ├── checkpoint.rs       # file:pos + gtid_set checkpoint
│   ├── gtid.rs             # GTID helpers
│   ├── ddl.rs              # DDL detection for metadata invalidation
│   ├── binlog_info.rs      # binlog-info subcommand
│   ├── time_seek.rs        # datetime parse + file/offset seek
│   ├── db.rs               # information_schema / BINARY LOGS helpers
│   ├── privileges.rs       # privilege check + CREATE USER / GRANT guidance
│   ├── logger.rs           # JSON log writer
│   ├── storage.rs          # GlueSQL EventStorage
│   └── export.rs           # GlueSQL export
├── docker-compose.yml      # Local MySQL 8 with ROW binlog for integration tests
└── tests/
    └── integration.rs      # Integration tests against live Docker MySQL
```

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Run tests: `cargo test -- --test-threads=1`
4. Open a pull request

Integration tests require a MySQL instance at `127.0.0.1:3306` (root/rootpassword).
Use the companion `docker-compose.yml` in the repository root.

---

## License

MIT © [lihongjie0209](https://github.com/lihongjie0209)
