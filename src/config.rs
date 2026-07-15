use clap::{Parser, Subcommand};

// ── Wildcard matching ──────────────────────────────────────────────────────────
// Supports `*` (any sequence of chars) and `?` (exactly one char), case-sensitive.

fn wildmatch(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    wm(&p, &t)
}

fn wm(p: &[char], t: &[char]) -> bool {
    match p.first() {
        None => t.is_empty(),
        Some('*') => {
            let p = &p[1..];
            (0..=t.len()).any(|i| wm(p, &t[i..]))
        }
        Some('?') => !t.is_empty() && wm(&p[1..], &t[1..]),
        Some(pc) => t.first() == Some(pc) && wm(&p[1..], &t[1..]),
    }
}

pub fn matches_any(patterns: &[String], value: &str) -> bool {
    patterns.iter().any(|pat| wildmatch(pat, value))
}

/// Split a comma-separated filter list, trimming empties.
pub fn parse_csv_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

// ── Top-level CLI ──────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "mysql-binlog-monitor",
    about = "Monitor MySQL binlog changes and export captured events.",
    after_help = crate::privileges::HELP_MYSQL_PRIVILEGES,
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Monitor MySQL binlog and stream events to a JSON log file (and optionally GlueSQL).
    Monitor(Args),
    /// One-shot historical scan of binlog events (no continuous follow).
    Scan(ScanArgs),
    /// Export events stored in a GlueSQL database to JSON or CSV.
    Export(ExportArgs),
    /// Show available binlog files and the current write position.
    BinlogInfo(BinlogInfoArgs),
}

// ── Monitor args ───────────────────────────────────────────────────────────────

#[derive(clap::Args, Debug, Clone)]
#[command(
    about = "Monitor MySQL binlog changes and output structured JSON logs.\n\
             Events (INSERT/UPDATE/DELETE) are logged with table name, primary key,\n\
             timestamp, database name, and full row data.",
    after_help = crate::privileges::HELP_MYSQL_PRIVILEGES
)]
pub struct Args {
    /// MySQL host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// MySQL port
    #[arg(long, default_value_t = 3306)]
    pub port: u16,

    /// MySQL user
    #[arg(long, default_value = "root")]
    pub user: String,

    /// MySQL password (or set MYSQL_PASSWORD env var)
    #[arg(long, env = "MYSQL_PASSWORD")]
    pub password: String,

    /// Replication server ID — must be unique within the cluster
    #[arg(long, default_value_t = 100)]
    pub server_id: u32,

    /// Path to the JSON log output file
    #[arg(long, default_value = "binlog.log")]
    pub log_file: String,

    /// Rotate the log file when it exceeds this many bytes (0 = never rotate).
    /// The previous file is renamed to `<log-file>.1`.
    #[arg(long, default_value_t = 0)]
    pub log_max_bytes: u64,

    /// MySQL user for metadata queries (information_schema).
    /// If omitted, uses --user. Set this when the replication user lacks SELECT privilege.
    #[arg(long)]
    pub metadata_user: Option<String>,

    /// Password for the metadata user (or set MYSQL_METADATA_PASSWORD env var).
    /// Falls back to --password when not set.
    #[arg(long, env = "MYSQL_METADATA_PASSWORD")]
    pub metadata_password: Option<String>,

    /// Comma-separated databases to monitor, supports wildcards * and ? (omit for all)
    #[arg(long, default_value = "")]
    pub databases: String,

    /// Comma-separated tables to monitor, supports wildcards * and ? (omit for all)
    #[arg(long, default_value = "")]
    pub tables: String,

    /// Filter by row field values. Repeatable; all conditions must match (AND).
    /// Format: `COL=VALUE` (value supports `*` / `?`). For UPDATE, matches if
    /// either before or after row satisfies the predicates.
    #[arg(long = "field-filter", value_name = "COL=VALUE")]
    pub field_filters: Vec<String>,

    /// Log level: debug | info | warn | error
    #[arg(long, default_value = "info",
          value_parser = ["debug", "info", "warn", "error"])]
    pub log_level: String,

    /// Path to GlueSQL (sled) database directory.
    /// When set, captured events are persisted to GlueSQL in addition to the log file.
    #[arg(long)]
    pub gluesql_path: Option<String>,

    /// What to store in GlueSQL: "id-only" persists only the primary key + metadata;
    /// "full" also stores the complete row data as JSON.
    #[arg(long, default_value = "id-only",
          value_parser = ["id-only", "full"])]
    pub store_mode: String,

    /// Binlog starting position. Three forms are accepted:
    ///
    ///   end          (default) Start from the current live position (SHOW MASTER STATUS).
    ///                Only events that occur after the monitor starts are captured.
    ///
    ///   start        Replay from position 4 of the current binlog file.
    ///                Useful to re-process today's history.
    ///
    ///   <file>:<pos> Start from a specific binlog file and byte offset,
    ///                e.g. "mysql-bin.042863:380228940".
    #[arg(long, default_value = "end")]
    pub binlog_start: String,

    /// Start monitoring from this point in time instead of from the current position.
    /// Scans available binlog files to find the exact file + byte offset.
    /// Accepted formats: RFC 3339 (2026-04-17T10:00:00Z, 2026-04-17T10:00:00+08:00)
    /// or 'YYYY-MM-DD HH:MM:SS' (treated as UTC).
    /// Takes precedence over --binlog-start when both are set.
    #[arg(long)]
    pub since: Option<String>,

    /// Path to a JSON checkpoint file that stores the last consumed binlog position.
    ///
    /// When set:
    /// - On startup (with default `--binlog-start end` and no `--since`), resume from
    ///   the saved file:pos / gtid_set if the file exists.
    /// - After each processed event / committed GTID, the checkpoint is written atomically.
    /// - On reconnect, the in-memory position is always preferred (even without this flag).
    #[arg(long)]
    pub checkpoint_path: Option<String>,

    /// GTID streaming mode for COM_BINLOG_DUMP_GTID:
    ///
    ///   auto  (default) Detect server `gtid_mode`; enable GTID dump when ON.
    ///   on              Prefer GTID; fall back to file:pos with a warning if unavailable.
    ///   off             Never use GTID dump (file:pos only).
    ///
    /// Bare `--gtid` (no value) is treated as `on`.
    /// With `--checkpoint-path`, the executed GTID set is persisted for resume.
    /// Combined with GlueSQL GTID dedupe this provides effectively exactly-once local persistence.
    #[arg(
        long,
        default_value = "auto",
        num_args = 0..=1,
        default_missing_value = "on",
        value_parser = ["auto", "on", "off"]
    )]
    pub gtid: String,
}

impl Args {
    /// Parse `--gtid` into a typed preference.
    pub fn gtid_preference(&self) -> crate::gtid::GtidPreference {
        crate::gtid::GtidPreference::parse(&self.gtid).unwrap_or(crate::gtid::GtidPreference::Auto)
    }
}

// ── Binlog start position ──────────────────────────────────────────────────────

/// Parsed form of the `--binlog-start` argument.
#[derive(Debug, Clone, PartialEq)]
pub enum BinlogStart {
    /// Start from the current live position (`SHOW MASTER STATUS`).
    End,
    /// Replay from position 4 of the current binlog file (old default behaviour).
    Start,
    /// Start from a specific file and byte offset.
    At { file: String, pos: u64 },
}

impl Args {
    /// Parse the `--binlog-start` string into a typed `BinlogStart`.
    ///
    /// Returns `Err(String)` with a human-readable message on bad input.
    pub fn parse_binlog_start(&self) -> Result<BinlogStart, String> {
        match self.binlog_start.as_str() {
            "end"   => Ok(BinlogStart::End),
            "start" => Ok(BinlogStart::Start),
            s => {
                // Expect "file:pos", e.g. "mysql-bin.042863:380228940"
                let mut parts = s.rsplitn(2, ':');
                let pos_str = parts.next().ok_or_else(|| format!("Invalid --binlog-start: '{}'", s))?;
                let file    = parts.next().ok_or_else(|| {
                    format!("Invalid --binlog-start '{}': expected <file>:<pos> or 'start' or 'end'", s)
                })?;
                let pos: u64 = pos_str.parse().map_err(|_| {
                    format!("Invalid --binlog-start '{}': position '{}' is not a valid u64", s, pos_str)
                })?;
                Ok(BinlogStart::At { file: file.to_string(), pos })
            }
        }
    }
}

impl Args {
    pub fn filter_databases(&self) -> Vec<String> {
        parse_csv_list(&self.databases)
    }

    pub fn filter_tables(&self) -> Vec<String> {
        parse_csv_list(&self.tables)
    }

    pub fn should_include(&self, database: &str, table: &str) -> bool {
        crate::event::should_include(
            &self.filter_databases(),
            &self.filter_tables(),
            database,
            table,
            matches_any,
        )
    }

    pub fn field_predicates(&self) -> Result<Vec<crate::field_filter::FieldPredicate>, String> {
        crate::field_filter::parse_field_filters(&self.field_filters)
    }
}

// ── Scan args (one-shot historical) ────────────────────────────────────────────

#[derive(clap::Args, Debug, Clone)]
#[command(
    about = "One-shot scan of historical binlog changes (does not keep running).\n\
             Filter by database/table wildcards and optional --since / --until time range.\n\
             Writes one JSON event per line to stdout or --output.",
    after_help = crate::privileges::HELP_MYSQL_PRIVILEGES
)]
pub struct ScanArgs {
    /// MySQL host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// MySQL port
    #[arg(long, default_value_t = 3306)]
    pub port: u16,

    /// MySQL user
    #[arg(long, default_value = "root")]
    pub user: String,

    /// MySQL password (or set MYSQL_PASSWORD env var)
    #[arg(long, env = "MYSQL_PASSWORD")]
    pub password: String,

    /// User for information_schema metadata (defaults to --user)
    #[arg(long)]
    pub metadata_user: Option<String>,

    /// Password for metadata user (or MYSQL_METADATA_PASSWORD)
    #[arg(long, env = "MYSQL_METADATA_PASSWORD")]
    pub metadata_password: Option<String>,

    /// Replication server ID used for the short-lived binlog dump
    #[arg(long, default_value_t = 300)]
    pub server_id: u32,

    /// Comma-separated databases; supports * and ? (omit for all)
    #[arg(long, default_value = "")]
    pub databases: String,

    /// Comma-separated tables; supports * and ? (omit for all)
    #[arg(long, default_value = "")]
    pub tables: String,

    /// Filter by row field values. Repeatable; all must match (AND).
    /// Format: `COL=VALUE` (value supports `*` / `?`). UPDATE matches before or after.
    #[arg(long = "field-filter", value_name = "COL=VALUE")]
    pub field_filters: Vec<String>,

    /// Only events at or after this datetime (RFC 3339 or 'YYYY-MM-DD HH:MM:SS' UTC)
    #[arg(long)]
    pub since: Option<String>,

    /// Only events at or before this datetime (same formats as --since)
    #[arg(long)]
    pub until: Option<String>,

    /// Optional exact start `file:pos` (overrides beginning-of-history when no --since)
    #[arg(long)]
    pub binlog_start: Option<String>,

    /// Write events to this file instead of stdout (newline-delimited JSON)
    #[arg(long)]
    pub output: Option<String>,

    /// Maximum number of matching events to emit (omit for unlimited)
    #[arg(long)]
    pub limit: Option<u64>,

    /// GTID mode for the dump: auto | on | off (same as monitor)
    #[arg(
        long,
        default_value = "auto",
        num_args = 0..=1,
        default_missing_value = "on",
        value_parser = ["auto", "on", "off"]
    )]
    pub gtid: String,
}

impl ScanArgs {
    pub fn filter_databases(&self) -> Vec<String> {
        parse_csv_list(&self.databases)
    }

    pub fn filter_tables(&self) -> Vec<String> {
        parse_csv_list(&self.tables)
    }

    pub fn should_include(&self, database: &str, table: &str) -> bool {
        crate::event::should_include(
            &self.filter_databases(),
            &self.filter_tables(),
            database,
            table,
            matches_any,
        )
    }

    pub fn field_predicates(&self) -> Result<Vec<crate::field_filter::FieldPredicate>, String> {
        crate::field_filter::parse_field_filters(&self.field_filters)
    }

    pub fn gtid_preference(&self) -> crate::gtid::GtidPreference {
        crate::gtid::GtidPreference::parse(&self.gtid).unwrap_or(crate::gtid::GtidPreference::Auto)
    }

    /// Parse optional `--binlog-start file:pos` (scan does not accept end/start keywords).
    pub fn parse_file_pos(&self) -> Result<Option<(String, u64)>, String> {
        let Some(s) = &self.binlog_start else {
            return Ok(None);
        };
        let mut parts = s.rsplitn(2, ':');
        let pos_str = parts
            .next()
            .ok_or_else(|| format!("Invalid --binlog-start: '{s}'"))?;
        let file = parts.next().ok_or_else(|| {
            format!("Invalid --binlog-start '{s}': expected <file>:<pos>")
        })?;
        let pos: u64 = pos_str
            .parse()
            .map_err(|_| format!("Invalid --binlog-start '{s}': bad position"))?;
        if pos == 0 {
            return Err("position must be > 0".into());
        }
        Ok(Some((file.to_string(), pos)))
    }
}

// ── Export args ────────────────────────────────────────────────────────────────

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Export events from a GlueSQL database to JSON or CSV.")]
pub struct ExportArgs {
    /// Path to the GlueSQL (sled) database directory to read from.
    #[arg(long)]
    pub gluesql_path: String,

    /// Output format: "json" (array of objects) or "csv" (with header row).
    #[arg(long, default_value = "json",
          value_parser = ["json", "csv"])]
    pub format: String,

    /// Write output to this file path instead of stdout.
    #[arg(long)]
    pub output: Option<String>,

    /// Only export events for this database name (exact match; omit for all).
    #[arg(long)]
    pub db_filter: Option<String>,

    /// Only export events for this table name (exact match; omit for all).
    #[arg(long)]
    pub table_filter: Option<String>,

    /// Only export events of this operation type: INSERT | UPDATE | DELETE (omit for all).
    #[arg(long, value_parser = ["INSERT", "UPDATE", "DELETE"])]
    pub operation: Option<String>,

    /// Maximum number of rows to export (omit for all).
    #[arg(long)]
    pub limit: Option<u64>,
}

// ── BinlogInfo args ────────────────────────────────────────────────────────────

#[derive(clap::Args, Debug, Clone)]
#[command(
    about = "Show available binlog files and the current write position.",
    after_help = crate::privileges::HELP_MYSQL_PRIVILEGES
)]
pub struct BinlogInfoArgs {
    /// MySQL host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// MySQL port
    #[arg(long, default_value_t = 3306)]
    pub port: u16,

    /// MySQL user (needs REPLICATION CLIENT + REPLICATION SLAVE)
    #[arg(long, default_value = "root")]
    pub user: String,

    /// MySQL password (or set MYSQL_PASSWORD env var)
    #[arg(long, env = "MYSQL_PASSWORD")]
    pub password: String,

    /// Output format: "table" (human-readable) or "json"
    #[arg(long, default_value = "table",
          value_parser = ["table", "json"])]
    pub format: String,

    /// Replication server ID used to open the binlog stream for timestamp scanning.
    /// Only needed with --since / --until. Must be unique within the cluster.
    #[arg(long, default_value_t = 200)]
    pub server_id: u32,

    /// Show only binlog files whose events fall on or after this datetime.
    /// Accepted formats: RFC 3339 (2026-04-17T10:00:00Z, 2026-04-17T10:00:00+08:00)
    /// or 'YYYY-MM-DD HH:MM:SS' (UTC).
    #[arg(long)]
    pub since: Option<String>,

    /// Show only binlog files whose events fall on or before this datetime.
    /// Same format as --since.
    #[arg(long)]
    pub until: Option<String>,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildmatch_exact() {
        assert!(wildmatch("foo", "foo"));
        assert!(!wildmatch("foo", "bar"));
    }

    #[test]
    fn wildmatch_star() {
        assert!(wildmatch("app_*", "app_users"));
        assert!(wildmatch("app_*", "app_"));
        assert!(!wildmatch("app_*", "other_users"));
        assert!(wildmatch("*", "anything"));
        assert!(wildmatch("*", ""));
        assert!(wildmatch("*log*", "binlog_events"));
    }

    #[test]
    fn wildmatch_question() {
        assert!(wildmatch("db?", "db1"));
        assert!(wildmatch("db?", "dbX"));
        assert!(!wildmatch("db?", "db12"));
        assert!(!wildmatch("db?", "db"));
    }

    #[test]
    fn should_include_no_filter() {
        let args = Args {
            host: "".into(), port: 3306, user: "".into(), password: "".into(),
            metadata_user: None, metadata_password: None,
            server_id: 1, log_file: "".into(), log_max_bytes: 0,
            databases: "".into(),
            tables: "".into(), field_filters: vec![], log_level: "info".into(),
            gluesql_path: None, store_mode: "id-only".into(),
            binlog_start: "end".into(),
            since: None,
            checkpoint_path: None,
            gtid: "off".into(),
        };
        assert!(args.should_include("any_db", "any_table"));
    }

    #[test]
    fn should_include_wildcard_db() {
        let args = Args {
            host: "".into(), port: 3306, user: "".into(), password: "".into(),
            metadata_user: None, metadata_password: None,
            server_id: 1, log_file: "".into(), log_max_bytes: 0,
            databases: "app_*,legacy".into(),
            tables: "".into(), field_filters: vec![], log_level: "info".into(),
            gluesql_path: None, store_mode: "id-only".into(),
            binlog_start: "end".into(),
            since: None,
            checkpoint_path: None,
            gtid: "off".into(),
        };
        assert!(args.should_include("app_users", "events"));
        assert!(args.should_include("legacy", "orders"));
        assert!(!args.should_include("other_db", "events"));
    }

    #[test]
    fn should_include_wildcard_table() {
        let args = Args {
            host: "".into(), port: 3306, user: "".into(), password: "".into(),
            metadata_user: None, metadata_password: None,
            server_id: 1, log_file: "".into(), log_max_bytes: 0,
            databases: "".into(),
            tables: "order_*,user?".into(), field_filters: vec![], log_level: "info".into(),
            gluesql_path: None, store_mode: "id-only".into(),
            binlog_start: "end".into(),
            since: None,
            checkpoint_path: None,
            gtid: "off".into(),
        };
        assert!(args.should_include("db", "order_items"));
        assert!(args.should_include("db", "user1"));
        assert!(!args.should_include("db", "products"));
    }

    #[test]
    fn parse_binlog_start_variants() {
        let mut args = Args {
            host: "".into(), port: 3306, user: "".into(), password: "".into(),
            metadata_user: None, metadata_password: None,
            server_id: 1, log_file: "".into(), log_max_bytes: 0,
            databases: "".into(),
            tables: "".into(), field_filters: vec![], log_level: "info".into(),
            gluesql_path: None, store_mode: "id-only".into(),
            binlog_start: "end".into(),
            since: None,
            checkpoint_path: None,
            gtid: "off".into(),
        };
        assert_eq!(args.parse_binlog_start().unwrap(), BinlogStart::End);

        args.binlog_start = "start".into();
        assert_eq!(args.parse_binlog_start().unwrap(), BinlogStart::Start);

        args.binlog_start = "mysql-bin.042863:380228940".into();
        assert_eq!(
            args.parse_binlog_start().unwrap(),
            BinlogStart::At { file: "mysql-bin.042863".into(), pos: 380228940 }
        );

        // File name with dots and colons should still work — the rightmost colon is the separator
        args.binlog_start = "mysql-bin.000001:4".into();
        assert_eq!(
            args.parse_binlog_start().unwrap(),
            BinlogStart::At { file: "mysql-bin.000001".into(), pos: 4 }
        );

        args.binlog_start = "bad-value".into();
        assert!(args.parse_binlog_start().is_err());

        args.binlog_start = "mysql-bin.000001:notanumber".into();
        assert!(args.parse_binlog_start().is_err());
    }
}
