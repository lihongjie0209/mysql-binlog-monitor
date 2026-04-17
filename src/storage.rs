use anyhow::{anyhow, Result};
use gluesql::prelude::*;
use serde_json::Value as JsonValue;

// ── Store mode ─────────────────────────────────────────────────────────────────

/// Controls how much row data is persisted in GlueSQL.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StoreMode {
    /// Only store primary key + metadata (operation, db, table, timestamps).
    IdOnly,
    /// Store everything including the full row JSON.
    Full,
}

impl StoreMode {
    pub fn from_str(s: &str) -> Self {
        if s == "full" { StoreMode::Full } else { StoreMode::IdOnly }
    }
}

// ── EventStorage ───────────────────────────────────────────────────────────────

pub struct EventStorage {
    pub glue: Glue<SledStorage>,
    pub mode: StoreMode,
    next_id: i64,
}

impl EventStorage {
    /// Open (or create) the GlueSQL sled database at `path`.
    pub async fn new(path: &str, mode: StoreMode) -> Result<Self> {
        let storage = SledStorage::new(path)
            .map_err(|e| anyhow!("GlueSQL sled open failed at {path}: {e}"))?;
        let mut glue = Glue::new(storage);

        glue.execute(
            "CREATE TABLE IF NOT EXISTS binlog_events (
                id          INTEGER,
                captured_at TEXT,
                event_time  TEXT,
                operation   TEXT,
                db_name     TEXT,
                table_name  TEXT,
                primary_key TEXT,
                row_data    TEXT
            )",
        )
        .await
        .map_err(|e| anyhow!("GlueSQL CREATE TABLE failed: {e}"))?;

        // Resume the auto-increment counter from the persisted max id.
        let results = glue
            .execute("SELECT MAX(id) FROM binlog_events")
            .await
            .map_err(|e| anyhow!("{e}"))?;

        let next_id = extract_max_id(&results).map(|n| n + 1).unwrap_or(1);

        Ok(Self { glue, mode, next_id })
    }

    /// Insert one binlog event into the GlueSQL table.
    ///
    /// `event` is the same JSON object that is written to the log file.
    pub async fn insert(&mut self, event: &JsonValue) -> Result<()> {
        let id = self.next_id;
        self.next_id += 1;

        let captured_at = chrono::Utc::now().to_rfc3339();
        let event_time  = event["timestamp"].as_str().unwrap_or("").to_string();
        let operation   = event["operation"].as_str().unwrap_or("").to_string();
        let db_name     = event["database"].as_str().unwrap_or("").to_string();
        let table_name  = event["table"].as_str().unwrap_or("").to_string();
        let primary_key = event["primary_key"].to_string();

        let row_data_sql = match self.mode {
            StoreMode::Full   => format!("'{}'", sq(&event["row"].to_string())),
            StoreMode::IdOnly => "NULL".to_string(),
        };

        let sql = format!(
            "INSERT INTO binlog_events VALUES ({id}, '{ca}', '{et}', '{op}', '{db}', '{tbl}', '{pk}', {row})",
            id  = id,
            ca  = sq(&captured_at),
            et  = sq(&event_time),
            op  = sq(&operation),
            db  = sq(&db_name),
            tbl = sq(&table_name),
            pk  = sq(&primary_key),
            row = row_data_sql,
        );

        self.glue
            .execute(&sql)
            .await
            .map_err(|e| anyhow!("GlueSQL INSERT failed: {e}"))?;

        Ok(())
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Escape single-quote characters for SQL string literals.
fn sq(s: &str) -> String {
    s.replace('\'', "''")
}

/// Extract the integer from `SELECT MAX(id) FROM binlog_events`.
/// Returns `None` when the table is empty (MAX returns NULL).
fn extract_max_id(payloads: &[Payload]) -> Option<i64> {
    for payload in payloads {
        if let Payload::Select { rows, .. } = payload {
            if let Some(row) = rows.first() {
                return match row.first() {
                    Some(Value::I64(n)) => Some(*n),
                    Some(Value::I32(n)) => Some(*n as i64),
                    _ => None, // Value::Null when table is empty
                };
            }
        }
    }
    None
}
