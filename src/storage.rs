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
                row_data    TEXT,
                gtid        TEXT
            )",
        )
        .await
        .map_err(|e| anyhow!("GlueSQL CREATE TABLE binlog_events failed: {e}"))?;

        // Separate table for exactly-once GTID dedupe (works even if event rows
        // come from mixed store modes).
        glue.execute(
            "CREATE TABLE IF NOT EXISTS processed_gtids (
                gtid TEXT
            )",
        )
        .await
        .map_err(|e| anyhow!("GlueSQL CREATE TABLE processed_gtids failed: {e}"))?;

        let results = glue
            .execute("SELECT MAX(id) FROM binlog_events")
            .await
            .map_err(|e| anyhow!("{e}"))?;

        let next_id = extract_max_id(&results).map(|n| n + 1).unwrap_or(1);

        Ok(Self { glue, mode, next_id })
    }

    /// Insert one binlog event. Returns `Ok(true)` if inserted, `Ok(false)` if
    /// skipped as a GTID duplicate (exactly-once local sink).
    pub async fn insert(&mut self, event: &JsonValue) -> Result<bool> {
        let gtid = event
            .get("gtid")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        if let Some(ref g) = gtid {
            if self.gtid_seen(g).await? {
                return Ok(false);
            }
        }

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
        let gtid_sql = match &gtid {
            Some(g) => format!("'{}'", sq(g)),
            None => "NULL".to_string(),
        };

        let sql = format!(
            "INSERT INTO binlog_events VALUES ({id}, '{ca}', '{et}', '{op}', '{db}', '{tbl}', '{pk}', {row}, {gtid})",
            id  = id,
            ca  = sq(&captured_at),
            et  = sq(&event_time),
            op  = sq(&operation),
            db  = sq(&db_name),
            tbl = sq(&table_name),
            pk  = sq(&primary_key),
            row = row_data_sql,
            gtid = gtid_sql,
        );

        self.glue
            .execute(&sql)
            .await
            .map_err(|e| anyhow!("GlueSQL INSERT failed: {e}"))?;

        if let Some(g) = gtid {
            let mark = format!("INSERT INTO processed_gtids VALUES ('{}')", sq(&g));
            self.glue
                .execute(&mark)
                .await
                .map_err(|e| anyhow!("GlueSQL GTID mark failed: {e}"))?;
        }

        Ok(true)
    }

    async fn gtid_seen(&mut self, gtid: &str) -> Result<bool> {
        let sql = format!(
            "SELECT gtid FROM processed_gtids WHERE gtid = '{}'",
            sq(gtid)
        );
        let results = self
            .glue
            .execute(&sql)
            .await
            .map_err(|e| anyhow!("GlueSQL GTID lookup failed: {e}"))?;
        for payload in results {
            if let Payload::Select { rows, .. } = payload {
                if !rows.is_empty() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn sq(s: &str) -> String {
    s.replace('\'', "''")
}

fn extract_max_id(payloads: &[Payload]) -> Option<i64> {
    for payload in payloads {
        if let Payload::Select { rows, .. } = payload {
            if let Some(row) = rows.first() {
                return match row.first() {
                    Some(Value::I64(n)) => Some(*n),
                    Some(Value::I32(n)) => Some(*n as i64),
                    _ => None,
                };
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn gtid_dedupe_skips_second_insert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut storage = EventStorage::new(path, StoreMode::IdOnly).await.unwrap();

        let event = json!({
            "timestamp": "2026-01-01T00:00:00Z",
            "operation": "INSERT",
            "database": "db",
            "table": "t",
            "primary_key": 1,
            "row": { "values": { "id": 1 } },
            "gtid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1"
        });

        assert!(storage.insert(&event).await.unwrap());
        assert!(!storage.insert(&event).await.unwrap());

        let results = storage
            .glue
            .execute("SELECT id FROM binlog_events")
            .await
            .unwrap();
        let mut count = 0;
        for p in results {
            if let Payload::Select { rows, .. } = p {
                count += rows.len();
            }
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn insert_without_gtid_always_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut storage = EventStorage::new(path, StoreMode::IdOnly).await.unwrap();

        let event = json!({
            "timestamp": "2026-01-01T00:00:00Z",
            "operation": "INSERT",
            "database": "db",
            "table": "t",
            "primary_key": 1,
            "row": { "values": { "id": 1 } }
        });

        assert!(storage.insert(&event).await.unwrap());
        assert!(storage.insert(&event).await.unwrap());

        let results = storage
            .glue
            .execute("SELECT id FROM binlog_events")
            .await
            .unwrap();
        let mut count = 0;
        for p in results {
            if let Payload::Select { rows, .. } = p {
                count += rows.len();
            }
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn different_gtids_both_insert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut storage = EventStorage::new(path, StoreMode::Full).await.unwrap();

        let base = json!({
            "timestamp": "2026-01-01T00:00:00Z",
            "operation": "INSERT",
            "database": "db",
            "table": "t",
            "primary_key": 1,
            "row": { "values": { "id": 1 } }
        });
        let mut e1 = base.clone();
        e1.as_object_mut().unwrap().insert(
            "gtid".into(),
            json!("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1"),
        );
        let mut e2 = base;
        e2.as_object_mut().unwrap().insert(
            "gtid".into(),
            json!("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2"),
        );

        assert!(storage.insert(&e1).await.unwrap());
        assert!(storage.insert(&e2).await.unwrap());

        let results = storage
            .glue
            .execute("SELECT gtid FROM binlog_events ORDER BY id")
            .await
            .unwrap();
        let mut gtids = Vec::new();
        for p in results {
            if let Payload::Select { rows, .. } = p {
                for row in rows {
                    if let Some(Value::Str(s)) = row.first() {
                        gtids.push(s.clone());
                    }
                }
            }
        }
        assert_eq!(gtids.len(), 2);
        assert_ne!(gtids[0], gtids[1]);
    }
}
