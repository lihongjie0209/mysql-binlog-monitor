use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::{BinlogStreamRequest, Opts, Pool, Value};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use serde_json::{json, Value as JsonValue};

use crate::config::Args;
use crate::db::{ColMap, PkMap};
use crate::logger::Logger;
use crate::storage::{EventStorage, StoreMode};

// ── Value serialization ────────────────────────────────────────────────────────

fn bytes_to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn value_to_json(v: Value) -> JsonValue {
    match v {
        Value::NULL => JsonValue::Null,
        Value::Bytes(b) => match String::from_utf8(b) {
            Ok(s) => JsonValue::String(s),
            Err(e) => JsonValue::String(format!("0x{}", bytes_to_hex(e.as_bytes()))),
        },
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f as f64),
        Value::Double(d) => json!(d),
        Value::Date(y, mo, d, h, mi, s, us) => {
            if h == 0 && mi == 0 && s == 0 && us == 0 {
                JsonValue::String(format!("{:04}-{:02}-{:02}", y, mo, d))
            } else {
                JsonValue::String(format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                    y, mo, d, h, mi, s, us
                ))
            }
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let total_h = days * 24 + h as u32;
            let sign = if neg { "-" } else { "" };
            JsonValue::String(format!("{}{:02}:{:02}:{:02}.{:06}", sign, total_h, mi, s, us))
        }
    }
}

fn binlog_value_to_json(val: BinlogValue<'_>) -> JsonValue {
    match val {
        BinlogValue::Value(v) => value_to_json(v),
        // Serialize JSONB and JsonDiff as debug strings — rarely encountered in practice
        BinlogValue::Jsonb(j) => JsonValue::String(format!("{:?}", j)),
        BinlogValue::JsonDiff(d) => JsonValue::String(format!("{:?}", d)),
    }
}

/// Convert a BinlogRow to a JSON object using `col_names` for field names.
/// Falls back to `col_0`, `col_1`, ... when the name list is shorter than the row.
fn binlog_row_to_json(row: &BinlogRow, col_names: &[String]) -> JsonValue {
    let mut map = serde_json::Map::new();
    for i in 0..row.len() {
        let key = col_names
            .get(i)
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("col_{}", i));
        let json_val = match row.as_ref(i) {
            None => JsonValue::Null,
            Some(bv) => binlog_value_to_json(bv.clone()),
        };
        map.insert(key, json_val);
    }
    JsonValue::Object(map)
}

// ── Primary key extraction ─────────────────────────────────────────────────────

/// Extract the primary key value from a row's JSON object using the cached PK column list.
///
/// - Single-column PK → scalar value
/// - Composite PK     → `{ "col1": v1, "col2": v2 }`
/// - No PK metadata   → fallback: look for `id` / `ID` / `Id`; return `null` if absent
fn extract_pk(values: &serde_json::Map<String, JsonValue>, pk_columns: &[String]) -> JsonValue {
    if !pk_columns.is_empty() {
        if pk_columns.len() == 1 {
            return values
                .get(&pk_columns[0])
                .cloned()
                .unwrap_or(JsonValue::Null);
        }
        let mut m = serde_json::Map::new();
        for col in pk_columns {
            m.insert(col.clone(), values.get(col).cloned().unwrap_or(JsonValue::Null));
        }
        return JsonValue::Object(m);
    }
    // Fallback: common naming conventions
    for fallback in &["id", "ID", "Id"] {
        if let Some(v) = values.get(*fallback) {
            return v.clone();
        }
    }
    JsonValue::Null
}

// ── Monitor ────────────────────────────────────────────────────────────────────

pub async fn run_monitor(args: Args, shutdown: CancellationToken) -> Result<()> {
    let logger = Logger::new(&args.log_file, &args.log_level)
        .context("Failed to open log file")?;

    // Binlog stream URL (replication user)
    let stream_url = format!(
        "mysql://{}:{}@{}:{}/",
        args.user, args.password, args.host, args.port
    );

    // Metadata pool: use dedicated credentials if provided, otherwise reuse the stream user
    let meta_user = args.metadata_user.as_deref().unwrap_or(&args.user);
    let meta_pass = args.metadata_password.as_deref().unwrap_or(&args.password);
    let meta_url = format!(
        "mysql://{}:{}@{}:{}/",
        meta_user, meta_pass, args.host, args.port
    );
    let meta_pool = Pool::new(meta_url.as_str());

    // ── GlueSQL storage (optional) ─────────────────────────────────────────────
    let mut event_storage: Option<EventStorage> = match &args.gluesql_path {
        Some(path) => {
            let mode = StoreMode::from_str(&args.store_mode);
            match EventStorage::new(path, mode).await {
                Ok(s) => {
                    logger.info(json!({
                        "message": "GlueSQL storage opened",
                        "path": path,
                        "store_mode": args.store_mode
                    }));
                    Some(s)
                }
                Err(e) => {
                    logger.warn(json!({ "message": "Failed to open GlueSQL storage, events will not be persisted to DB", "error": e.to_string() }));
                    None
                }
            }
        }
        None => None,
    };

    // ── Fetch column + PK metadata ─────────────────────────────────────────────
    let mut col_map: ColMap = match crate::db::fetch_all_column_names(&meta_pool).await {
        Ok(m) => {
            logger.info(json!({ "message": "Fetched column metadata", "tables": m.len() }));
            m
        }
        Err(e) => {
            logger.warn(json!({ "message": "Could not fetch column names; using col_N keys", "error": e.to_string() }));
            HashMap::new()
        }
    };

    let mut pk_map: PkMap = match crate::db::fetch_all_primary_keys(&meta_pool).await {
        Ok(m) => {
            logger.info(json!({ "message": "Fetched primary key metadata", "tables_with_pk": m.len() }));
            m
        }
        Err(e) => {
            logger.warn(json!({ "message": "Could not fetch PK metadata; falling back to 'id' column", "error": e.to_string() }));
            HashMap::new()
        }
    };

    // ── Stream loop with exponential-backoff reconnect ─────────────────────────
    // On connection drop the inner loop breaks with stream_error=true; we then
    // wait before reconnecting (1 s → 2 s → 4 s … capped at 60 s).
    // A successful stream run resets the delay back to 1 s.
    let opts = Opts::from_url(&stream_url).context("Invalid MySQL URL")?;
    let mut backoff = Duration::from_secs(1);

    'reconnect: loop {
        // ── Connect ──────────────────────────────────────────────────────────
        let conn = match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => c,
            Err(e) => {
                logger.warn(json!({
                    "message": "Failed to connect for binlog stream, retrying",
                    "error": e.to_string(),
                    "retry_in_secs": backoff.as_secs()
                }));
                tokio::select! {
                    _ = sleep(backoff) => {}
                    _ = shutdown.cancelled() => break 'reconnect,
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue 'reconnect;
            }
        };

        let request = BinlogStreamRequest::new(args.server_id);
        let mut stream = match conn.get_binlog_stream(request).await {
            Ok(s) => s,
            Err(e) => {
                logger.warn(json!({
                    "message": "Failed to start binlog stream, retrying",
                    "error": e.to_string(),
                    "retry_in_secs": backoff.as_secs()
                }));
                tokio::select! {
                    _ = sleep(backoff) => {}
                    _ = shutdown.cancelled() => break 'reconnect,
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue 'reconnect;
            }
        };

        logger.info(json!({
            "message": "MySQL binlog monitor started",
            "host": args.host,
            "port": args.port,
            "server_id": args.server_id,
            "filter_databases": if args.filter_databases().is_empty() { json!("all") } else { json!(args.filter_databases()) },
            "filter_tables":    if args.filter_tables().is_empty()    { json!("all") } else { json!(args.filter_tables()) },
        }));

        // Reset backoff on successful connect
        backoff = Duration::from_secs(1);

        // ── Event loop ───────────────────────────────────────────────────────
        loop {
            tokio::select! {
                maybe_event = stream.next() => {
                    let event = match maybe_event {
                        None => break,
                        Some(Err(e)) => {
                            logger.warn(json!({ "message": "Binlog stream error", "error": e.to_string() }));
                            break;
                        }
                        Some(Ok(ev)) => ev,
                    };

                let ts_unix = event.header().timestamp();
                let event_time = Utc
                    .timestamp_opt(ts_unix as i64, 0)
                    .single()
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default();

                // Parse event data — EventData borrows from `event`
                let data = match event.read_data() {
                    Ok(Some(d)) => d,
                    _ => continue,
                };

                let re = match data {
                    EventData::RowsEvent(re) => re,
                    _ => continue,
                };

                let operation = match &re {
                    RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => "INSERT",
                    RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_)
                    | RowsEventData::PartialUpdateRowsEvent(_) => "UPDATE",
                    RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => "DELETE",
                };

                let table_id = re.table_id();
                let tme = match stream.get_tme(table_id) {
                    Some(t) => t,
                    None => continue,
                };

                let database = tme.database_name().to_string();
                let table    = tme.table_name().to_string();

                if !args.should_include(&database, &table) {
                    continue;
                }

                // Lazy refresh: on first encounter of a table, fetch only that table's metadata
                let tbl_key = (database.clone(), table.clone());
                if !col_map.contains_key(&tbl_key) {
                    logger.info(json!({ "message": "New table detected, fetching metadata", "database": database, "table": table }));
                    if let Ok(cols) = crate::db::fetch_column_names_for_table(&meta_pool, &database, &table).await {
                        col_map.insert(tbl_key.clone(), cols);
                    }
                    let pks = crate::db::fetch_primary_keys_for_table(&meta_pool, &database, &table)
                        .await
                        .unwrap_or_default();
                    pk_map.insert(tbl_key.clone(), pks);
                }

                let col_names  = col_map.get(&tbl_key).cloned().unwrap_or_default();
                let pk_columns = pk_map.get(&tbl_key).cloned().unwrap_or_default();

                // Collect rows into owned Vec so we can release the `tme` borrow
                let rows: Vec<_> = re.rows(tme).collect();

                for row_result in rows {
                    let (before, after) = match row_result {
                        Ok(pair) => pair,
                        Err(e) => {
                            logger.warn(json!({ "message": "Failed to parse binlog row", "error": e.to_string() }));
                            continue;
                        }
                    };

                    let (pk_source_obj, row_value) = match operation {
                        "INSERT" => {
                            let after_json = binlog_row_to_json(after.as_ref().unwrap(), &col_names);
                            let obj = after_json.as_object().unwrap().clone();
                            (obj, json!({ "values": after_json }))
                        }
                        "DELETE" => {
                            let before_json = binlog_row_to_json(before.as_ref().unwrap(), &col_names);
                            let obj = before_json.as_object().unwrap().clone();
                            (obj, json!({ "values": before_json }))
                        }
                        _ => {
                            let before_json = binlog_row_to_json(before.as_ref().unwrap(), &col_names);
                            let after_json  = binlog_row_to_json(after.as_ref().unwrap(),  &col_names);
                            let obj = after_json.as_object().unwrap().clone();
                            (obj, json!({ "before_values": before_json, "after_values": after_json }))
                        }
                    };

                    let primary_key = extract_pk(&pk_source_obj, &pk_columns);

                    let event_json = json!({
                        "timestamp":   event_time,
                        "event_time":  ts_unix,
                        "operation":   operation,
                        "database":    database,
                        "table":       table,
                        "pk_columns":  if pk_columns.is_empty() { JsonValue::Null } else { json!(pk_columns) },
                        "primary_key": primary_key,
                        "row":         row_value,
                    });

                    logger.info(event_json.clone());

                    if let Some(storage) = event_storage.as_mut() {
                        if let Err(e) = storage.insert(&event_json).await {
                            logger.warn(json!({ "message": "GlueSQL insert failed", "error": e.to_string() }));
                        }
                    }
                }
            }
            _ = shutdown.cancelled() => {
                logger.info(json!({ "message": "Received Ctrl+C, shutting down" }));
                break 'reconnect;
            }
        }
        } // end inner event loop

        backoff = (backoff * 2).min(Duration::from_secs(60));
        logger.warn(json!({
            "message": "Binlog stream disconnected, reconnecting",
            "retry_in_secs": backoff.as_secs()
        }));
        tokio::select! {
            _ = sleep(backoff) => {}
            _ = shutdown.cancelled() => break 'reconnect,
        }
    } // end 'reconnect

    meta_pool.disconnect().await?;
    Ok(())
}
