//! Shared binlog row → JSON conversion used by `monitor` and `scan`.

use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;
use serde_json::{json, Value as JsonValue};

fn bytes_to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

pub fn value_to_json(v: Value) -> JsonValue {
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

pub fn binlog_value_to_json(val: BinlogValue<'_>) -> JsonValue {
    match val {
        BinlogValue::Value(v) => value_to_json(v),
        BinlogValue::Jsonb(j) => JsonValue::String(format!("{:?}", j)),
        BinlogValue::JsonDiff(d) => JsonValue::String(format!("{:?}", d)),
    }
}

/// Convert a BinlogRow to a JSON object using `col_names` for field names.
pub fn binlog_row_to_json(row: &BinlogRow, col_names: &[String]) -> JsonValue {
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

/// Extract the primary key value from a row's JSON object.
///
/// - Single-column PK → scalar value
/// - Composite PK     → `{ "col1": v1, "col2": v2 }`
/// - No PK metadata   → fallback: look for `id` / `ID` / `Id`; return `null` if absent
pub fn extract_pk(values: &serde_json::Map<String, JsonValue>, pk_columns: &[String]) -> JsonValue {
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
    for fallback in &["id", "ID", "Id"] {
        if let Some(v) = values.get(*fallback) {
            return v.clone();
        }
    }
    JsonValue::Null
}

/// Build the standard change-event JSON object.
pub fn build_change_event(
    event_time: &str,
    ts_unix: u32,
    operation: &str,
    database: &str,
    table: &str,
    pk_columns: &[String],
    primary_key: JsonValue,
    row_value: JsonValue,
    gtid: Option<&str>,
) -> JsonValue {
    let mut event_json = json!({
        "timestamp":   event_time,
        "event_time":  ts_unix,
        "operation":   operation,
        "database":    database,
        "table":       table,
        "pk_columns":  if pk_columns.is_empty() { JsonValue::Null } else { json!(pk_columns) },
        "primary_key": primary_key,
        "row":         row_value,
    });
    if let Some(g) = gtid {
        event_json
            .as_object_mut()
            .unwrap()
            .insert("gtid".into(), JsonValue::String(g.to_string()));
    }
    event_json
}

/// Whether a table event should be emitted given optional db/table filter lists.
pub fn should_include(
    filter_databases: &[String],
    filter_tables: &[String],
    database: &str,
    table: &str,
    match_fn: impl Fn(&[String], &str) -> bool,
) -> bool {
    if !filter_databases.is_empty() && !match_fn(filter_databases, database) {
        return false;
    }
    if !filter_tables.is_empty() && !match_fn(filter_tables, table) {
        return false;
    }
    true
}
