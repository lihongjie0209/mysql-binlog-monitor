use anyhow::{anyhow, Result};
use gluesql::prelude::*;
use serde_json::{Map, Value as JsonValue};
use std::io::{self, BufWriter, Write};

use crate::config::ExportArgs;

// ── Entry point ────────────────────────────────────────────────────────────────

pub async fn run_export(args: ExportArgs) -> Result<()> {
    let storage = SledStorage::new(&args.gluesql_path)
        .map_err(|e| anyhow!("Cannot open GlueSQL at '{}': {e}", args.gluesql_path))?;
    let mut glue = Glue::new(storage);

    let sql = build_query(&args);
    let payloads = glue
        .execute(&sql)
        .await
        .map_err(|e| anyhow!("Query failed: {e}"))?;

    // Open output writer
    let stdout = io::stdout();
    let mut writer: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(BufWriter::new(
            std::fs::File::create(path)
                .map_err(|e| anyhow!("Cannot create output file '{}': {e}", path))?,
        )),
        None => Box::new(BufWriter::new(stdout.lock())),
    };

    for payload in payloads {
        match payload {
            Payload::Select { labels, rows } => match args.format.as_str() {
                "csv" => write_csv(&labels, &rows, &mut writer)?,
                _     => write_json(&labels, &rows, &mut writer)?,
            },
            _ => {}
        }
    }

    writer.flush()?;
    Ok(())
}

// ── Query builder ──────────────────────────────────────────────────────────────

fn build_query(args: &ExportArgs) -> String {
    let mut conditions: Vec<String> = Vec::new();

    if let Some(db) = &args.db_filter {
        conditions.push(format!("db_name = '{}'", db.replace('\'', "''")));
    }
    if let Some(tbl) = &args.table_filter {
        conditions.push(format!("table_name = '{}'", tbl.replace('\'', "''")));
    }
    if let Some(op) = &args.operation {
        conditions.push(format!("operation = '{}'", op.replace('\'', "''")));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let limit_clause = match args.limit {
        Some(n) => format!(" LIMIT {n}"),
        None    => String::new(),
    };

    format!(
        "SELECT id, captured_at, event_time, operation, db_name, table_name, primary_key, row_data \
         FROM binlog_events{where_clause} ORDER BY id{limit_clause}"
    )
}

// ── JSON output ────────────────────────────────────────────────────────────────

fn write_json(labels: &[String], rows: &[Vec<Value>], w: &mut dyn Write) -> Result<()> {
    let mut records: Vec<JsonValue> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut obj = Map::new();
        for (label, cell) in labels.iter().zip(row.iter()) {
            obj.insert(label.clone(), glue_to_json(cell));
        }
        records.push(JsonValue::Object(obj));
    }
    let json = serde_json::to_string_pretty(&records)?;
    writeln!(w, "{json}")?;
    Ok(())
}

// ── CSV output ────────────────────────────────────────────────────────────────

fn write_csv(labels: &[String], rows: &[Vec<Value>], w: &mut dyn Write) -> Result<()> {
    // Header
    writeln!(w, "{}", labels.iter().map(|l| csv_field(l)).collect::<Vec<_>>().join(","))?;
    // Rows
    for row in rows {
        let fields: Vec<String> = row.iter().map(|cell| csv_field(&glue_to_string(cell))).collect();
        writeln!(w, "{}", fields.join(","))?;
    }
    Ok(())
}

/// Wrap a field in double-quotes if it contains commas, quotes, or newlines.
fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

// ── Value conversion ───────────────────────────────────────────────────────────

fn glue_to_json(v: &Value) -> JsonValue {
    match v {
        Value::I8(n)  => JsonValue::Number((*n).into()),
        Value::I16(n) => JsonValue::Number((*n).into()),
        Value::I32(n) => JsonValue::Number((*n).into()),
        Value::I64(n) => JsonValue::Number((*n).into()),
        Value::U8(n)  => JsonValue::Number((*n).into()),
        Value::U16(n) => JsonValue::Number((*n).into()),
        Value::U32(n) => JsonValue::Number((*n).into()),
        Value::U64(n) => JsonValue::Number((*n).into()),
        Value::F32(n) => serde_json::Number::from_f64(*n as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::F64(n) => serde_json::Number::from_f64(*n)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Bool(b)   => JsonValue::Bool(*b),
        Value::Str(s)    => JsonValue::String(s.clone()),
        Value::Bytea(b)  => JsonValue::String(
            b.iter().map(|byte| format!("{byte:02x}")).collect()
        ),
        Value::Null      => JsonValue::Null,
        other            => JsonValue::String(format!("{other:?}")),
    }
}

fn glue_to_string(v: &Value) -> String {
    match glue_to_json(v) {
        JsonValue::Null        => String::new(),
        JsonValue::String(s)   => s,
        other                  => other.to_string(),
    }
}
