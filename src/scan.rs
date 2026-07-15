//! One-shot historical binlog scan — filter by db/table/time, emit NDJSON, exit.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::{BinlogStreamRequest, Opts, OptsBuilder, Pool};
use serde_json::json;

use crate::config::ScanArgs;
use crate::db::{self, ColMap, PkMap};
use crate::event::{binlog_row_to_json, build_change_event, extract_pk};
use crate::field_filter::{self, FieldPredicate};
use crate::gtid::{format_gtid, GtidPreference};
use crate::time_seek::{self, parse_datetime};

/// Run a non-following historical scan and exit when done.
pub async fn run_scan(args: ScanArgs) -> Result<()> {
    let since_ts = args
        .since
        .as_deref()
        .map(parse_datetime)
        .transpose()
        .context("Bad --since")?;
    let until_ts = args
        .until
        .as_deref()
        .map(parse_datetime)
        .transpose()
        .context("Bad --until")?;

    if let (Some(s), Some(u)) = (since_ts, until_ts) {
        if s > u {
            return Err(anyhow!("--since ({s}) is after --until ({u})"));
        }
    }

    let field_preds: Vec<FieldPredicate> = args
        .field_predicates()
        .map_err(|e| anyhow!(e))?;

    let meta_user = args.metadata_user.as_deref().unwrap_or(&args.user);
    let meta_pass = args.metadata_password.as_deref().unwrap_or(&args.password);
    let meta_url = format!(
        "mysql://{}:{}@{}:{}/",
        meta_user, meta_pass, args.host, args.port
    );
    let stream_url = format!(
        "mysql://{}:{}@{}:{}/",
        args.user, args.password, args.host, args.port
    );

    let meta_pool = Pool::new(Opts::from_url(&meta_url).context("Invalid MySQL URL")?);
    let stream_check_pool =
        Pool::new(Opts::from_url(&stream_url).context("Invalid MySQL stream URL")?);

    // Fail fast with CREATE USER / GRANT guidance when binlog privileges are missing
    let stream_warnings =
        crate::privileges::require_stream_privileges(&stream_check_pool, &args.user).await?;
    let _ = stream_check_pool.disconnect().await;
    if args.metadata_user.is_none() {
        for w in stream_warnings {
            eprintln!("{}", json!({ "message": "privilege_warning", "detail": w }));
        }
    }
    if let Some(w) = crate::privileges::metadata_select_warning(&meta_pool, meta_user).await {
        eprintln!(
            "{}",
            json!({
                "message": "metadata_privilege_warning",
                "user": meta_user,
                "guidance": w,
            })
        );
    }

    let mut col_map: ColMap = db::fetch_all_column_names(&meta_pool)
        .await
        .unwrap_or_default();
    let mut pk_map: PkMap = db::fetch_all_primary_keys(&meta_pool)
        .await
        .unwrap_or_default();

    let files = db::fetch_binary_logs(&meta_pool).await?;
    let (cur_file, cur_pos) = db::fetch_master_status(&meta_pool).await?;

    // ── Resolve start position ───────────────────────────────────────────────
    let (start_file, start_pos, use_gtid, gtid_set_str) =
        resolve_start(&args, &meta_pool, &files, &cur_file, cur_pos, since_ts).await?;

    eprintln!(
        "{}",
        json!({
            "message": "scan_start",
            "file": start_file,
            "pos": start_pos,
            "gtid": use_gtid,
            "gtid_set": gtid_set_str,
            "since": args.since,
            "until": args.until,
            "databases": if args.filter_databases().is_empty() { json!("all") } else { json!(args.filter_databases()) },
            "tables": if args.filter_tables().is_empty() { json!("all") } else { json!(args.filter_tables()) },
            "field_filters": args.field_filters,
            "limit": args.limit,
        })
    );

    // ── Output writer ────────────────────────────────────────────────────────
    let mut writer: Box<dyn Write> = match &args.output {
        Some(path) => {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            Box::new(BufWriter::new(
                File::create(path).with_context(|| format!("Cannot create '{path}'"))?,
            ))
        }
        None => Box::new(BufWriter::new(io::stdout().lock())),
    };

    // ── Open non-blocking binlog stream ──────────────────────────────────────
    let opts = OptsBuilder::default()
        .ip_or_hostname(args.host.clone())
        .tcp_port(args.port)
        .user(Some(args.user.clone()))
        .pass(Some(args.password.clone()));
    let _ = stream_url; // connection via OptsBuilder
    let conn = mysql_async::Conn::new(Opts::from(opts))
        .await
        .context("Failed to connect for scan")?;

    let gtid_sids = if use_gtid {
        if let Some(ref s) = gtid_set_str {
            crate::gtid::ExecutedGtidSet::parse(s)
                .ok()
                .and_then(|set| set.to_sids().ok())
        } else {
            Some(Vec::new())
        }
    } else {
        None
    };

    let request = if let Some(ref sids) = gtid_sids {
        BinlogStreamRequest::new(args.server_id)
            .with_gtid()
            .with_gtid_set(sids.clone())
            .with_non_blocking()
    } else if let (Some(file), Some(pos)) = (start_file.as_ref(), start_pos) {
        BinlogStreamRequest::new(args.server_id)
            .with_filename(file.as_bytes())
            .with_pos(pos)
            .with_non_blocking()
    } else {
        BinlogStreamRequest::new(args.server_id).with_non_blocking()
    };

    let mut stream = conn
        .get_binlog_stream(request)
        .await
        .context("Failed to open binlog stream for scan")?;

    let mut current_gtid: Option<String> = None;
    let mut emitted: u64 = 0;
    let mut scanned_rows: u64 = 0;

    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(ev) => ev,
            Err(e) => {
                // Non-blocking end / stream closed often surfaces as error or None
                eprintln!(
                    "{}",
                    json!({ "message": "scan_stream_end", "error": e.to_string(), "emitted": emitted })
                );
                break;
            }
        };

        let ts_unix = event.header().timestamp();
        if ts_unix > 0 {
            let ts = ts_unix as u64;
            if let Some(until) = until_ts {
                if ts > until {
                    break;
                }
            }
            // Skip events strictly before --since (time seek may land slightly early)
            if let Some(since) = since_ts {
                if ts < since {
                    // still parse rotate/gtid for state, but skip row emit below via flag
                }
            }
        }

        let event_time = Utc
            .timestamp_opt(ts_unix as i64, 0)
            .single()
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        let data = match event.read_data() {
            Ok(Some(d)) => d,
            _ => continue,
        };

        match data {
            EventData::GtidEvent(ref ge) => {
                current_gtid = Some(format_gtid(&ge.sid(), ge.gno()));
            }
            EventData::AnonymousGtidEvent(ref age) => {
                let ge = &age.0;
                if ge.gno() > 0 {
                    current_gtid = Some(format_gtid(&ge.sid(), ge.gno()));
                }
            }
            EventData::XidEvent(_) => {
                current_gtid = None;
            }
            EventData::RowsEvent(re) => {
                let operation = match &re {
                    RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                        "INSERT"
                    }
                    RowsEventData::UpdateRowsEvent(_)
                    | RowsEventData::UpdateRowsEventV1(_)
                    | RowsEventData::PartialUpdateRowsEvent(_) => "UPDATE",
                    RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                        "DELETE"
                    }
                };

                let table_id = re.table_id();
                let tme = match stream.get_tme(table_id) {
                    Some(t) => t,
                    None => continue,
                };
                let database = tme.database_name().to_string();
                let table = tme.table_name().to_string();

                if !args.should_include(&database, &table) {
                    continue;
                }

                // Time window (row events with ts=0 still pass if no since filter)
                if ts_unix > 0 {
                    let ts = ts_unix as u64;
                    if let Some(since) = since_ts {
                        if ts < since {
                            continue;
                        }
                    }
                    if let Some(until) = until_ts {
                        if ts > until {
                            break;
                        }
                    }
                }

                let tbl_key = (database.clone(), table.clone());
                if !col_map.contains_key(&tbl_key) {
                    if let Ok(cols) =
                        db::fetch_column_names_for_table(&meta_pool, &database, &table).await
                    {
                        col_map.insert(tbl_key.clone(), cols);
                    }
                    let pks = db::fetch_primary_keys_for_table(&meta_pool, &database, &table)
                        .await
                        .unwrap_or_default();
                    pk_map.insert(tbl_key.clone(), pks);
                }
                let col_names = col_map.get(&tbl_key).cloned().unwrap_or_default();
                let pk_columns = pk_map.get(&tbl_key).cloned().unwrap_or_default();

                let rows: Vec<_> = re.rows(tme).collect();
                for row_result in rows {
                    let (before, after) = match row_result {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    };
                    scanned_rows += 1;

                    let (pk_source_obj, row_value) = match operation {
                        "INSERT" => {
                            let after_json =
                                binlog_row_to_json(after.as_ref().unwrap(), &col_names);
                            let obj = after_json.as_object().unwrap().clone();
                            (obj, json!({ "values": after_json }))
                        }
                        "DELETE" => {
                            let before_json =
                                binlog_row_to_json(before.as_ref().unwrap(), &col_names);
                            let obj = before_json.as_object().unwrap().clone();
                            (obj, json!({ "values": before_json }))
                        }
                        _ => {
                            let before_json =
                                binlog_row_to_json(before.as_ref().unwrap(), &col_names);
                            let after_json =
                                binlog_row_to_json(after.as_ref().unwrap(), &col_names);
                            let obj = after_json.as_object().unwrap().clone();
                            (
                                obj,
                                json!({ "before_values": before_json, "after_values": after_json }),
                            )
                        }
                    };

                    if !field_filter::row_matches_fields(&row_value, &field_preds) {
                        continue;
                    }

                    let primary_key = extract_pk(&pk_source_obj, &pk_columns);
                    let event_json = build_change_event(
                        &event_time,
                        ts_unix,
                        operation,
                        &database,
                        &table,
                        &pk_columns,
                        primary_key,
                        row_value,
                        current_gtid.as_deref(),
                    );

                    writeln!(writer, "{}", serde_json::to_string(&event_json)?)?;
                    emitted += 1;

                    if let Some(lim) = args.limit {
                        if emitted >= lim {
                            writer.flush()?;
                            eprintln!(
                                "{}",
                                json!({
                                    "message": "scan_complete",
                                    "reason": "limit",
                                    "emitted": emitted,
                                    "scanned_rows": scanned_rows,
                                })
                            );
                            meta_pool.disconnect().await?;
                            return Ok(());
                        }
                    }
                }
            }
            _ => {}
        }
    }

    writer.flush()?;
    eprintln!(
        "{}",
        json!({
            "message": "scan_complete",
            "reason": "eof_or_until",
            "emitted": emitted,
            "scanned_rows": scanned_rows,
        })
    );
    meta_pool.disconnect().await?;
    Ok(())
}

/// Decide start file:pos and whether to use GTID dump.
async fn resolve_start(
    args: &ScanArgs,
    pool: &Pool,
    files: &[db::BinlogFile],
    cur_file: &str,
    cur_pos: u64,
    since_ts: Option<u64>,
) -> Result<(Option<String>, Option<u64>, bool, Option<String>)> {
    // Explicit file:pos wins over since for the dump start (since still filters events).
    if let Some((file, pos)) = args.parse_file_pos().map_err(|e| anyhow!(e))? {
        return Ok((Some(file), Some(pos), false, None));
    }

    if let Some(ts) = since_ts {
        let (file, pos) =
            time_seek::find_pos_by_time(pool, args.server_id, files, ts, cur_file, cur_pos).await?;
        // MySQL rejects dump starts at position < 4
        return Ok((Some(file), Some(pos.max(4)), false, None));
    }

    // No since / no file:pos → start of earliest retained binlog.
    // GTID auto empty set would replay *all* GTIDs which is fine for history, but
    // file-based from first log is more predictable for "scan everything we have".
    let gtid_pref = args.gtid_preference();
    let server_on = db::is_gtid_mode_on(pool).await.unwrap_or(false);
    let gtid_on = gtid_pref.resolve(server_on);

    if gtid_on && matches!(gtid_pref, GtidPreference::On) {
        // Forced GTID with empty executed set = full available GTID history
        return Ok((None, None, true, Some(String::new())));
    }

    if let Some(first) = files.first() {
        Ok((Some(first.log_name.clone()), Some(4), false, None))
    } else {
        Ok((Some(cur_file.to_string()), Some(4.min(cur_pos).max(4)), false, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScanArgs;

    fn sample_args() -> ScanArgs {
        ScanArgs {
            host: "127.0.0.1".into(),
            port: 3306,
            user: "root".into(),
            password: "x".into(),
            metadata_user: None,
            metadata_password: None,
            server_id: 300,
            databases: "shop_*".into(),
            tables: "orders".into(),
            field_filters: vec!["status=open".into()],
            since: Some("2026-01-01T00:00:00Z".into()),
            until: Some("2026-01-02T00:00:00Z".into()),
            binlog_start: None,
            output: None,
            limit: Some(10),
            gtid: "off".into(),
        }
    }

    #[test]
    fn filter_includes_matching() {
        let a = sample_args();
        assert!(a.should_include("shop_a", "orders"));
        assert!(!a.should_include("other", "orders"));
        assert!(!a.should_include("shop_a", "items"));
    }

    #[test]
    fn parse_file_pos_ok() {
        let mut a = sample_args();
        a.binlog_start = Some("mysql-bin.000003:4096".into());
        let p = a.parse_file_pos().unwrap().unwrap();
        assert_eq!(p.0, "mysql-bin.000003");
        assert_eq!(p.1, 4096);
    }

    #[test]
    fn parse_datetime_window() {
        let s = parse_datetime("2026-04-17T10:00:00Z").unwrap();
        let u = parse_datetime("2026-04-17T12:00:00Z").unwrap();
        assert!(s < u);
    }
}
