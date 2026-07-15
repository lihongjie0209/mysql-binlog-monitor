use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::{BinlogStreamRequest, Opts, Pool};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use serde_json::json;

use crate::checkpoint::{self, BinlogPosition, CheckpointState, StartKind};
use crate::config::Args;
use crate::db::{ColMap, PkMap};
use crate::ddl::is_schema_changing_ddl;
use crate::event::{binlog_row_to_json, build_change_event, extract_pk};
use crate::field_filter::{self, FieldPredicate};
use crate::gtid::{format_gtid, ExecutedGtidSet, GtidPreference};
use crate::logger::Logger;
use crate::storage::{EventStorage, StoreMode};

/// Update in-memory file:pos and optionally flush the full checkpoint (incl. GTID set).
fn note_position(
    runtime_pos: &mut Option<BinlogPosition>,
    current_file: &Option<String>,
    log_pos: u64,
    executed: &ExecutedGtidSet,
    last_gtid: Option<&str>,
    checkpoint_path: Option<&str>,
    logger: &Logger,
) {
    if log_pos > 0 {
        if let Some(file) = current_file.as_ref() {
            *runtime_pos = Some(BinlogPosition::new(file.clone(), log_pos));
        }
    }
    persist_checkpoint(checkpoint_path, runtime_pos, executed, last_gtid, logger);
}

fn persist_checkpoint(
    checkpoint_path: Option<&str>,
    runtime_pos: &Option<BinlogPosition>,
    executed: &ExecutedGtidSet,
    last_gtid: Option<&str>,
    logger: &Logger,
) {
    let Some(path) = checkpoint_path else {
        return;
    };
    let state = CheckpointState {
        position: runtime_pos.clone(),
        gtid_set: if executed.is_empty() {
            None
        } else {
            Some(executed.to_mysql_string())
        },
        last_gtid: last_gtid.map(|s| s.to_string()),
    };
    if state.position.is_none() && state.gtid_set.is_none() {
        return;
    }
    if let Err(e) = checkpoint::save(Path::new(path), &state) {
        logger.warn(json!({
            "message": "Failed to write checkpoint",
            "path": path,
            "error": e.to_string(),
        }));
    }
}

// ── Monitor ────────────────────────────────────────────────────────────────────

pub async fn run_monitor(args: Args, shutdown: CancellationToken) -> Result<()> {
    let logger = Logger::with_max_bytes(&args.log_file, &args.log_level, args.log_max_bytes)
        .context("Failed to open log file")?;

    let field_preds: Vec<FieldPredicate> = args
        .field_predicates()
        .map_err(|e| anyhow::anyhow!(e))?;
    if !field_preds.is_empty() {
        logger.info(json!({
            "message": "Field filters active",
            "filters": args.field_filters,
        }));
    }

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
    // On connection drop the inner loop breaks; we then wait before reconnecting
    // (1 s → 2 s → 4 s … capped at 60 s). A successful connect resets delay to 1 s.
    //
    // runtime_pos is updated on every event and preferred on reconnect so we do
    // not re-seek from the original --since / --binlog-start (avoids duplicates).
    let opts = Opts::from_url(&stream_url).context("Invalid MySQL URL")?;
    let mut backoff = Duration::from_secs(1);
    let mut runtime_pos: Option<BinlogPosition> = None;
    let mut current_file: Option<String> = None;
    let mut executed = ExecutedGtidSet::new();
    let mut current_gtid: Option<String> = None;
    let mut last_gtid: Option<String> = None;

    // Cold-start disk checkpoint.
    let disk_checkpoint: Option<CheckpointState> = match &args.checkpoint_path {
        Some(path) => match checkpoint::load(Path::new(path)) {
            Ok(Some(st)) => {
                logger.info(json!({
                    "message": "Loaded checkpoint from disk",
                    "path": path,
                    "file": st.position.as_ref().map(|p| p.file.clone()),
                    "pos": st.position.as_ref().map(|p| p.pos),
                    "gtid_set": st.gtid_set,
                    "last_gtid": st.last_gtid,
                }));
                if let Ok(set) = st.executed_set() {
                    executed = set;
                }
                last_gtid = st.last_gtid.clone();
                Some(st)
            }
            Ok(None) => {
                logger.info(json!({ "message": "No checkpoint file yet", "path": path }));
                None
            }
            Err(e) => {
                logger.warn(json!({
                    "message": "Failed to load checkpoint, ignoring",
                    "path": path,
                    "error": e.to_string(),
                }));
                None
            }
        },
        None => None,
    };

    let binlog_start = args.parse_binlog_start().unwrap_or_else(|e| {
        logger.warn(json!({ "message": "Bad --binlog-start value, defaulting to 'end'", "error": e }));
        crate::config::BinlogStart::End
    });

    // ── Resolve GTID preference (auto-detect server when needed) ─────────────
    let gtid_pref = args.gtid_preference();
    let server_gtid_on = match gtid_pref {
        GtidPreference::Off => false,
        GtidPreference::Auto | GtidPreference::On => {
            match crate::db::is_gtid_mode_on(&meta_pool).await {
                Ok(on) => on,
                Err(e) => {
                    logger.warn(json!({
                        "message": "Could not read @@gtid_mode; treating as off",
                        "error": e.to_string(),
                    }));
                    false
                }
            }
        }
    };
    let gtid_enabled = gtid_pref.resolve(server_gtid_on);
    if gtid_pref.forced_but_unavailable(server_gtid_on) {
        logger.warn(json!({
            "message": "--gtid on requested but server gtid_mode is not ON; falling back to file:pos",
            "preference": args.gtid,
            "server_gtid_on": server_gtid_on,
        }));
    } else {
        logger.info(json!({
            "message": "GTID streaming decision",
            "preference": args.gtid,
            "server_gtid_on": server_gtid_on,
            "enabled": gtid_enabled,
        }));
    }

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

        // ── Determine starting binlog position ───────────────────────────────
        let runtime_gtid_str = if executed.is_empty() {
            None
        } else {
            Some(executed.to_mysql_string())
        };
        let start_kind = checkpoint::decide_start(
            runtime_pos.as_ref(),
            args.since.is_some(),
            &binlog_start,
            disk_checkpoint.as_ref(),
            gtid_enabled,
            runtime_gtid_str.as_deref(),
        );

        // When Some((file,pos)) use classic dump; when Gtid mode, request is built separately.
        let mut use_gtid_request: Option<String> = None;

        let master_status: Option<(String, u64)> = match start_kind {
            StartKind::Position(p) => {
                logger.info(json!({
                    "message": "Starting binlog stream from position",
                    "file": p.file,
                    "pos": p.pos,
                    "source": if runtime_pos.is_some() { "runtime_resume" } else { "explicit_or_disk" },
                }));
                current_file = Some(p.file.clone());
                Some((p.file, p.pos))
            }
            StartKind::Gtid { executed: set_str } => {
                let set_str = if set_str.is_empty() {
                    // Live GTID start: only stream transactions after current gtid_executed.
                    match crate::db::fetch_gtid_executed(&meta_pool).await {
                        Ok(s) => {
                            if let Ok(server_set) = ExecutedGtidSet::parse(&s) {
                                // Seed local executed set so we don't re-process on next restart
                                // if no events arrive before exit.
                                if executed.is_empty() {
                                    executed = server_set;
                                }
                            }
                            s
                        }
                        Err(e) => {
                            logger.warn(json!({
                                "message": "Could not read @@gtid_executed; using empty set (full history)",
                                "error": e.to_string(),
                            }));
                            String::new()
                        }
                    }
                } else {
                    set_str
                };
                logger.info(json!({
                    "message": "Starting binlog stream with GTID auto-position",
                    "gtid_executed": set_str,
                }));
                use_gtid_request = Some(set_str);
                None
            }
            StartKind::FileBegin => {
                logger.info(json!({ "message": "Starting binlog stream from beginning of current binlog file" }));
                None
            }
            StartKind::LiveEnd => {
                match crate::db::fetch_master_status(&meta_pool).await {
                    Ok(status) => {
                        logger.info(json!({
                            "message": "Starting binlog stream from current position",
                            "file": status.0,
                            "pos": status.1,
                        }));
                        current_file = Some(status.0.clone());
                        Some(status)
                    }
                    Err(e) => {
                        logger.warn(json!({
                            "message": "Could not fetch master status; starting from beginning of current file",
                            "error": e.to_string(),
                        }));
                        None
                    }
                }
            }
            StartKind::TimeSeek => {
                let since_str = args.since.as_deref().unwrap_or("");
                match crate::time_seek::parse_datetime(since_str) {
                    Err(e) => {
                        logger.warn(json!({ "message": "Bad --since value, falling back to 'end'", "error": e.to_string() }));
                        match crate::db::fetch_master_status(&meta_pool).await {
                            Ok(s) => {
                                current_file = Some(s.0.clone());
                                Some(s)
                            }
                            Err(_) => None,
                        }
                    }
                    Ok(target_ts) => {
                        logger.info(json!({
                            "message": "Seeking binlog position by time",
                            "since": since_str,
                            "target_unix": target_ts,
                        }));
                        match crate::db::fetch_binary_logs(&meta_pool).await {
                            Err(e) => {
                                logger.warn(json!({ "message": "Could not list binary logs", "error": e.to_string() }));
                                None
                            }
                            Ok(files) => {
                                let (cur_file, cur_pos) = crate::db::fetch_master_status(&meta_pool)
                                    .await
                                    .unwrap_or_default();
                                match crate::time_seek::find_pos_by_time(
                                    &meta_pool, args.server_id, &files, target_ts, &cur_file, cur_pos,
                                ).await {
                                    Err(e) => {
                                        logger.warn(json!({ "message": "Time seek failed, using current pos", "error": e.to_string() }));
                                        current_file = Some(cur_file.clone());
                                        Some((cur_file, cur_pos))
                                    }
                                    Ok((file, pos)) => {
                                        logger.info(json!({
                                            "message": "Binlog position found by time seek",
                                            "file": file,
                                            "pos": pos,
                                        }));
                                        current_file = Some(file.clone());
                                        Some((file, pos))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // Build stream request. GTID mode needs owned Sids for the request lifetime.
        let gtid_sids_owned: Option<Vec<mysql_async::Sid<'static>>> =
            if let Some(ref set_str) = use_gtid_request {
                match ExecutedGtidSet::parse(set_str).and_then(|s| s.to_sids()) {
                    Ok(sids) => Some(sids),
                    Err(e) => {
                        logger.warn(json!({
                            "message": "Failed to build GTID set for stream request",
                            "error": e.to_string(),
                        }));
                        Some(Vec::new())
                    }
                }
            } else {
                None
            };

        let request = if let Some(ref sids) = gtid_sids_owned {
            BinlogStreamRequest::new(args.server_id)
                .with_gtid()
                .with_gtid_set(sids.clone())
        } else {
            match &master_status {
                Some((file, pos)) => BinlogStreamRequest::new(args.server_id)
                    .with_filename(file.as_bytes())
                    .with_pos(*pos),
                None => BinlogStreamRequest::new(args.server_id),
            }
        };
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
                let log_pos = event.header().log_pos() as u64;
                let event_time = Utc
                    .timestamp_opt(ts_unix as i64, 0)
                    .single()
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default();

                // Log every raw event at trace level for deep debugging
                logger.debug(json!({
                    "message":    "Raw binlog event",
                    "event_type": format!("{:?}", event.header().event_type()),
                    "timestamp":  event.header().timestamp(),
                    "log_pos":    log_pos,
                }));

                // Parse event data — EventData borrows from `event`
                let data = match event.read_data() {
                    Ok(Some(d)) => d,
                    Ok(None) => {
                        logger.debug(json!({ "message": "Binlog event has no data, skipping" }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    Err(e) => {
                        logger.debug(json!({ "message": "Failed to read binlog event data", "error": e.to_string() }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                };

                let re = match data {
                    EventData::RowsEvent(re) => re,
                    EventData::GtidEvent(ref ge) => {
                        let g = format_gtid(&ge.sid(), ge.gno());
                        current_gtid = Some(g.clone());
                        logger.debug(json!({ "message": "GtidEvent", "gtid": g }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    EventData::AnonymousGtidEvent(ref age) => {
                        // MySQL may emit ANONYMOUS_GTID_LOG_EVENT; still carries sid/gno.
                        let ge = &age.0;
                        let g = format_gtid(&ge.sid(), ge.gno());
                        // gno==0 means truly anonymous — keep as synthetic tag for correlation only
                        if ge.gno() > 0 {
                            current_gtid = Some(g.clone());
                        } else {
                            current_gtid = Some(format!("anonymous:{}", g));
                        }
                        logger.debug(json!({ "message": "AnonymousGtidEvent", "gtid": g }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    EventData::XidEvent(_) => {
                        // Transaction commit — mark current GTID as executed (exactly-once resume).
                        if let Some(g) = current_gtid.take() {
                            if let Err(e) = executed.add_gtid_str(&g) {
                                logger.warn(json!({
                                    "message": "Failed to record executed GTID",
                                    "gtid": g,
                                    "error": e.to_string(),
                                }));
                            } else {
                                last_gtid = Some(g.clone());
                                logger.debug(json!({
                                    "message": "Committed GTID",
                                    "gtid": g,
                                    "gtid_set": executed.to_mysql_string(),
                                }));
                            }
                        }
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    EventData::QueryEvent(ref qe) => {
                        let sql = qe.query();
                        if is_schema_changing_ddl(&sql) {
                            // Table layout may have changed — drop cached column/PK maps.
                            // Next row event re-fetches metadata lazily per table.
                            let cleared = col_map.len() + pk_map.len();
                            col_map.clear();
                            pk_map.clear();
                            logger.info(json!({
                                "message": "Schema-changing DDL detected; metadata cache cleared",
                                "schema": qe.schema(),
                                "query": sql.chars().take(200).collect::<String>(),
                                "entries_cleared": cleared,
                            }));
                        } else {
                            logger.debug(json!({
                                "message": "QueryEvent (non-DDL)",
                                "schema": qe.schema(),
                                "query": sql.chars().take(120).collect::<String>(),
                            }));
                        }
                        // Only DDL auto-commit ends a GTID group here without XidEvent.
                        // Do NOT clear current_gtid on BEGIN/COMMIT QueryEvents — ROW
                        // transactions still need it for the following RowsEvent.
                        if is_schema_changing_ddl(&sql) {
                            if let Some(g) = current_gtid.take() {
                                let _ = executed.add_gtid_str(&g);
                                last_gtid = Some(g);
                            }
                        }
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    EventData::RotateEvent(ref re) => {
                        // Real + fake rotate both carry the next (or current) file name.
                        let file = re.name().into_owned();
                        let pos = if re.position() > 0 { re.position() } else { 4 };
                        current_file = Some(file.clone());
                        logger.debug(json!({
                            "message": "RotateEvent",
                            "file": file,
                            "pos": pos,
                            "fake": re.is_fake(),
                        }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    EventData::TableMapEvent(ref tme_data) => {
                        logger.debug(json!({
                            "message":  "TableMapEvent received",
                            "table_id": tme_data.table_id(),
                            "database": tme_data.database_name(),
                            "table":    tme_data.table_name(),
                        }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
                    other => {
                        logger.debug(json!({ "message": "Non-rows event, skipping", "event_type": format!("{other:?}").split('(').next().unwrap_or("unknown") }));
                        note_position(
                            &mut runtime_pos,
                            &current_file,
                            log_pos,
                            &executed,
                            last_gtid.as_deref(),
                            args.checkpoint_path.as_deref(),
                            &logger,
                        );
                        continue;
                    }
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
                    None => {
                        logger.debug(json!({ "message": "No TableMapEvent for table_id, skipping", "table_id": table_id }));
                        continue;
                    }
                };

                let database = tme.database_name().to_string();
                let table    = tme.table_name().to_string();

                logger.debug(json!({
                    "message":   "Binlog rows event received",
                    "operation": operation,
                    "database":  database,
                    "table":     table,
                }));

                if !args.should_include(&database, &table) {
                    logger.debug(json!({
                        "message":          "Event filtered out",
                        "database":         database,
                        "table":            table,
                        "filter_databases": args.filter_databases(),
                        "filter_tables":    args.filter_tables(),
                    }));
                    // Advance checkpoint even for filtered events so reconnect skips them.
                    note_position(
                        &mut runtime_pos,
                        &current_file,
                        log_pos,
                        &executed,
                        last_gtid.as_deref(),
                        args.checkpoint_path.as_deref(),
                        &logger,
                    );
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

                    if !field_filter::row_matches_fields(&row_value, &field_preds) {
                        logger.debug(json!({
                            "message": "Event filtered by field-filter",
                            "database": database,
                            "table": table,
                            "filters": args.field_filters,
                        }));
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

                    logger.info(event_json.clone());

                    if let Some(storage) = event_storage.as_mut() {
                        match storage.insert(&event_json).await {
                            Ok(false) => logger.debug(json!({
                                "message": "Skipped duplicate GTID event (exactly-once sink)",
                                "gtid": current_gtid,
                            })),
                            Ok(true) => {}
                            Err(e) => logger.warn(json!({
                                "message": "GlueSQL insert failed",
                                "error": e.to_string()
                            })),
                        }
                    }
                }

                // Advance after the rows event is fully processed.
                note_position(
                    &mut runtime_pos,
                    &current_file,
                    log_pos,
                    &executed,
                    last_gtid.as_deref(),
                    args.checkpoint_path.as_deref(),
                    &logger,
                );
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
