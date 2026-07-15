/// Integration tests for mysql-binlog-monitor.
///
/// Each test spins up the monitor in a background tokio task, executes DML
/// against a dedicated test database, polls the JSON log file for expected
/// events (up to 15 s), then cancels the monitor and asserts the results.
///
/// Prerequisites: docker-compose MySQL reachable at 127.0.0.1:3306
/// (root / rootpassword). Tests are skipped automatically if the database
/// is unreachable. GTID tests additionally require `gtid_mode=ON`.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Short unique suffix so scan tests that re-read full binlog history do not
/// collide with rows from previous test runs (DROP DATABASE does not purge binlog).
fn uniq() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", n % 0xffff_ffff)
}

use mysql_async::{Opts, Pool};
use mysql_binlog_monitor::checkpoint;
use mysql_binlog_monitor::config::{Args, ScanArgs};
use mysql_binlog_monitor::db;
use mysql_binlog_monitor::gtid::{self, ExecutedGtidSet};
use mysql_binlog_monitor::storage::{EventStorage, StoreMode};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

// ── Helpers ────────────────────────────────────────────────────────────────────

const MYSQL_URL: &str = "mysql://root:rootpassword@127.0.0.1:3306/mysql";

/// Returns a pool connected to MySQL, or None if MySQL is unreachable.
async fn try_connect() -> Option<Pool> {
    let opts = Opts::from_url(MYSQL_URL).ok()?;
    let pool = Pool::new(opts);
    // probe with a short timeout
    match tokio::time::timeout(Duration::from_secs(5), pool.get_conn()).await {
        Ok(Ok(_)) => Some(pool),
        _ => {
            eprintln!("SKIP: MySQL not reachable at 127.0.0.1:3306");
            None
        }
    }
}

/// Returns true when the server has GTID mode fully ON.
async fn gtid_mode_on(pool: &Pool) -> bool {
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    let row: Option<mysql_async::Row> =
        match mysql_async::prelude::Queryable::query_first(&mut conn, "SELECT @@GLOBAL.gtid_mode")
            .await
        {
            Ok(r) => r,
            Err(_) => return false,
        };
    let Some(row) = row else {
        return false;
    };
    let mode = row
        .get::<String, _>(0)
        .or_else(|| {
            row.get::<Vec<u8>, _>(0)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .unwrap_or_default();
    mode.eq_ignore_ascii_case("ON")
}

/// Build an `Args` struct for testing: monitors only `db`, writes to `log_path`,
/// uses `server_id` (must be unique per test to avoid replica conflicts).
fn test_args(log_path: &str, db: &str, server_id: u32) -> Args {
    Args {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id,
        log_file: log_path.to_string(),
        log_max_bytes: 0,
        databases: db.to_string(),
        tables: "".into(),
        field_filters: vec![],
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
        checkpoint_path: None,
        // auto: detect server; non-GTID assertions still hold when mode is OFF
        gtid: "auto".into(),
    }
}

/// Create a fresh database and a simple `events` table inside it.
async fn setup_db(pool: &Pool, db: &str) -> anyhow::Result<()> {
    let mut conn = pool.get_conn().await?;
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("DROP DATABASE IF EXISTS `{db}`"),
    )
    .await?;
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("CREATE DATABASE `{db}`"),
    )
    .await?;
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!(
            "CREATE TABLE `{db}`.`events` (
               id    INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
               name  VARCHAR(64)  NOT NULL,
               value INT          NOT NULL DEFAULT 0
             ) ENGINE=InnoDB"
        ),
    )
    .await?;
    Ok(())
}

/// Poll `path` for up to `timeout` collecting lines that parse as JSON objects.
/// Returns once `count` lines have been collected or timeout expires.
async fn collect_events(path: &str, count: usize, timeout: Duration) -> Vec<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut results = Vec::new();
    loop {
        if let Ok(f) = std::fs::File::open(path) {
            let reader = BufReader::new(f);
            results.clear();
            for line in reader.lines().flatten() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    if v.get("operation").is_some() {
                        results.push(v);
                    }
                }
            }
            if results.len() >= count {
                return results;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return results;
        }
        sleep(Duration::from_millis(300)).await;
    }
}

// ── Test 1: INSERT ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    let db = "binlog_test_insert";
    setup_db(&pool, db).await.expect("setup_db failed");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let args = test_args(&log_path, db, 201);

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    // Give the monitor a moment to connect and start streaming
    sleep(Duration::from_secs(3)).await;

    // DML
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('alice', 42)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected at least one INSERT event");
    let ev = &events[0];
    assert_eq!(ev["operation"], "INSERT", "operation should be INSERT");
    assert_eq!(ev["database"], db);
    assert_eq!(ev["table"], "events");
    // primary_key should resolve to 1 (first auto-increment row)
    assert_eq!(ev["primary_key"], 1, "primary_key should be 1");
    let row = &ev["row"]["values"];
    assert_eq!(row["name"], "alice");
    assert_eq!(row["value"], 42);
}

// ── Test 2: UPDATE ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    let db = "binlog_test_update";
    setup_db(&pool, db).await.expect("setup_db failed");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let args = test_args(&log_path, db, 202);

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('bob', 10)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("UPDATE `{db}`.`events` SET value = 99 WHERE name = 'bob'"),
    )
    .await
    .unwrap();

    // Expect 2 events: INSERT + UPDATE
    let events = collect_events(&log_path, 2, Duration::from_secs(15)).await;
    token.cancel();
    monitor.await.ok();

    let update_ev = events
        .iter()
        .find(|e| e["operation"] == "UPDATE")
        .expect("Expected an UPDATE event");

    assert_eq!(update_ev["table"], "events");
    assert_eq!(update_ev["database"], db);
    let before = &update_ev["row"]["before_values"];
    let after = &update_ev["row"]["after_values"];
    assert_eq!(before["value"], 10, "before value should be 10");
    assert_eq!(after["value"], 99, "after value should be 99");
}

// ── Test 3: DELETE ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    let db = "binlog_test_delete";
    setup_db(&pool, db).await.expect("setup_db failed");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let args = test_args(&log_path, db, 203);

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('charlie', 7)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("DELETE FROM `{db}`.`events` WHERE name = 'charlie'"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 2, Duration::from_secs(15)).await;
    token.cancel();
    monitor.await.ok();

    let delete_ev = events
        .iter()
        .find(|e| e["operation"] == "DELETE")
        .expect("Expected a DELETE event");

    assert_eq!(delete_ev["table"], "events");
    assert_eq!(delete_ev["primary_key"], 1, "primary_key should be 1");
    let row = &delete_ev["row"]["values"];
    assert_eq!(row["name"], "charlie");
}

// ── Test 4: Table filter ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    let db = "binlog_test_filter";
    setup_db(&pool, db).await.expect("setup_db failed");

    // Create a second table
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!(
            "CREATE TABLE `{db}`.`ignored` (
               id   INT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
               data VARCHAR(32) NOT NULL
             ) ENGINE=InnoDB"
        ),
    )
    .await
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let mut args = test_args(&log_path, db, 204);
    // Only monitor the `events` table
    args.tables = "events".into();

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`ignored` (data) VALUES ('should_not_appear')"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('dave', 5)"),
    )
    .await
    .unwrap();

    // Wait for 1 event (the `events` INSERT); give extra time so that any
    // stray `ignored` event would also arrive if the filter is broken.
    let _first = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_secs(1)).await; // let any stray events arrive
    let events = collect_events(&log_path, 99, Duration::from_millis(100)).await;

    token.cancel();
    monitor.await.ok();

    assert!(
        !events.is_empty(),
        "Expected at least one event from `events` table"
    );
    for ev in &events {
        assert_ne!(
            ev["table"], "ignored",
            "Filtered table `ignored` should not appear in log"
        );
        assert_eq!(ev["table"], "events", "Only `events` table should appear");
    }
}

// ── Test 5: Wildcard database filter ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wildcard_db_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    // Create two databases: one should match the wildcard, one should not.
    let db_match = "binlog_wc_alpha";
    let db_no    = "other_wc_beta";
    setup_db(&pool, db_match).await.expect("setup_db alpha");
    setup_db(&pool, db_no).await.expect("setup_db beta");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();

    // Monitor only databases matching "binlog_wc_*"
    let args = Args {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 205,
        log_file: log_path.clone(),
        log_max_bytes: 0,
        databases: "binlog_wc_*".into(),
        tables: "".into(),
        field_filters: vec![],
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
        checkpoint_path: None,
        gtid: "auto".into(),
    };

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown).await.ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    // Insert into the non-matching DB first, then the matching one.
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db_no}`.`events` (name, value) VALUES ('should_skip', 0)"),
    ).await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db_match}`.`events` (name, value) VALUES ('should_see', 1)"),
    ).await.unwrap();

    let _first = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_secs(1)).await;
    let events = collect_events(&log_path, 99, Duration::from_millis(100)).await;

    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected at least one event from binlog_wc_alpha");
    for ev in &events {
        // `other_wc_beta` must be filtered out; other `binlog_wc_*` DBs are fine
        assert_ne!(ev["database"], db_no, "`other_wc_beta` should be filtered out by wildcard");
        assert!(
            ev["database"].as_str().unwrap_or("").starts_with("binlog_wc_"),
            "Only binlog_wc_* databases should appear, got: {}",
            ev["database"]
        );
    }
}

// ── Test 6: Wildcard table filter ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wildcard_table_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_wc_tables";
    setup_db(&pool, db).await.expect("setup_db");

    // Add a second table whose name doesn't match the wildcard
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!(
            "CREATE TABLE `{db}`.`audit_log` (
               id   INT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
               info VARCHAR(32) NOT NULL
             ) ENGINE=InnoDB"
        ),
    ).await.unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();

    // Monitor tables matching "event*"
    let args = Args {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 206,
        log_file: log_path.clone(),
        log_max_bytes: 0,
        databases: db.into(),
        tables: "event*".into(),
        field_filters: vec![],
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
        checkpoint_path: None,
        gtid: "auto".into(),
    };

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown).await.ok();
    });

    sleep(Duration::from_secs(3)).await;

    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`audit_log` (info) VALUES ('skip_me')"),
    ).await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('keep_me', 77)"),
    ).await.unwrap();

    let _first = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_secs(1)).await;
    let events = collect_events(&log_path, 99, Duration::from_millis(100)).await;

    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected events from `events` table");
    for ev in &events {
        assert_ne!(ev["table"], "audit_log", "`audit_log` should be filtered out by wildcard");
    }
}

// ── Test 7: GlueSQL storage (id-only + full) ──────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gluesql_storage() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_gluesql_test";
    setup_db(&pool, db).await.expect("setup_db");

    let log_tmp  = tempfile::NamedTempFile::new().unwrap();
    let log_path = log_tmp.path().to_str().unwrap().to_string();
    let db_dir   = tempfile::TempDir::new().unwrap();
    let db_path  = db_dir.path().to_str().unwrap().to_string();

    let mut args = test_args(&log_path, db, 207);
    args.gluesql_path = Some(db_path.clone());
    args.store_mode   = "full".into();

    let token    = CancellationToken::new();
    let shutdown = token.clone();
    let monitor  = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown).await.ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('glue_row', 123)"),
    ).await.unwrap();

    // Wait for the event to land in the log (and GlueSQL)
    collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_millis(500)).await;

    token.cancel();
    monitor.await.ok();

    // ── Now verify via GlueSQL directly ──────────────────────────────────────
    let mut storage = EventStorage::new(&db_path, StoreMode::Full)
        .await
        .expect("open GlueSQL storage for verification");

    // Use the internal Glue handle to SELECT
    let results = storage
        .glue
        .execute("SELECT id, operation, db_name, table_name, primary_key, row_data FROM binlog_events")
        .await
        .expect("SELECT from binlog_events");

    use gluesql::prelude::Payload;
    use gluesql::prelude::Value as GlueValue;
    let mut found = false;
    for payload in results {
        if let Payload::Select { rows, .. } = payload {
            for row in &rows {
                // Check operation column (index 1) == "INSERT"
                let is_insert = matches!(row.get(1), Some(GlueValue::Str(s)) if s == "INSERT");
                if is_insert {
                    // row_data (index 5) should be non-null in full mode
                    let is_null = matches!(row.get(5), Some(GlueValue::Null) | None);
                    assert!(!is_null, "row_data should not be NULL in full mode");
                    found = true;
                }
            }
        }
    }
    assert!(found, "Expected at least one INSERT event in GlueSQL");
}

// ── Test 8: GTID field on events ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gtid_event_field() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON");
        return;
    }

    let db = "binlog_gtid_field";
    setup_db(&pool, db).await.expect("setup_db");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let mut args = test_args(&log_path, db, 210);
    args.gtid = "on".into();

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('gtid_row', 1)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected at least one event");
    let gtid = events[0]["gtid"]
        .as_str()
        .expect("event should include gtid field when --gtid is on");
    assert!(
        gtid::parse_single_gtid(gtid).is_ok(),
        "gtid should parse as uuid:gno, got {gtid}"
    );
}

// ── Test 9: GTID checkpoint persistence ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gtid_checkpoint_written() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON");
        return;
    }

    let db = "binlog_gtid_ckpt";
    setup_db(&pool, db).await.expect("setup_db");

    let log_tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = log_tmp.path().to_str().unwrap().to_string();
    let ckpt_tmp = tempfile::NamedTempFile::new().unwrap();
    let ckpt_path = ckpt_tmp.path().to_str().unwrap().to_string();
    // NamedTempFile creates an empty file; checkpoint load treats empty as invalid
    // so remove it so load returns None on cold start.
    std::fs::remove_file(&ckpt_path).ok();

    let mut args = test_args(&log_path, db, 211);
    args.gtid = "on".into();
    args.checkpoint_path = Some(ckpt_path.clone());

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('ckpt', 42)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    // Allow checkpoint flush after XidEvent
    sleep(Duration::from_millis(800)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected event before checkpoint check");
    let event_gtid = events[0]["gtid"].as_str().expect("gtid on event");

    assert!(
        Path::new(&ckpt_path).exists(),
        "checkpoint file should be created"
    );
    let state = checkpoint::load(Path::new(&ckpt_path))
        .expect("load checkpoint")
        .expect("checkpoint should not be empty");

    let gtid_set = state
        .gtid_set
        .as_deref()
        .expect("checkpoint should contain gtid_set");
    let executed = ExecutedGtidSet::parse(gtid_set).expect("parse gtid_set");
    assert!(
        executed.contains_gtid_str(event_gtid),
        "executed set {gtid_set} should include event gtid {event_gtid}"
    );
    if let Some(last) = &state.last_gtid {
        assert_eq!(last, event_gtid);
    }
    // file:pos should also be present for hybrid resume
    assert!(
        state.position.is_some(),
        "checkpoint should also record file:pos"
    );
}

// ── Test 10: GTID resume does not re-deliver old events ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gtid_resume_no_replay() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON");
        return;
    }

    let db = "binlog_gtid_resume";
    setup_db(&pool, db).await.expect("setup_db");

    let ckpt_dir = tempfile::TempDir::new().unwrap();
    let ckpt_path = ckpt_dir
        .path()
        .join("checkpoint.json")
        .to_str()
        .unwrap()
        .to_string();

    // ── Phase 1: capture first event and write checkpoint ────────────────────
    let log1 = tempfile::NamedTempFile::new().unwrap();
    let log1_path = log1.path().to_str().unwrap().to_string();
    let mut args1 = test_args(&log1_path, db, 212);
    args1.gtid = "on".into();
    args1.checkpoint_path = Some(ckpt_path.clone());

    let token1 = CancellationToken::new();
    let shutdown1 = token1.clone();
    let mon1 = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args1, shutdown1)
            .await
            .ok();
    });
    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('old_row', 1)"),
    )
    .await
    .unwrap();

    let phase1 = collect_events(&log1_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_millis(800)).await;
    token1.cancel();
    mon1.await.ok();

    assert_eq!(phase1.len(), 1, "phase1 should capture one event");
    let old_gtid = phase1[0]["gtid"].as_str().unwrap().to_string();
    assert!(
        Path::new(&ckpt_path).exists(),
        "checkpoint must exist after phase1"
    );

    // ── Phase 2: restart from checkpoint; only new DML should appear ─────────
    let log2 = tempfile::NamedTempFile::new().unwrap();
    let log2_path = log2.path().to_str().unwrap().to_string();
    let mut args2 = test_args(&log2_path, db, 213);
    args2.gtid = "on".into();
    args2.checkpoint_path = Some(ckpt_path.clone());

    let token2 = CancellationToken::new();
    let shutdown2 = token2.clone();
    let mon2 = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args2, shutdown2)
            .await
            .ok();
    });
    sleep(Duration::from_secs(3)).await;

    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('new_row', 2)"),
    )
    .await
    .unwrap();

    let phase2 = collect_events(&log2_path, 1, Duration::from_secs(15)).await;
    // Extra wait so a wrongly-replayed old event would also arrive
    sleep(Duration::from_secs(2)).await;
    let phase2_all = collect_events(&log2_path, 99, Duration::from_millis(200)).await;
    token2.cancel();
    mon2.await.ok();

    assert!(
        !phase2.is_empty(),
        "phase2 should capture the new INSERT event"
    );
    for ev in &phase2_all {
        let name = ev["row"]["values"]["name"].as_str().unwrap_or("");
        assert_ne!(
            name, "old_row",
            "old_row must not be re-delivered after GTID resume"
        );
        if let Some(g) = ev["gtid"].as_str() {
            assert_ne!(
                g, old_gtid,
                "old GTID must not reappear after resume"
            );
        }
    }
    let saw_new = phase2_all.iter().any(|ev| {
        ev["row"]["values"]["name"].as_str() == Some("new_row")
    });
    assert!(saw_new, "phase2 should see new_row");
}

// ── Test 11: GTID + GlueSQL end-to-end with gtid column ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gtid_gluesql_stores_gtid() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON");
        return;
    }

    let db = "binlog_gtid_glue";
    setup_db(&pool, db).await.expect("setup_db");

    let log_tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = log_tmp.path().to_str().unwrap().to_string();
    let db_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().to_str().unwrap().to_string();

    let mut args = test_args(&log_path, db, 214);
    args.gtid = "on".into();
    args.gluesql_path = Some(db_path.clone());
    args.store_mode = "full".into();

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('glue_gtid', 9)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_millis(500)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty());
    let event_gtid = events[0]["gtid"].as_str().expect("gtid on event").to_string();

    let mut storage = EventStorage::new(&db_path, StoreMode::Full)
        .await
        .expect("open storage");
    let results = storage
        .glue
        .execute("SELECT gtid, operation FROM binlog_events")
        .await
        .expect("select");

    use gluesql::prelude::Payload;
    use gluesql::prelude::Value as GlueValue;
    let mut found_gtid = false;
    for payload in results {
        if let Payload::Select { rows, .. } = payload {
            for row in &rows {
                if let Some(GlueValue::Str(g)) = row.first() {
                    assert_eq!(g, &event_gtid);
                    found_gtid = true;
                }
            }
        }
    }
    assert!(found_gtid, "GlueSQL row should store gtid");

    // Dedupe: re-inserting the same event must be a no-op
    let reinsert = events[0].clone();
    let inserted = storage.insert(&reinsert).await.expect("reinsert");
    assert!(!inserted, "duplicate GTID must be skipped by GlueSQL sink");
}

// ── Test 12: --gtid auto attaches gtid when server supports it ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gtid_auto_detect() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON (cannot verify auto-detect path)");
        return;
    }

    let db = "binlog_gtid_auto";
    setup_db(&pool, db).await.expect("setup_db");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let mut args = test_args(&log_path, db, 215);
    // Explicit auto (also the default)
    args.gtid = "auto".into();

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });

    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('auto_row', 1)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "Expected event under --gtid auto");
    let gtid = events[0]["gtid"]
        .as_str()
        .expect("--gtid auto should attach gtid when server gtid_mode=ON");
    assert!(gtid::parse_single_gtid(gtid).is_ok(), "bad gtid: {gtid}");
}

// ── Test 13: historical scan (one-shot, no continuous follow) ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scan_history_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_scan_hist";
    setup_db(&pool, db).await.expect("setup_db");

    // Seed history before scan
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('scan_a', 1)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('scan_b', 2)"),
    )
    .await
    .unwrap();
    // Noise table should be filtered out
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!(
            "CREATE TABLE `{db}`.`noise` (
               id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
               x  VARCHAR(16) NOT NULL
             ) ENGINE=InnoDB"
        ),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`noise` (x) VALUES ('nope')"),
    )
    .await
    .unwrap();

    let out = tempfile::NamedTempFile::new().unwrap();
    let out_path = out.path().to_str().unwrap().to_string();

    let args = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 320,
        databases: db.into(),
        tables: "events".into(),
        field_filters: vec![],
        since: None,
        until: None,
        binlog_start: None,
        output: Some(out_path.clone()),
        limit: Some(100),
        gtid: "off".into(),
    };

    mysql_binlog_monitor::scan::run_scan(args)
        .await
        .expect("scan should succeed");

    let body = std::fs::read_to_string(&out_path).expect("read scan output");
    let mut events = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).expect("json line");
        events.push(v);
    }

    assert!(
        events.len() >= 2,
        "scan should emit at least the two seed INSERTs, got {}",
        events.len()
    );
    for ev in &events {
        assert_eq!(ev["database"], db);
        assert_eq!(ev["table"], "events");
        assert_ne!(ev["table"], "noise");
    }
    let names: Vec<_> = events
        .iter()
        .filter_map(|e| e["row"]["values"]["name"].as_str())
        .collect();
    assert!(names.contains(&"scan_a"), "missing scan_a: {names:?}");
    assert!(names.contains(&"scan_b"), "missing scan_b: {names:?}");
}

// ── Test 14: scan with --field-filter ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scan_field_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_scan_ffield";
    setup_db(&pool, db).await.expect("setup_db");

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('keep_me', 1)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('drop_me', 2)"),
    )
    .await
    .unwrap();

    let out = tempfile::NamedTempFile::new().unwrap();
    let out_path = out.path().to_str().unwrap().to_string();

    let args = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 321,
        databases: db.into(),
        tables: "events".into(),
        field_filters: vec!["name=keep_me".into()],
        since: None,
        until: None,
        binlog_start: None,
        output: Some(out_path.clone()),
        limit: Some(50),
        gtid: "off".into(),
    };

    mysql_binlog_monitor::scan::run_scan(args)
        .await
        .expect("scan");

    let body = std::fs::read_to_string(&out_path).unwrap();
    let mut events = Vec::new();
    for line in body.lines().filter(|l| !l.trim().is_empty()) {
        events.push(serde_json::from_str::<serde_json::Value>(line).unwrap());
    }
    assert!(!events.is_empty(), "expected keep_me event");
    for ev in &events {
        assert_eq!(ev["row"]["values"]["name"], "keep_me");
    }
}

// ── Test 15: monitor --field-filter ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_monitor_field_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_mon_ffield";
    setup_db(&pool, db).await.expect("setup_db");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = tmp.path().to_str().unwrap().to_string();
    let mut args = test_args(&log_path, db, 330);
    args.field_filters = vec!["name=keep_mon".into()];
    args.gtid = "off".into();

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });
    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('drop_mon', 1)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('keep_mon', 2)"),
    )
    .await
    .unwrap();

    let events = collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_secs(1)).await;
    let all = collect_events(&log_path, 99, Duration::from_millis(200)).await;
    token.cancel();
    monitor.await.ok();

    assert!(!events.is_empty(), "expected keep_mon event");
    for ev in &all {
        assert_eq!(
            ev["row"]["values"]["name"], "keep_mon",
            "field-filter should drop other names, got {}",
            ev
        );
    }
}

// ── Test 16: scan --since / --until time window ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scan_time_window() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let tag = format!("in_win_{}", uniq());
    let db = format!("bs_t_{}", uniq());
    setup_db(&pool, &db).await.expect("setup_db");

    // Snapshot master position, then insert — scan only from that pos (fast, no full history).
    let (bin_file, bin_pos) = db::fetch_master_status(&pool).await.expect("master status");
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('{tag}', 1)"),
    )
    .await
    .unwrap();

    let out_ok = tempfile::NamedTempFile::new().unwrap();
    let path_ok = out_ok.path().to_str().unwrap().to_string();
    let args_ok = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 331,
        databases: db.clone(),
        tables: "events".into(),
        field_filters: vec![format!("name={tag}")],
        // Wide window still applies as a filter on event timestamps.
        since: Some("2000-01-01T00:00:00Z".into()),
        until: Some("2099-01-01T00:00:00Z".into()),
        binlog_start: Some(format!("{bin_file}:{bin_pos}")),
        output: Some(path_ok.clone()),
        limit: Some(100),
        gtid: "off".into(),
    };
    mysql_binlog_monitor::scan::run_scan(args_ok)
        .await
        .expect("scan wide window");

    let body = std::fs::read_to_string(&path_ok).unwrap();
    let wide: Vec<_> = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect();
    assert!(
        wide.iter()
            .any(|e| e["row"]["values"]["name"].as_str() == Some(tag.as_str())),
        "wide time window should include {tag}, got {wide:?}"
    );

    let out_empty = tempfile::NamedTempFile::new().unwrap();
    let path_empty = out_empty.path().to_str().unwrap().to_string();
    let args_empty = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 332,
        databases: db.clone(),
        tables: "events".into(),
        field_filters: vec![format!("name={tag}")],
        since: Some("2090-01-01T00:00:00Z".into()),
        until: Some("2099-01-01T00:00:00Z".into()),
        binlog_start: Some(format!("{bin_file}:{bin_pos}")),
        output: Some(path_empty.clone()),
        limit: Some(100),
        gtid: "off".into(),
    };
    mysql_binlog_monitor::scan::run_scan(args_empty)
        .await
        .expect("scan future window");

    let body_empty = std::fs::read_to_string(&path_empty).unwrap();
    let future_events: Vec<_> = body_empty
        .lines()
        .filter(|l| !l.trim().is_empty())
        .collect();
    assert!(
        future_events.is_empty(),
        "future --since/--until should emit no past events for {tag}, got {}",
        body_empty
    );
}

// ── Test 17: scan --limit ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scan_limit() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_scan_limit";
    setup_db(&pool, db).await.expect("setup_db");

    let mut conn = pool.get_conn().await.unwrap();
    for i in 0..5 {
        mysql_async::prelude::Queryable::query_drop(
            &mut conn,
            format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('row_{i}', {i})"),
        )
        .await
        .unwrap();
    }

    let out = tempfile::NamedTempFile::new().unwrap();
    let out_path = out.path().to_str().unwrap().to_string();
    let args = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 333,
        databases: db.into(),
        tables: "events".into(),
        field_filters: vec![],
        since: None,
        until: None,
        binlog_start: None,
        output: Some(out_path.clone()),
        limit: Some(2),
        gtid: "off".into(),
    };
    mysql_binlog_monitor::scan::run_scan(args)
        .await
        .expect("scan limit");

    let n = std::fs::read_to_string(&out_path)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(n, 2, "scan --limit 2 should emit exactly 2 events, got {n}");
}

// ── Test 18: binlog-info lists current file ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_binlog_info() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    let _ = pool; // connectivity probe only

    let args = mysql_binlog_monitor::config::BinlogInfoArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        format: "json".into(),
        server_id: 340,
        since: None,
        until: None,
    };

    // Capture stdout is hard; instead call underlying DB helpers the command uses.
    let (file, pos) = db::fetch_master_status(&try_connect().await.unwrap())
        .await
        .expect("master status");
    assert!(!file.is_empty(), "binlog file name");
    assert!(pos >= 4, "binlog pos should be >= 4");

    let files = db::fetch_binary_logs(&try_connect().await.unwrap())
        .await
        .expect("binary logs");
    assert!(!files.is_empty(), "SHOW BINARY LOGS should list files");
    assert!(
        files.iter().any(|f| f.log_name == file),
        "current file should appear in binary logs list"
    );

    // Full subcommand should succeed (prints to stdout)
    mysql_binlog_monitor::binlog_info::run_binlog_info(args)
        .await
        .expect("binlog-info");
}

// ── Test 19: file-position checkpoint without forcing GTID ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_pos_checkpoint() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    let db = "binlog_file_ckpt";
    setup_db(&pool, db).await.expect("setup_db");

    let log_tmp = tempfile::NamedTempFile::new().unwrap();
    let log_path = log_tmp.path().to_str().unwrap().to_string();
    let ckpt_dir = tempfile::TempDir::new().unwrap();
    let ckpt_path = ckpt_dir
        .path()
        .join("cp.json")
        .to_str()
        .unwrap()
        .to_string();

    let mut args = test_args(&log_path, db, 341);
    args.gtid = "off".into();
    args.checkpoint_path = Some(ckpt_path.clone());

    let token = CancellationToken::new();
    let shutdown = token.clone();
    let monitor = tokio::spawn(async move {
        mysql_binlog_monitor::monitor::run_monitor(args, shutdown)
            .await
            .ok();
    });
    sleep(Duration::from_secs(3)).await;

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('ckpt_row', 1)"),
    )
    .await
    .unwrap();

    collect_events(&log_path, 1, Duration::from_secs(15)).await;
    sleep(Duration::from_millis(800)).await;
    token.cancel();
    monitor.await.ok();

    let state = checkpoint::load(Path::new(&ckpt_path))
        .expect("load")
        .expect("checkpoint exists");
    let pos = state.position.expect("file:pos present when gtid=off");
    assert!(!pos.file.is_empty());
    assert!(pos.pos >= 4);
}

// ── Test 20: scan multi field-filter AND + value number ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scan_multi_field_filter() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };

    // Unique name so historical binlog rows from prior runs cannot inflate the count.
    let tag = format!("and_{}", uniq());
    let db = format!("bs_and_{}", uniq());
    setup_db(&pool, &db).await.expect("setup_db");

    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('{tag}', 10)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('{tag}', 99)"),
    )
    .await
    .unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!("INSERT INTO `{db}`.`events` (name, value) VALUES ('other_{tag}', 10)"),
    )
    .await
    .unwrap();

    let out = tempfile::NamedTempFile::new().unwrap();
    let out_path = out.path().to_str().unwrap().to_string();
    let args = ScanArgs {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "root".into(),
        password: "rootpassword".into(),
        metadata_user: None,
        metadata_password: None,
        server_id: 342,
        databases: db.clone(),
        tables: "events".into(),
        field_filters: vec![format!("name={tag}"), "value=10".into()],
        since: None,
        until: None,
        binlog_start: None,
        output: Some(out_path.clone()),
        limit: Some(50),
        gtid: "off".into(),
    };
    mysql_binlog_monitor::scan::run_scan(args)
        .await
        .expect("scan and-filters");

    let events: Vec<serde_json::Value> = std::fs::read_to_string(&out_path)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        events.len(),
        1,
        "only name={tag} AND value=10, got {events:?}"
    );
    assert_eq!(events[0]["row"]["values"]["name"].as_str(), Some(tag.as_str()));
    assert_eq!(events[0]["row"]["values"]["value"], 10);
}

// ── Test 21: fetch_gtid_executed readable when GTID on ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fetch_gtid_executed() {
    let pool = match try_connect().await {
        Some(p) => p,
        None => return,
    };
    if !gtid_mode_on(&pool).await {
        eprintln!("SKIP: gtid_mode is not ON");
        return;
    }

    // Generate at least one GTID
    let mut conn = pool.get_conn().await.unwrap();
    mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        "CREATE DATABASE IF NOT EXISTS binlog_gtid_probe",
    )
    .await
    .ok();

    let executed = db::fetch_gtid_executed(&pool)
        .await
        .expect("fetch_gtid_executed");
    // After any DML/DDL with gtid_mode=ON, set is usually non-empty; empty is still valid
    // on a brand-new empty server. At minimum the call must succeed and parse.
    if !executed.is_empty() {
        ExecutedGtidSet::parse(&executed)
            .unwrap_or_else(|e| panic!("gtid_executed '{executed}' should parse: {e}"));
    }
}
