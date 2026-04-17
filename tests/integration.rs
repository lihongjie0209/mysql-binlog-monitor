/// Integration tests for mysql-binlog-monitor.
///
/// Each test spins up the monitor in a background tokio task, executes DML
/// against a dedicated test database, polls the JSON log file for expected
/// events (up to 15 s), then cancels the monitor and asserts the results.
///
/// Prerequisites: docker-compose MySQL reachable at 127.0.0.1:3306
/// (root / rootpassword). Tests are skipped automatically if the database
/// is unreachable.

use std::io::{BufRead, BufReader};
use std::time::Duration;

use mysql_async::{Opts, Pool};
use mysql_binlog_monitor::config::Args;
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
        databases: db.to_string(),
        tables: "".into(),
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
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
        databases: "binlog_wc_*".into(),
        tables: "".into(),
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
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
        databases: db.into(),
        tables: "event*".into(),
        log_level: "info".into(),
        gluesql_path: None,
        store_mode: "id-only".into(),
        binlog_start: "end".into(),
        since: None,
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
