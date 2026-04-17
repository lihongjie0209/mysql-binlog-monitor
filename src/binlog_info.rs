use anyhow::Result;
use chrono::Utc;
use mysql_async::{Opts, OptsBuilder, Pool};
use serde_json::json;

use crate::config::BinlogInfoArgs;
use crate::db::{fetch_binary_logs, fetch_master_status, BinlogFile};
use crate::time_seek::{first_event_time, parse_datetime};

pub async fn run_binlog_info(args: BinlogInfoArgs) -> Result<()> {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname(args.host.clone())
        .tcp_port(args.port)
        .user(Some(args.user.clone()))
        .pass(Some(args.password.clone()))
        .into();
    let pool = Pool::new(opts);

    let files           = fetch_binary_logs(&pool).await?;
    let (cur_file, cur_pos) = fetch_master_status(&pool).await?;

    // Always fetch first-event timestamp for every file.
    let mut start_times: Vec<Option<u64>> = Vec::with_capacity(files.len());
    for f in &files {
        let ts = first_event_time(&pool, args.server_id, &f.log_name).await?;
        start_times.push(ts);
    }

    // Derive end time for each file:
    //   non-current file  → start time of the NEXT file (close approximation)
    //   current file      → now (UTC)
    let now_ts = Utc::now().timestamp() as u64;
    let end_times: Vec<Option<u64>> = (0..files.len())
        .map(|i| {
            if files[i].log_name == cur_file {
                Some(now_ts)
            } else {
                start_times.get(i + 1).copied().flatten()
            }
        })
        .collect();

    // Parse --since / --until filter timestamps
    let since_ts = args.since.as_deref().map(parse_datetime).transpose()?;
    let until_ts = args.until.as_deref().map(parse_datetime).transpose()?;

    // Keep files that overlap [since_ts, until_ts].
    // A file [start, end] overlaps [since, until] when start ≤ until AND end ≥ since.
    let entries: Vec<(&BinlogFile, Option<u64>, Option<u64>)> = files
        .iter()
        .zip(start_times.iter())
        .zip(end_times.iter())
        .filter(|((_, start), end)| {
            let s = start.unwrap_or(0);
            let e = end.unwrap_or(u64::MAX);
            let after  = since_ts.map(|si| e >= si).unwrap_or(true);
            let before = until_ts.map(|un| s <= un).unwrap_or(true);
            after && before
        })
        .map(|((f, start), end)| (f, *start, *end))
        .collect();

    match args.format.as_str() {
        "json" => print_json(&entries, &cur_file, cur_pos),
        _      => print_table(&entries, &cur_file, cur_pos),
    }

    pool.disconnect().await?;
    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn fmt_ts(ts: u64) -> String {
    chrono::DateTime::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn fmt_opt(ts: Option<u64>) -> String {
    ts.map(fmt_ts).unwrap_or_else(|| "-".into())
}

// ── Formatters ─────────────────────────────────────────────────────────────────

fn print_table(
    entries: &[(&BinlogFile, Option<u64>, Option<u64>)],
    cur_file: &str,
    cur_pos: u64,
) {
    let total: u64 = entries.iter().map(|(f, _, _)| f.file_size).sum();

    println!(
        "{:<30}  {:>14}  {:<22}  {:<22}  {}",
        "File", "Size (bytes)", "Start (UTC)", "End (UTC)", "Note"
    );
    println!("{}", "-".repeat(115));

    for (f, start, end) in entries {
        let note = if f.log_name == cur_file {
            format!("← current pos {}", cur_pos)
        } else {
            String::new()
        };
        println!(
            "{:<30}  {:>14}  {:<22}  {:<22}  {}",
            f.log_name,
            f.file_size,
            fmt_opt(*start),
            fmt_opt(*end),
            note,
        );
    }

    println!("{}", "-".repeat(115));
    println!("{:<30}  {:>14}", format!("{} file(s)", entries.len()), total);
    println!();
    println!("Current write position: {}:{}", cur_file, cur_pos);
    println!("Use with --binlog-start to replay from a specific file, e.g.:");
    println!("  --binlog-start {}:{}", cur_file, cur_pos);
}

fn print_json(
    entries: &[(&BinlogFile, Option<u64>, Option<u64>)],
    cur_file: &str,
    cur_pos: u64,
) {
    let arr: Vec<_> = entries
        .iter()
        .map(|(f, start, end)| {
            json!({
                "log_name":        f.log_name,
                "file_size":       f.file_size,
                "start_time":      start.map(fmt_ts),
                "end_time":        end.map(fmt_ts),
                "is_current":      f.log_name == cur_file,
            })
        })
        .collect();

    let out = json!({
        "binlog_files": arr,
        "current": {
            "file": cur_file,
            "pos":  cur_pos,
            "binlog_start": format!("{}:{}", cur_file, cur_pos),
        },
    });

    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}

