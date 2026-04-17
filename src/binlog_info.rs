use anyhow::Result;
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

    let files  = fetch_binary_logs(&pool).await?;
    let (cur_file, cur_pos) = fetch_master_status(&pool).await?;

    // Parse --since / --until into Unix timestamps if provided
    let since_ts = args.since.as_deref().map(parse_datetime).transpose()?;
    let until_ts = args.until.as_deref().map(parse_datetime).transpose()?;

    let need_times = since_ts.is_some() || until_ts.is_some();

    // Fetch first-event timestamp for each file when time filtering is needed
    let timestamps: Vec<Option<u64>> = if need_times {
        let mut ts_vec = Vec::with_capacity(files.len());
        for f in &files {
            let ts = first_event_time(&pool, args.server_id, &f.log_name).await?;
            ts_vec.push(ts);
        }
        ts_vec
    } else {
        vec![None; files.len()]
    };

    // Apply time filter — keep files that overlap the requested range.
    // A file "overlaps" if its first event time falls within the range, OR if it
    // is the file immediately before the range start (it may contain the crossing).
    let entries: Vec<(&BinlogFile, Option<u64>)> = files
        .iter()
        .zip(timestamps.iter())
        .filter(|(_, ts)| {
            if !need_times {
                return true;
            }
            let t = match ts {
                Some(t) => *t,
                None    => return true, // keep files where we couldn't read a timestamp
            };
            let after  = since_ts.map(|s| t >= s).unwrap_or(true);
            let before = until_ts.map(|u| t <= u).unwrap_or(true);
            after && before
        })
        .map(|(f, ts)| (f, *ts))
        .collect();

    match args.format.as_str() {
        "json" => print_json(&entries, &cur_file, cur_pos),
        _      => print_table(&entries, &cur_file, cur_pos, need_times),
    }

    pool.disconnect().await?;
    Ok(())
}

// ── Formatters ─────────────────────────────────────────────────────────────────

fn fmt_ts(ts: u64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_opt(ts as i64, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn print_table(
    entries: &[(&BinlogFile, Option<u64>)],
    cur_file: &str,
    cur_pos: u64,
    show_times: bool,
) {
    let total: u64 = entries.iter().map(|(f, _)| f.file_size).sum();

    if show_times {
        println!("{:<30}  {:>14}  {:<22}  {}", "File", "Size (bytes)", "First event (UTC)", "Note");
        println!("{}", "-".repeat(86));
        for (f, ts) in entries {
            let time_str = ts.map(|t| fmt_ts(t)).unwrap_or_else(|| "-".into());
            let marker = if f.log_name == cur_file {
                format!("← current pos {}", cur_pos)
            } else {
                String::new()
            };
            println!("{:<30}  {:>14}  {:<22}  {}", f.log_name, f.file_size, time_str, marker);
        }
        println!("{}", "-".repeat(86));
    } else {
        println!("{:<30}  {:>14}  {}", "File", "Size (bytes)", "Current position");
        println!("{}", "-".repeat(65));
        for (f, _) in entries {
            let marker = if f.log_name == cur_file {
                format!("← pos {}", cur_pos)
            } else {
                String::new()
            };
            println!("{:<30}  {:>14}  {}", f.log_name, f.file_size, marker);
        }
        println!("{}", "-".repeat(65));
    }

    println!(
        "{:<30}  {:>14}",
        format!("{} file(s)", entries.len()),
        total
    );
    println!();
    println!("Current write position: {}:{}", cur_file, cur_pos);
    println!("Use with --binlog-start to replay from a specific file, e.g.:");
    println!("  --binlog-start {}:{}", cur_file, cur_pos);
}

fn print_json(
    entries: &[(&BinlogFile, Option<u64>)],
    cur_file: &str,
    cur_pos: u64,
) {
    let arr: Vec<_> = entries
        .iter()
        .map(|(f, ts)| {
            json!({
                "log_name":         f.log_name,
                "file_size":        f.file_size,
                "first_event_time": ts.map(fmt_ts),
                "is_current":       f.log_name == cur_file,
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

