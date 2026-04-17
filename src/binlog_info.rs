use anyhow::Result;
use mysql_async::{Opts, OptsBuilder, Pool};
use serde_json::json;

use crate::config::BinlogInfoArgs;
use crate::db::{fetch_binary_logs, fetch_master_status};

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

    match args.format.as_str() {
        "json" => print_json(&files, &cur_file, cur_pos),
        _      => print_table(&files, &cur_file, cur_pos),
    }

    pool.disconnect().await?;
    Ok(())
}

// ── Formatters ─────────────────────────────────────────────────────────────────

fn print_table(
    files: &[crate::db::BinlogFile],
    cur_file: &str,
    cur_pos: u64,
) {
    let total: u64 = files.iter().map(|f| f.file_size).sum();

    // Header
    println!("{:<30}  {:>14}  {}", "File", "Size (bytes)", "Current position");
    println!("{}", "-".repeat(65));

    for f in files {
        let marker = if f.log_name == cur_file {
            format!("← pos {}", cur_pos)
        } else {
            String::new()
        };
        println!("{:<30}  {:>14}  {}", f.log_name, f.file_size, marker);
    }

    println!("{}", "-".repeat(65));
    println!(
        "{:<30}  {:>14}",
        format!("{} file(s)", files.len()),
        total
    );
    println!();
    println!("Current write position: {}:{}", cur_file, cur_pos);
    println!(
        "Use with --binlog-start to replay from a specific file, e.g.:"
    );
    println!("  --binlog-start {}:{}", cur_file, cur_pos);
}

fn print_json(
    files: &[crate::db::BinlogFile],
    cur_file: &str,
    cur_pos: u64,
) {
    let arr: Vec<_> = files
        .iter()
        .map(|f| {
            json!({
                "log_name":  f.log_name,
                "file_size": f.file_size,
                "is_current": f.log_name == cur_file,
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
