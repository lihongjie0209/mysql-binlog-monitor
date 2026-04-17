use clap::Parser;
use mysql_binlog_monitor::config::{Cli, Command};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Monitor(args) => {
            let token = CancellationToken::new();
            let shutdown = token.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                token.cancel();
            });
            mysql_binlog_monitor::monitor::run_monitor(args, shutdown).await
        }
        Command::Export(args) => mysql_binlog_monitor::export::run_export(args).await,
        Command::BinlogInfo(args) => mysql_binlog_monitor::binlog_info::run_binlog_info(args).await,
    }
}
