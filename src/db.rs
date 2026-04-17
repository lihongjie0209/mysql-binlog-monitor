use std::collections::HashMap;

use anyhow::Result;
use mysql_async::prelude::*;
use mysql_async::Pool;

/// (schema, table) → ordered list of primary key column names
pub type PkMap = HashMap<(String, String), Vec<String>>;

/// (schema, table) → ordered list of all column names (by ORDINAL_POSITION)
pub type ColMap = HashMap<(String, String), Vec<String>>;

/// Fetch primary key columns for ALL tables at startup.
pub async fn fetch_all_primary_keys(pool: &Pool) -> Result<PkMap> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String, String)> = conn
        .query(
            r"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
              FROM information_schema.KEY_COLUMN_USAGE
              WHERE CONSTRAINT_NAME = 'PRIMARY'
              ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
        )
        .await?;

    let mut map: PkMap = HashMap::new();
    for (schema, table, column) in rows {
        map.entry((schema, table)).or_default().push(column);
    }
    Ok(map)
}

/// Fetch column names for ALL tables at startup.
pub async fn fetch_all_column_names(pool: &Pool) -> Result<ColMap> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String, String)> = conn
        .query(
            r"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
              FROM information_schema.COLUMNS
              ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
        )
        .await?;

    let mut map: ColMap = HashMap::new();
    for (schema, table, column) in rows {
        map.entry((schema, table)).or_default().push(column);
    }
    Ok(map)
}

/// Fetch primary key columns for a single table (used for newly-seen tables).
pub async fn fetch_primary_keys_for_table(
    pool: &Pool,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<String> = conn
        .exec(
            r"SELECT COLUMN_NAME
              FROM information_schema.KEY_COLUMN_USAGE
              WHERE CONSTRAINT_NAME = 'PRIMARY'
                AND TABLE_SCHEMA = :schema
                AND TABLE_NAME   = :table
              ORDER BY ORDINAL_POSITION",
            params! { "schema" => schema, "table" => table },
        )
        .await?;
    Ok(rows)
}

/// One row from SHOW BINARY LOGS.
#[derive(Debug)]
pub struct BinlogFile {
    pub log_name:  String,
    pub file_size: u64,
}

/// Fetch all available binlog files via SHOW BINARY LOGS.
pub async fn fetch_binary_logs(pool: &Pool) -> Result<Vec<BinlogFile>> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<mysql_async::Row> = conn.query("SHOW BINARY LOGS").await?;
    let mut files = Vec::with_capacity(rows.len());
    for row in rows {
        let log_name: String = row.get(0).ok_or_else(|| anyhow::anyhow!("Missing Log_name"))?;
        let file_size: u64   = row.get(1).ok_or_else(|| anyhow::anyhow!("Missing File_size"))?;
        files.push(BinlogFile { log_name, file_size });
    }
    Ok(files)
}

pub async fn fetch_master_status(pool: &Pool) -> Result<(String, u64)> {
    let mut conn = pool.get_conn().await?;
    let row: mysql_async::Row = conn
        .query_first("SHOW MASTER STATUS")
        .await?
        .ok_or_else(|| anyhow::anyhow!("SHOW MASTER STATUS returned no rows"))?;
    let file: String = row.get(0).ok_or_else(|| anyhow::anyhow!("Missing File column"))?;
    let pos: u64    = row.get(1).ok_or_else(|| anyhow::anyhow!("Missing Position column"))?;
    Ok((file, pos))
}

/// Fetch column names for a single table (used for newly-seen tables).
pub async fn fetch_column_names_for_table(
    pool: &Pool,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<String> = conn
        .exec(
            r"SELECT COLUMN_NAME
              FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = :schema
                AND TABLE_NAME   = :table
              ORDER BY ORDINAL_POSITION",
            params! { "schema" => schema, "table" => table },
        )
        .await?;
    Ok(rows)
}
