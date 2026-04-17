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

/// Fetch current binlog file and position via SHOW MASTER STATUS.
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
