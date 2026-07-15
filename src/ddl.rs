//! Detect schema-changing DDL so column/PK caches can be invalidated.

/// Returns `true` when `sql` looks like a statement that may change table layout
/// (columns, primary keys, table existence). Used to flush metadata caches.
///
/// Matching is intentionally conservative (prefix keywords only). Transaction
/// markers (`BEGIN`/`COMMIT`) and DML are not treated as schema changes.
pub fn is_schema_changing_ddl(sql: &str) -> bool {
    let trimmed = strip_sql_noise(sql);
    if trimmed.is_empty() {
        return false;
    }
    let upper = trimmed.to_ascii_uppercase();

    // Multi-word prefixes first.
    const MULTI: &[&str] = &[
        "ALTER TABLE",
        "CREATE TABLE",
        "DROP TABLE",
        "RENAME TABLE",
        "TRUNCATE TABLE",
        "CREATE INDEX",
        "DROP INDEX",
        "CREATE UNIQUE INDEX",
        "ALTER DATABASE",
        "CREATE DATABASE",
        "DROP DATABASE",
        "CREATE SCHEMA",
        "DROP SCHEMA",
    ];
    for p in MULTI {
        if upper.starts_with(p) {
            // Require a boundary after the keyword phrase (space, backtick, or end).
            let rest = &upper[p.len()..];
            if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace() || c == '`' || c == '"') {
                return true;
            }
        }
    }
    false
}

/// Strip leading comments / whitespace so keyword detection is reliable.
fn strip_sql_noise(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if s.starts_with("--") {
            // Line comment
            if let Some(pos) = s.find('\n') {
                s = s[pos + 1..].trim_start();
                continue;
            }
            return "";
        }
        if s.starts_with("/*") {
            if let Some(pos) = s.find("*/") {
                s = s[pos + 2..].trim_start();
                continue;
            }
            return "";
        }
        break;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_ddl() {
        assert!(is_schema_changing_ddl("ALTER TABLE t ADD COLUMN x INT"));
        assert!(is_schema_changing_ddl("alter table `db`.`t` drop column y"));
        assert!(is_schema_changing_ddl("CREATE TABLE foo (id INT)"));
        assert!(is_schema_changing_ddl("DROP TABLE IF EXISTS foo"));
        assert!(is_schema_changing_ddl("RENAME TABLE a TO b"));
        assert!(is_schema_changing_ddl("TRUNCATE TABLE orders"));
        assert!(is_schema_changing_ddl("CREATE INDEX idx ON t(x)"));
        assert!(is_schema_changing_ddl("DROP DATABASE old_db"));
    }

    #[test]
    fn ignores_dml_and_tx() {
        assert!(!is_schema_changing_ddl("INSERT INTO t VALUES (1)"));
        assert!(!is_schema_changing_ddl("UPDATE t SET x=1"));
        assert!(!is_schema_changing_ddl("DELETE FROM t"));
        assert!(!is_schema_changing_ddl("BEGIN"));
        assert!(!is_schema_changing_ddl("COMMIT"));
        assert!(!is_schema_changing_ddl("SELECT 1"));
        assert!(!is_schema_changing_ddl("CREATE USER 'u'@'%'"));
    }

    #[test]
    fn strips_leading_comments() {
        assert!(is_schema_changing_ddl(
            "/* comment */\nALTER TABLE t ADD COLUMN z INT"
        ));
        assert!(is_schema_changing_ddl(
            "-- head\nCREATE TABLE t (id INT PRIMARY KEY)"
        ));
    }

    #[test]
    fn empty_is_not_ddl() {
        assert!(!is_schema_changing_ddl(""));
        assert!(!is_schema_changing_ddl("   "));
        assert!(!is_schema_changing_ddl("-- only comment"));
    }
}
