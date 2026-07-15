//! MySQL privilege checks and operator guidance (CREATE USER / GRANT examples).

use anyhow::{anyhow, Result};
use mysql_async::prelude::*;
use mysql_async::Pool;

// ── SQL snippets shown in --help and in runtime error messages ─────────────────

/// Full CREATE USER + GRANT for a dedicated replication account.
pub const SQL_CREATE_REPL_USER: &str = r#"-- Create a dedicated replication user (run as admin / root)
CREATE USER 'repl'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;"#;

/// GRANT for an existing account that will stream binlog.
pub const SQL_GRANT_EXISTING_REPL: &str = r#"-- Grant binlog privileges to an existing user (run as admin / root)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'existing_user'@'%';
FLUSH PRIVILEGES;"#;

/// Optional metadata user for information_schema (column / PK names).
pub const SQL_CREATE_META_USER: &str = r#"-- Optional: separate read-only user for table metadata
CREATE USER 'meta'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT ON information_schema.* TO 'meta'@'%';
-- Or allow SELECT on the business schemas you monitor:
-- GRANT SELECT ON shop_*.* TO 'meta'@'%';
FLUSH PRIVILEGES;"#;

/// GRANT metadata SELECT to an existing user.
pub const SQL_GRANT_EXISTING_META: &str = r#"-- Grant metadata SELECT to an existing user (run as admin / root)
GRANT SELECT ON information_schema.* TO 'existing_user'@'%';
FLUSH PRIVILEGES;"#;

/// Combined one-user setup (simplest).
pub const SQL_ALL_IN_ONE: &str = r#"-- Single user with everything this tool needs
CREATE USER 'binlog'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlog'@'%';
GRANT SELECT ON information_schema.* TO 'binlog'@'%';
FLUSH PRIVILEGES;"#;

/// Text block for clap `after_help` / README.
pub const HELP_MYSQL_PRIVILEGES: &str = r#"MySQL privileges
  This tool needs:
    REPLICATION CLIENT  - SHOW MASTER STATUS / SHOW BINARY LOGS
    REPLICATION SLAVE   - open the binlog dump stream
    SELECT (optional)   - information_schema for column / primary-key names
                          (without it, logs use col_N keys and PK falls back to "id")

  Create a new user:
    CREATE USER 'repl'@'%' IDENTIFIED BY 'your_password';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
    GRANT SELECT ON information_schema.* TO 'repl'@'%';
    FLUSH PRIVILEGES;

  Grant to an existing user:
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'existing_user'@'%';
    GRANT SELECT ON information_schema.* TO 'existing_user'@'%';
    FLUSH PRIVILEGES;

  Split credentials (replication user without SELECT):
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
    CREATE USER 'meta'@'%' IDENTIFIED BY 'meta_password';
    GRANT SELECT ON information_schema.* TO 'meta'@'%';
    FLUSH PRIVILEGES;
    # then: --user repl --password ... --metadata-user meta --metadata-password ..."#;

// ── Parsed privilege flags ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrivilegeFlags {
    pub replication_client: bool,
    pub replication_slave: bool,
    /// Global SELECT or SELECT on information_schema / *.*
    pub select_metadata: bool,
    /// Raw SHOW GRANTS lines (for diagnostics)
    pub grants: Vec<String>,
}

impl PrivilegeFlags {
    pub fn has_binlog_stream(&self) -> bool {
        self.replication_client && self.replication_slave
    }

    /// True when grants only assign roles / USAGE and do not list concrete privileges.
    /// In that case SHOW GRANTS may under-report until roles are expanded.
    pub fn grants_look_like_roles_only(&self) -> bool {
        if self.replication_client || self.replication_slave || self.select_metadata {
            return false;
        }
        if self.grants.is_empty() {
            return true;
        }
        self.grants.iter().all(|line| {
            let u = line.to_ascii_uppercase();
            // `GRANT `role`@`%` TO `user`@`%``
            // or `GRANT USAGE ON *.* TO ...`
            (!u.starts_with("GRANT ") && !u.starts_with("PROXY "))
                || u.contains(" USAGE ON ")
                || (u.starts_with("GRANT ")
                    && !u.contains(" ON ")
                    && u.contains(" TO "))
                || (u.starts_with("GRANT `") && u.contains("` TO "))
        })
    }
}

/// Parse `SHOW GRANTS` lines into flags.
///
/// Handles forms like:
/// - `GRANT ALL PRIVILEGES ON *.* TO ...`
/// - `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO ...`
/// - `GRANT SELECT ON information_schema.* TO ...`
/// - Role activation lines are ignored.
pub fn parse_grants(lines: &[String]) -> PrivilegeFlags {
    let mut flags = PrivilegeFlags {
        grants: lines.to_vec(),
        ..Default::default()
    };

    for line in lines {
        let upper = line.to_ascii_uppercase();
        if !upper.starts_with("GRANT ") {
            continue;
        }

        // ALL PRIVILEGES / ALL on *.* covers everything we need
        if upper.contains("ALL PRIVILEGES") || upper.contains("GRANT ALL ON") {
            flags.replication_client = true;
            flags.replication_slave = true;
            flags.select_metadata = true;
            continue;
        }

        if upper.contains("REPLICATION CLIENT") {
            flags.replication_client = true;
        }
        if upper.contains("REPLICATION SLAVE") {
            flags.replication_slave = true;
        }

        // SELECT privilege: global *.* or information_schema
        if upper.contains("SELECT") {
            if upper.contains(" ON *.*")
                || upper.contains(" ON `*.*`")
                || upper.contains("INFORMATION_SCHEMA")
            {
                flags.select_metadata = true;
            }
        }
    }

    flags
}

/// Load privileges for the current connection via `SHOW GRANTS`.
pub async fn fetch_privilege_flags(pool: &Pool) -> Result<PrivilegeFlags> {
    let mut conn = pool.get_conn().await?;
    let rows: Vec<mysql_async::Row> = conn.query("SHOW GRANTS").await?;
    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        let s = row_first_string(&row)
            .ok_or_else(|| anyhow!("SHOW GRANTS returned a row without a string column"))?;
        lines.push(s);
    }
    Ok(parse_grants(&lines))
}

fn row_first_string(row: &mysql_async::Row) -> Option<String> {
    if let Some(s) = row.get::<String, _>(0) {
        return Some(s);
    }
    if let Some(b) = row.get::<Vec<u8>, _>(0) {
        return Some(String::from_utf8_lossy(&b).into_owned());
    }
    None
}

// ── Guidance messages ──────────────────────────────────────────────────────────

/// What a command needs from MySQL.
#[derive(Debug, Clone, Copy)]
pub struct RequiredPrivileges {
    pub replication_client: bool,
    pub replication_slave: bool,
    /// If true, missing SELECT is a hard error; if false, only a warning.
    pub select_metadata_required: bool,
}

impl RequiredPrivileges {
    pub fn monitor_or_scan() -> Self {
        Self {
            replication_client: true,
            replication_slave: true,
            select_metadata_required: false,
        }
    }

    pub fn binlog_info() -> Self {
        Self {
            replication_client: true,
            // timestamp scan opens a short dump stream
            replication_slave: true,
            select_metadata_required: false,
        }
    }
}

/// Human-readable fix steps for missing privileges.
pub fn guidance_for_missing(
    missing_client: bool,
    missing_slave: bool,
    missing_select: bool,
    user: &str,
) -> String {
    let mut parts = Vec::new();

    if missing_client || missing_slave {
        parts.push(format!(
            "User '{user}' is missing binlog privileges (need REPLICATION CLIENT + REPLICATION SLAVE)."
        ));
        parts.push("Fix — grant to existing user:".into());
        parts.push(format!(
            "  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%';\n  FLUSH PRIVILEGES;"
        ));
        parts.push("Or create a new user:".into());
        parts.push(
            "  CREATE USER 'repl'@'%' IDENTIFIED BY 'your_password';\n  \
             GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';\n  \
             FLUSH PRIVILEGES;"
                .into(),
        );
    }

    if missing_select {
        parts.push(format!(
            "User '{user}' cannot SELECT information_schema (column / PK names will be incomplete)."
        ));
        parts.push("Fix — grant to existing user:".into());
        parts.push(format!(
            "  GRANT SELECT ON information_schema.* TO '{user}'@'%';\n  FLUSH PRIVILEGES;"
        ));
        parts.push(
            "Or use a dedicated metadata user: --metadata-user meta --metadata-password ...".into(),
        );
        parts.push(
            "  CREATE USER 'meta'@'%' IDENTIFIED BY 'your_password';\n  \
             GRANT SELECT ON information_schema.* TO 'meta'@'%';\n  \
             FLUSH PRIVILEGES;"
                .into(),
        );
    }

    parts.join("\n")
}

/// Result of a startup privilege audit.
#[derive(Debug)]
pub struct PrivilegeAudit {
    pub flags: PrivilegeFlags,
    /// Hard failures (cannot stream / list binlogs).
    pub errors: Vec<String>,
    /// Soft issues (metadata degraded).
    pub warnings: Vec<String>,
}

/// Check privileges against requirements; never opens a dump stream.
pub fn audit_privileges(
    flags: &PrivilegeFlags,
    required: RequiredPrivileges,
    user: &str,
) -> PrivilegeAudit {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let missing_client = required.replication_client && !flags.replication_client;
    let missing_slave = required.replication_slave && !flags.replication_slave;
    let missing_select = !flags.select_metadata;

    if missing_client || missing_slave {
        errors.push(guidance_for_missing(missing_client, missing_slave, false, user));
    }

    if missing_select {
        let msg = guidance_for_missing(false, false, true, user);
        if required.select_metadata_required {
            errors.push(msg);
        } else {
            warnings.push(msg);
        }
    }

    PrivilegeAudit {
        flags: flags.clone(),
        errors,
        warnings,
    }
}

/// Fetch + audit. Returns `Ok(audit)` always when SHOW GRANTS works;
/// `Err` only if we cannot inspect privileges (connection/SQL error).
pub async fn check_privileges(
    pool: &Pool,
    required: RequiredPrivileges,
    user: &str,
) -> Result<PrivilegeAudit> {
    let flags = fetch_privilege_flags(pool).await?;
    Ok(audit_privileges(&flags, required, user))
}

/// Fail with a multi-line error if hard privileges are missing.
pub fn ensure_or_fail(audit: &PrivilegeAudit) -> Result<()> {
    if audit.errors.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "Insufficient MySQL privileges:\n\n{}\n\nSee also: mysql-binlog-monitor --help",
        audit.errors.join("\n\n")
    ))
}

// ── Probe fallback when SHOW GRANTS is unreadable ──────────────────────────────

/// Best-effort operational probes (used if SHOW GRANTS fails).
pub async fn probe_replication_client(pool: &Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let _: Option<mysql_async::Row> = conn.query_first("SHOW MASTER STATUS").await?;
    let _: Vec<mysql_async::Row> = conn.query("SHOW BINARY LOGS").await?;
    Ok(())
}

pub async fn probe_metadata_select(pool: &Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let _: Option<mysql_async::Row> = conn
        .query_first("SELECT 1 FROM information_schema.COLUMNS LIMIT 1")
        .await?;
    Ok(())
}

/// Startup check for the binlog stream user.
///
/// Hard-fails when REPLICATION CLIENT / SLAVE are missing (with SQL guidance).
/// Returns soft warnings (e.g. missing SELECT) for the caller to log.
///
/// When `SHOW GRANTS` itself fails, falls back to probing `SHOW MASTER STATUS` /
/// `SHOW BINARY LOGS` (covers REPLICATION CLIENT only; SLAVE is verified when the
/// dump stream opens).
pub async fn require_stream_privileges(pool: &Pool, user: &str) -> Result<Vec<String>> {
    let required = RequiredPrivileges::monitor_or_scan();
    match check_privileges(pool, required, user).await {
        Ok(audit) => {
            // If SHOW GRANTS under-reports (e.g. unexpanded roles) but the account
            // can still run CLIENT commands, allow start and keep SELECT warnings.
            if !audit.errors.is_empty() {
                if audit.flags.grants_look_like_roles_only()
                    && probe_replication_client(pool).await.is_ok()
                {
                    return Ok(audit.warnings);
                }
                ensure_or_fail(&audit)?;
            }
            Ok(audit.warnings)
        }
        Err(e) => {
            // Cannot SHOW GRANTS — probe commands instead
            if let Err(probe_err) = probe_replication_client(pool).await {
                return Err(anyhow!(
                    "Cannot use binlog as user '{user}': {probe_err}\n\n{}\n\n\
                     (Also failed to inspect grants: {e})\n\nSee also: mysql-binlog-monitor --help",
                    guidance_for_missing(true, true, false, user)
                ));
            }
            Ok(vec![])
        }
    }
}

/// Soft check for metadata SELECT. Returns `Some(guidance)` when missing.
pub async fn metadata_select_warning(pool: &Pool, user: &str) -> Option<String> {
    match check_privileges(
        pool,
        RequiredPrivileges {
            replication_client: false,
            replication_slave: false,
            select_metadata_required: false,
        },
        user,
    )
    .await
    {
        Ok(audit) if !audit.flags.select_metadata => {
            // Confirm with probe (SHOW GRANTS can miss schema-level SELECT)
            if probe_metadata_select(pool).await.is_ok() {
                return None;
            }
            Some(guidance_for_missing(false, false, true, user))
        }
        Ok(_) => None,
        Err(_) => {
            if probe_metadata_select(pool).await.is_ok() {
                None
            } else {
                Some(guidance_for_missing(false, false, true, user))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_all_privileges() {
        let lines = vec!["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'".into()];
        let f = parse_grants(&lines);
        assert!(f.replication_client);
        assert!(f.replication_slave);
        assert!(f.select_metadata);
        assert!(f.has_binlog_stream());
    }

    #[test]
    fn parse_replication_only() {
        let lines = vec![
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%'".into(),
        ];
        let f = parse_grants(&lines);
        assert!(f.replication_client);
        assert!(f.replication_slave);
        assert!(!f.select_metadata);
    }

    #[test]
    fn parse_select_information_schema() {
        let lines = vec![
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%'".into(),
            "GRANT SELECT ON information_schema.* TO 'repl'@'%'".into(),
        ];
        let f = parse_grants(&lines);
        assert!(f.select_metadata);
        assert!(f.has_binlog_stream());
    }

    #[test]
    fn parse_select_global() {
        let lines = vec!["GRANT SELECT ON *.* TO 'ro'@'%'".into()];
        let f = parse_grants(&lines);
        assert!(f.select_metadata);
        assert!(!f.replication_client);
    }

    #[test]
    fn parse_usage_only() {
        let lines = vec!["GRANT USAGE ON *.* TO 'nobody'@'%'".into()];
        let f = parse_grants(&lines);
        assert!(!f.replication_client);
        assert!(!f.replication_slave);
        assert!(!f.select_metadata);
    }

    #[test]
    fn audit_hard_fail_missing_repl() {
        let flags = PrivilegeFlags::default();
        let audit = audit_privileges(&flags, RequiredPrivileges::monitor_or_scan(), "repl");
        assert!(!audit.errors.is_empty());
        assert!(audit.errors[0].contains("REPLICATION"));
        assert!(audit.errors[0].contains("CREATE USER") || audit.errors[0].contains("GRANT"));
        // SELECT is warning only for monitor
        assert!(!audit.warnings.is_empty());
    }

    #[test]
    fn audit_ok_with_repl_only_warns_select() {
        let flags = PrivilegeFlags {
            replication_client: true,
            replication_slave: true,
            select_metadata: false,
            grants: vec![],
        };
        let audit = audit_privileges(&flags, RequiredPrivileges::monitor_or_scan(), "repl");
        assert!(audit.errors.is_empty());
        assert_eq!(audit.warnings.len(), 1);
        assert!(audit.warnings[0].contains("information_schema"));
        ensure_or_fail(&audit).unwrap();
    }

    #[test]
    fn audit_ok_full() {
        let flags = PrivilegeFlags {
            replication_client: true,
            replication_slave: true,
            select_metadata: true,
            grants: vec![],
        };
        let audit = audit_privileges(&flags, RequiredPrivileges::monitor_or_scan(), "repl");
        assert!(audit.errors.is_empty());
        assert!(audit.warnings.is_empty());
    }

    #[test]
    fn guidance_mentions_existing_and_create() {
        let g = guidance_for_missing(true, true, false, "app");
        assert!(g.contains("existing"));
        assert!(g.contains("CREATE USER"));
        assert!(g.contains("'app'@'%'") || g.contains("app"));
    }

    #[test]
    fn help_text_nonempty() {
        let h = HELP_MYSQL_PRIVILEGES;
        assert!(h.contains("CREATE USER"));
        assert!(h.contains("REPLICATION SLAVE"));
        assert!(h.contains("existing_user"));
    }

    #[test]
    fn roles_only_detection() {
        let flags = PrivilegeFlags {
            grants: vec![
                "GRANT `binlog_role`@`%` TO `app`@`%`".into(),
                "GRANT USAGE ON *.* TO `app`@`%`".into(),
            ],
            ..Default::default()
        };
        assert!(flags.grants_look_like_roles_only());

        let explicit = parse_grants(&[
            "GRANT REPLICATION CLIENT ON *.* TO 'app'@'%'".into(),
        ]);
        assert!(!explicit.grants_look_like_roles_only());
    }
}
