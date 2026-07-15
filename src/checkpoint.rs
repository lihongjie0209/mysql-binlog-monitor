//! Binlog consumption checkpoint — in-memory resume + optional disk persistence.
//!
//! Position priority (first match wins):
//! 1. Runtime position (reconnect within the same process)
//! 2. `--since` time seek
//! 3. Explicit `--binlog-start` (`start` / `file:pos`)
//! 4. Disk checkpoint (when `--checkpoint-path` is set and file exists)
//!    - with `--gtid` and a non-empty `gtid_set` → GTID auto-position
//!    - otherwise file:pos
//! 5. Live end (`SHOW MASTER STATUS` / empty GTID set for live GTID)

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde_json::json;

use crate::config::BinlogStart;
use crate::gtid::ExecutedGtidSet;

/// A binlog file + byte offset. `pos` is the next-read offset
/// (equal to `event.header().log_pos()` of the last fully processed event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogPosition {
    pub file: String,
    pub pos: u64,
}

impl BinlogPosition {
    pub fn new(file: impl Into<String>, pos: u64) -> Self {
        Self {
            file: file.into(),
            pos,
        }
    }
}

/// Full checkpoint payload persisted to disk.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointState {
    pub position: Option<BinlogPosition>,
    /// MySQL GTID set of fully processed transactions.
    pub gtid_set: Option<String>,
    /// Last committed single GTID (uuid:gno), if any.
    pub last_gtid: Option<String>,
}

impl CheckpointState {
    pub fn with_position(pos: BinlogPosition) -> Self {
        Self {
            position: Some(pos),
            gtid_set: None,
            last_gtid: None,
        }
    }

    pub fn executed_set(&self) -> Result<ExecutedGtidSet> {
        match &self.gtid_set {
            Some(s) if !s.is_empty() => ExecutedGtidSet::parse(s),
            _ => Ok(ExecutedGtidSet::new()),
        }
    }
}

/// How the monitor should open the next binlog stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartKind {
    /// Open at an exact file:pos.
    Position(BinlogPosition),
    /// GTID auto-position with the given executed set (may be empty = full history).
    Gtid { executed: String },
    /// Run time-based seek (`--since`).
    TimeSeek,
    /// Open at the beginning of the current binlog file (no filename in request).
    FileBegin,
    /// Open at the live write position (`SHOW MASTER STATUS`).
    LiveEnd,
}

/// Decide where to start streaming.
///
/// Pure function — no I/O — so unit tests can cover reconnect vs cold-start rules.
pub fn decide_start(
    runtime: Option<&BinlogPosition>,
    since_provided: bool,
    binlog_start: &BinlogStart,
    disk: Option<&CheckpointState>,
    gtid_mode: bool,
    runtime_gtid_set: Option<&str>,
) -> StartKind {
    // 1. In-process resume.
    if let Some(p) = runtime {
        if gtid_mode {
            if let Some(set) = runtime_gtid_set {
                if !set.is_empty() {
                    return StartKind::Gtid {
                        executed: set.to_string(),
                    };
                }
            }
        }
        return StartKind::Position(p.clone());
    }
    // 2. Explicit time seek on cold start.
    if since_provided {
        return StartKind::TimeSeek;
    }
    // 3–5. From --binlog-start, with disk checkpoint only when starting at "end".
    match binlog_start {
        BinlogStart::At { file, pos } => {
            StartKind::Position(BinlogPosition::new(file.clone(), *pos))
        }
        BinlogStart::Start => StartKind::FileBegin,
        BinlogStart::End => {
            if gtid_mode {
                if let Some(state) = disk {
                    if let Some(set) = &state.gtid_set {
                        if !set.is_empty() {
                            return StartKind::Gtid {
                                executed: set.clone(),
                            };
                        }
                    }
                }
                // Live GTID: empty set means "fetch gtid_executed from server" later.
                return StartKind::Gtid {
                    executed: String::new(),
                };
            }
            if let Some(state) = disk {
                if let Some(p) = &state.position {
                    return StartKind::Position(p.clone());
                }
            }
            StartKind::LiveEnd
        }
    }
}

/// Load a checkpoint from disk. Missing file → `Ok(None)`.
pub fn load(path: &Path) -> Result<Option<CheckpointState>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("Failed to read checkpoint '{}'", path.display()))?;
    parse_checkpoint(&text)
        .map(Some)
        .with_context(|| format!("Invalid checkpoint file '{}'", path.display()))
}

/// Atomically write a checkpoint (temp file in the same directory, then rename).
pub fn save(path: &Path, state: &CheckpointState) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create checkpoint dir '{}'", parent.display()))?;
        }
    }

    let mut payload = serde_json::Map::new();
    if let Some(p) = &state.position {
        payload.insert("file".into(), json!(p.file));
        payload.insert("pos".into(), json!(p.pos));
    }
    if let Some(g) = &state.gtid_set {
        payload.insert("gtid_set".into(), json!(g));
    }
    if let Some(g) = &state.last_gtid {
        payload.insert("last_gtid".into(), json!(g));
    }
    let body = serde_json::to_string_pretty(&serde_json::Value::Object(payload))?;

    let tmp = tmp_path(path);
    fs::write(&tmp, body.as_bytes())
        .with_context(|| format!("Failed to write temp checkpoint '{}'", tmp.display()))?;

    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("Failed to remove old checkpoint '{}'", path.display()))?;
    }
    fs::rename(&tmp, path).with_context(|| {
        format!(
            "Failed to rename checkpoint '{}' → '{}'",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

/// Convenience: save file:pos only (keeps existing gtid fields if caller merges first).
pub fn save_position(path: &Path, pos: &BinlogPosition) -> Result<()> {
    save(path, &CheckpointState::with_position(pos.clone()))
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn parse_checkpoint(text: &str) -> Result<CheckpointState> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(|e| anyhow!("{e}"))?;

    let position = match (v.get("file").and_then(|x| x.as_str()), v.get("pos").and_then(|x| x.as_u64())) {
        (Some(file), Some(pos)) if !file.is_empty() && pos > 0 => {
            Some(BinlogPosition::new(file, pos))
        }
        (None, None) => None,
        (Some(_), Some(0)) => return Err(anyhow!("'pos' must be > 0")),
        (Some(""), _) => return Err(anyhow!("empty 'file'")),
        (Some(_), None) | (None, Some(_)) => {
            return Err(anyhow!("file and pos must both be present or both absent"));
        }
        _ => None,
    };

    let gtid_set = v
        .get("gtid_set")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    let last_gtid = v
        .get("last_gtid")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());

    if position.is_none() && gtid_set.is_none() {
        return Err(anyhow!("checkpoint must contain file:pos and/or gtid_set"));
    }

    Ok(CheckpointState {
        position,
        gtid_set,
        last_gtid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("mbm_cp_{name}_{nanos}.json"))
    }

    #[test]
    fn load_missing_returns_none() {
        let path = unique_path("missing");
        let _ = fs::remove_file(&path);
        assert_eq!(load(&path).unwrap(), None);
    }

    #[test]
    fn save_load_roundtrip_file_pos() {
        let path = unique_path("roundtrip");
        let _ = fs::remove_file(&path);
        let state = CheckpointState::with_position(BinlogPosition::new("mysql-bin.000042", 1_234_567));
        save(&path, &state).unwrap();
        assert_eq!(load(&path).unwrap(), Some(state));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn save_load_gtid_fields() {
        let path = unique_path("gtid");
        let _ = fs::remove_file(&path);
        let state = CheckpointState {
            position: Some(BinlogPosition::new("mysql-bin.000001", 100)),
            gtid_set: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-5".into()),
            last_gtid: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:5".into()),
        };
        save(&path, &state).unwrap();
        assert_eq!(load(&path).unwrap(), Some(state));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_legacy_file_pos_only() {
        let path = unique_path("legacy");
        fs::write(&path, r#"{"file":"mysql-bin.000001","pos":99}"#).unwrap();
        let st = load(&path).unwrap().unwrap();
        assert_eq!(st.position, Some(BinlogPosition::new("mysql-bin.000001", 99)));
        assert!(st.gtid_set.is_none());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_corrupt_json_errors() {
        let path = unique_path("corrupt");
        fs::write(&path, "not-json").unwrap();
        assert!(load(&path).is_err());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn decide_runtime_wins() {
        let runtime = BinlogPosition::new("mysql-bin.000009", 99);
        let disk = CheckpointState::with_position(BinlogPosition::new("mysql-bin.000001", 4));
        let kind = decide_start(
            Some(&runtime),
            true,
            &BinlogStart::At {
                file: "mysql-bin.000002".into(),
                pos: 50,
            },
            Some(&disk),
            false,
            None,
        );
        assert_eq!(kind, StartKind::Position(runtime));
    }

    #[test]
    fn decide_runtime_gtid_prefers_gtid_set() {
        let runtime = BinlogPosition::new("mysql-bin.000009", 99);
        let kind = decide_start(
            Some(&runtime),
            false,
            &BinlogStart::End,
            None,
            true,
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-3"),
        );
        assert_eq!(
            kind,
            StartKind::Gtid {
                executed: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-3".into()
            }
        );
    }

    #[test]
    fn decide_end_gtid_uses_disk_set() {
        let disk = CheckpointState {
            position: Some(BinlogPosition::new("mysql-bin.000007", 888)),
            gtid_set: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-9".into()),
            last_gtid: None,
        };
        let kind = decide_start(None, false, &BinlogStart::End, Some(&disk), true, None);
        assert_eq!(
            kind,
            StartKind::Gtid {
                executed: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-9".into()
            }
        );
    }

    #[test]
    fn decide_end_gtid_live_empty_set() {
        let kind = decide_start(None, false, &BinlogStart::End, None, true, None);
        assert_eq!(
            kind,
            StartKind::Gtid {
                executed: String::new()
            }
        );
    }

    #[test]
    fn decide_end_file_pos_without_gtid() {
        let disk = CheckpointState::with_position(BinlogPosition::new("mysql-bin.000007", 888));
        let kind = decide_start(None, false, &BinlogStart::End, Some(&disk), false, None);
        assert_eq!(
            kind,
            StartKind::Position(BinlogPosition::new("mysql-bin.000007", 888))
        );
    }

    #[test]
    fn decide_since_before_disk() {
        let disk = CheckpointState::with_position(BinlogPosition::new("mysql-bin.000001", 4));
        let kind = decide_start(None, true, &BinlogStart::End, Some(&disk), true, None);
        assert_eq!(kind, StartKind::TimeSeek);
    }

    #[test]
    fn decide_explicit_at_ignores_disk() {
        let disk = CheckpointState::with_position(BinlogPosition::new("mysql-bin.000001", 4));
        let kind = decide_start(
            None,
            false,
            &BinlogStart::At {
                file: "mysql-bin.000005".into(),
                pos: 4096,
            },
            Some(&disk),
            true,
            None,
        );
        assert_eq!(
            kind,
            StartKind::Position(BinlogPosition::new("mysql-bin.000005", 4096))
        );
    }

    #[test]
    fn decide_start_keyword_is_file_begin() {
        let kind = decide_start(None, false, &BinlogStart::Start, None, false, None);
        assert_eq!(kind, StartKind::FileBegin);
    }
}
