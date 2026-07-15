use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Level::Debug => write!(f, "DEBUG"),
            Level::Info => write!(f, "INFO"),
            Level::Warn => write!(f, "WARN"),
            Level::Error => write!(f, "ERROR"),
        }
    }
}

impl Level {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "debug" => Level::Debug,
            "warn" | "warning" => Level::Warn,
            "error" => Level::Error,
            _ => Level::Info,
        }
    }
}

/// Size-tracking writer. Rotates to `path.1` when the next write would exceed `max_bytes`.
struct RotatingFile {
    path: PathBuf,
    /// `None` only briefly during rotate on Windows (file must be closed before rename).
    file: Option<File>,
    size: u64,
    /// `0` = rotation disabled.
    max_bytes: u64,
}

impl RotatingFile {
    fn open(path: &Path, max_bytes: u64) -> Result<Self> {
        let file = open_append(path)?;
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(file),
            size,
            max_bytes,
        })
    }

    fn write_line(&mut self, line: &str) -> Result<()> {
        let bytes = line.as_bytes();
        if self.should_rotate(bytes.len()) {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .context("log file handle missing after rotate")?;
        file.write_all(bytes)?;
        self.size += bytes.len() as u64;
        Ok(())
    }

    fn should_rotate(&self, next_len: usize) -> bool {
        self.max_bytes > 0 && self.size > 0 && self.size + next_len as u64 > self.max_bytes
    }

    fn rotate(&mut self) -> Result<()> {
        // Flush + close the active handle (required on Windows before rename).
        if let Some(mut f) = self.file.take() {
            let _ = f.flush();
            drop(f);
        }

        let backup = backup_path(&self.path);
        if backup.exists() {
            fs::remove_file(&backup).with_context(|| {
                format!("Failed to remove old rotated log '{}'", backup.display())
            })?;
        }
        if self.path.exists() {
            fs::rename(&self.path, &backup).with_context(|| {
                format!(
                    "Failed to rotate log '{}' → '{}'",
                    self.path.display(),
                    backup.display()
                )
            })?;
        }

        self.file = Some(open_append(&self.path)?);
        self.size = 0;
        Ok(())
    }
}

fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open log file '{}'", path.display()))
}

fn backup_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".1");
    PathBuf::from(s)
}

/// Thread-safe logger that writes JSON lines to both stdout and a size-rotating log file.
#[derive(Clone)]
pub struct Logger {
    inner: Arc<Mutex<RotatingFile>>,
    min_level: Level,
}

impl Logger {
    /// Open (or create) `path` for append with rotation disabled.
    pub fn new(path: &str, level: &str) -> Result<Self> {
        Self::with_max_bytes(path, level, 0)
    }

    /// Open `path` for append. When `max_bytes > 0`, rename to `path.1` before the
    /// write that would exceed the limit, then continue on a fresh file.
    pub fn with_max_bytes(path: &str, level: &str, max_bytes: u64) -> Result<Self> {
        let file = RotatingFile::open(Path::new(path), max_bytes)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(file)),
            min_level: Level::from_str(level),
        })
    }

    /// Emit a JSON log line. `payload` must be a JSON object (`Value::Object`).
    /// `time` and `level` are prepended as the first two keys.
    pub fn log(&self, level: Level, payload: JsonValue) {
        if level < self.min_level {
            return;
        }
        let time = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let level_str = level.to_string();

        let mut out = serde_json::Map::new();
        out.insert("time".into(), JsonValue::String(time));
        out.insert("level".into(), JsonValue::String(level_str));

        if let JsonValue::Object(map) = payload {
            out.extend(map);
        }

        let line = serde_json::to_string(&JsonValue::Object(out)).unwrap_or_default() + "\n";
        print!("{line}");
        if let Ok(mut f) = self.inner.lock() {
            let _ = f.write_line(&line);
        }
    }

    pub fn info(&self, payload: JsonValue) {
        self.log(Level::Info, payload);
    }

    pub fn warn(&self, payload: JsonValue) {
        self.log(Level::Warn, payload);
    }

    #[allow(dead_code)]
    pub fn debug(&self, payload: JsonValue) {
        self.log(Level::Debug, payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_log(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("mbm_log_{name}_{nanos}.log"))
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(backup_path(path));
    }

    #[test]
    fn no_rotate_when_max_bytes_zero() {
        let path = tmp_log("norot");
        cleanup(&path);
        let logger = Logger::with_max_bytes(path.to_str().unwrap(), "info", 0).unwrap();
        for i in 0..20 {
            logger.info(json!({ "message": format!("line-{i}"), "n": i }));
        }
        assert!(path.exists());
        assert!(!backup_path(&path).exists());
        cleanup(&path);
    }

    #[test]
    fn rotates_when_exceeding_max_bytes() {
        let path = tmp_log("rot");
        cleanup(&path);

        // Small limit forces rotation after a couple of JSON lines.
        let logger = Logger::with_max_bytes(path.to_str().unwrap(), "info", 120).unwrap();
        for i in 0..10 {
            logger.info(json!({ "message": format!("rotate-me-{i}"), "n": i }));
        }

        assert!(path.exists(), "active log should exist");
        assert!(
            backup_path(&path).exists(),
            "rotated backup path.1 should exist after exceeding max_bytes"
        );

        let active = fs::read_to_string(&path).unwrap();
        let backup = fs::read_to_string(backup_path(&path)).unwrap();
        assert!(!active.is_empty());
        assert!(!backup.is_empty());
        // Active file should stay under the limit (plus one line tolerance is not needed —
        // we rotate before writing the overflowing line).
        assert!(
            fs::metadata(&path).unwrap().len() <= 120 + 200,
            "active file should not grow unbounded; len={}",
            fs::metadata(&path).unwrap().len()
        );

        cleanup(&path);
    }

    #[test]
    fn backup_path_appends_dot_one() {
        assert_eq!(
            backup_path(Path::new("binlog.log")),
            PathBuf::from("binlog.log.1")
        );
    }

    #[test]
    fn level_filter_skips_debug_when_info() {
        let path = tmp_log("level");
        cleanup(&path);
        let logger = Logger::with_max_bytes(path.to_str().unwrap(), "info", 0).unwrap();
        logger.debug(json!({ "message": "secret" }));
        logger.info(json!({ "message": "visible" }));
        let body = fs::read_to_string(&path).unwrap();
        assert!(!body.contains("secret"));
        assert!(body.contains("visible"));
        cleanup(&path);
    }
}
