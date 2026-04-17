use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::Result;
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

/// Thread-safe logger that writes JSON lines to both stdout and a rotating log file.
#[derive(Clone)]
pub struct Logger {
    file: Arc<Mutex<std::fs::File>>,
    min_level: Level,
}

impl Logger {
    pub fn new(path: &str, level: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
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

        // Build the final object with time+level first (preserve_order feature keeps insertion order)
        let mut out = serde_json::Map::new();
        out.insert("time".into(), JsonValue::String(time));
        out.insert("level".into(), JsonValue::String(level_str));

        if let JsonValue::Object(map) = payload {
            out.extend(map);
        }

        let line = serde_json::to_string(&JsonValue::Object(out)).unwrap_or_default() + "\n";
        print!("{line}");
        if let Ok(mut f) = self.file.lock() {
            let _ = f.write_all(line.as_bytes());
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
