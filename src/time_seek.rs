use anyhow::{anyhow, Result};
use futures::StreamExt;
use mysql_async::{BinlogStreamRequest, Pool};

use crate::db::BinlogFile;

// ── Datetime parsing ───────────────────────────────────────────────────────────

/// Parse a human-readable datetime string into a Unix timestamp (seconds, UTC).
///
/// Accepted formats:
///   2026-04-17T10:00:00Z          (RFC 3339, UTC)
///   2026-04-17T10:00:00+08:00     (RFC 3339, with offset)
///   2026-04-17T10:00:00           (ISO 8601, treated as UTC)
///   2026-04-17 10:00:00           (common SQL format, treated as UTC)
pub fn parse_datetime(s: &str) -> Result<u64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp() as u64);
    }
    for fmt in &["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(ndt.and_utc().timestamp() as u64);
        }
    }
    Err(anyhow!(
        "Cannot parse datetime '{}'. \
         Use RFC 3339 (e.g. 2026-04-17T10:00:00Z or 2026-04-17T10:00:00+08:00) \
         or 'YYYY-MM-DD HH:MM:SS' (treated as UTC).",
        s
    ))
}

// ── Per-file first-event timestamp ────────────────────────────────────────────

/// Return the Unix timestamp of the first non-metadata event in `file`.
///
/// Opens a short-lived binlog stream, reads up to `MAX_SCAN` events looking
/// for the first one with a non-zero timestamp, then drops the connection.
/// Returns `None` if no such event is found (empty / metadata-only file).
pub async fn first_event_time(pool: &Pool, server_id: u32, file: &str) -> Result<Option<u64>> {
    const MAX_SCAN: usize = 20;

    let conn = pool.get_conn().await?;
    let req = BinlogStreamRequest::new(server_id)
        .with_filename(file.as_bytes())
        .with_pos(4);
    let mut stream = conn.get_binlog_stream(req).await?;

    let mut scanned = 0;
    while let Some(ev) = stream.next().await {
        let ev = ev?;
        let ts = ev.header().timestamp() as u64;
        if ts > 0 {
            return Ok(Some(ts));
        }
        scanned += 1;
        if scanned >= MAX_SCAN {
            break;
        }
    }
    Ok(None)
}

// ── Position seek by timestamp ────────────────────────────────────────────────

/// Given an ordered slice of binlog files (oldest → newest), find the
/// `(file, pos)` of the first event whose timestamp is ≥ `target_ts`.
///
/// Algorithm:
///   1. Scan each file's first event timestamp to locate the "crossing" file —
///      the last file whose first event is ≤ `target_ts`.
///   2. Scan that file event-by-event to pinpoint the exact byte offset.
///
/// Falls back to `(current_file, current_pos)` when `target_ts` is in the
/// future (beyond all available history).
pub async fn find_pos_by_time(
    pool: &Pool,
    server_id: u32,
    files: &[BinlogFile],
    target_ts: u64,
    current_file: &str,
    current_pos: u64,
) -> Result<(String, u64)> {
    if files.is_empty() {
        return Ok((current_file.to_string(), current_pos));
    }

    // ── Step 1: collect first timestamps ──────────────────────────────────────
    let mut candidates: Vec<(&BinlogFile, Option<u64>)> = Vec::with_capacity(files.len());
    for f in files {
        let ts = first_event_time(pool, server_id, &f.log_name).await?;
        candidates.push((f, ts));
    }

    // ── Step 2: find the crossing file ────────────────────────────────────────
    // The crossing file is the last file whose first event ts ≤ target_ts.
    // If every file starts after target_ts, start from the very first file.
    let crossing_idx = candidates
        .iter()
        .enumerate()
        .filter(|(_, (_, ts))| ts.map(|t| t <= target_ts).unwrap_or(false))
        .map(|(i, _)| i)
        .last()
        .unwrap_or(0);

    let crossing_file = candidates[crossing_idx].0.log_name.as_str();

    // ── Step 3: scan crossing file for exact position ─────────────────────────
    let pos = scan_file_for_time(pool, server_id, crossing_file, target_ts).await?;
    Ok((crossing_file.to_string(), pos))
}

/// Scan a single binlog file and return the start position of the first event
/// with timestamp ≥ `target_ts`.  Returns `4` if the target is before the
/// first event, or the end-of-file position if all events are before target.
async fn scan_file_for_time(
    pool: &Pool,
    server_id: u32,
    file: &str,
    target_ts: u64,
) -> Result<u64> {
    let conn = pool.get_conn().await?;
    let req = BinlogStreamRequest::new(server_id)
        .with_filename(file.as_bytes())
        .with_pos(4);
    let mut stream = conn.get_binlog_stream(req).await?;

    // next_start tracks the byte offset where the NEXT event begins.
    // We start at 4 (first byte after the 4-byte magic header).
    let mut next_start: u64 = 4;
    let mut last_pos: u64 = 4;

    while let Some(ev) = stream.next().await {
        let ev = ev?;
        let current_start = next_start;
        // log_pos in the header is the end offset of this event (= start of next)
        next_start = ev.header().log_pos() as u64;

        let ts = ev.header().timestamp() as u64;
        if ts > 0 && ts >= target_ts {
            return Ok(current_start);
        }
        last_pos = next_start;
    }

    // All events are before target_ts — return end of file
    Ok(last_pos)
}
