//! GTID helpers: format, executed-set merge, and Sid conversion for COM_BINLOG_DUMP_GTID.
//!
//! Semantics this crate aims for:
//! - **At-least-once source**: reconnect may re-deliver until checkpoint advances.
//! - **Effectively exactly-once sink** (when GTID is available): events carry `gtid`,
//!   GlueSQL skips already-seen GTIDs, and resume uses executed GTID set.
//!
//! Streaming mode preference (`--gtid auto|on|off`):
//! - **auto** (default): use GTID dump only when server `gtid_mode=ON`
//! - **on**: force GTID dump; if server is not ON, fall back to file:pos with a warning
//! - **off**: never use GTID dump (file:pos only)

use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use mysql_async::{GnoInterval, Sid};

/// User preference for GTID-based streaming (CLI `--gtid`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GtidPreference {
    /// Detect server `gtid_mode` and enable when ON.
    Auto,
    /// Prefer GTID; fall back if server cannot support it.
    On,
    /// Never use GTID dump.
    Off,
}

impl GtidPreference {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "on" | "true" | "1" | "yes" => Ok(Self::On),
            "off" | "false" | "0" | "no" => Ok(Self::Off),
            other => Err(format!(
                "invalid --gtid value '{other}': expected auto | on | off"
            )),
        }
    }

    /// Resolve whether GTID streaming should be used.
    ///
    /// | preference | server ON | result |
    /// |---|---|---|
    /// | auto | true | true |
    /// | auto | false | false |
    /// | on | true | true |
    /// | on | false | false (caller should warn + file:pos fallback) |
    /// | off | * | false |
    pub fn resolve(self, server_gtid_on: bool) -> bool {
        match self {
            Self::Off => false,
            Self::On | Self::Auto => server_gtid_on,
        }
    }

    /// True when the user forced `--gtid on` but the server is not fully ON
    /// (so we must fall back and should warn).
    pub fn forced_but_unavailable(self, server_gtid_on: bool) -> bool {
        matches!(self, Self::On) && !server_gtid_on
    }
}

/// Format a binary SID (16 bytes) + GNO as `uuid:gno`.
pub fn format_gtid(sid: &[u8; 16], gno: u64) -> String {
    format!("{}:{}", uuid_string(sid), gno)
}

/// Format 16 raw bytes as a dashed UUID string (lowercase).
pub fn uuid_string(sid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        sid[0], sid[1], sid[2], sid[3],
        sid[4], sid[5],
        sid[6], sid[7],
        sid[8], sid[9],
        sid[10], sid[11], sid[12], sid[13], sid[14], sid[15],
    )
}

/// Parse `uuid:gno` into (uuid_lower, gno).
pub fn parse_single_gtid(s: &str) -> Result<(String, u64)> {
    let s = s.trim();
    let (uuid, gno_str) = s
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("invalid GTID '{}': expected uuid:gno", s))?;
    // Reject interval forms like uuid:1-5 here.
    if gno_str.contains('-') {
        return Err(anyhow!(
            "invalid single GTID '{}': use uuid:gno (not an interval)",
            s
        ));
    }
    let gno: u64 = gno_str
        .parse()
        .map_err(|_| anyhow!("invalid GTID gno in '{}'", s))?;
    if gno == 0 {
        return Err(anyhow!("GTID gno must be > 0"));
    }
    let uuid = uuid.trim().to_ascii_lowercase();
    if uuid.len() != 36 {
        return Err(anyhow!("invalid GTID uuid in '{}'", s));
    }
    Ok((uuid, gno))
}

/// Executed GTID set used for checkpoint resume and COM_BINLOG_DUMP_GTID.
///
/// Intervals are stored as half-open `[start, end)` matching MySQL / mysql_async
/// (`GnoInterval`), so a single GTID `uuid:5` becomes `[5, 6)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutedGtidSet {
    /// uuid (lowercase dashed) → sorted non-overlapping half-open intervals
    intervals: BTreeMap<String, Vec<(u64, u64)>>,
}

impl ExecutedGtidSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Parse a MySQL GTID set string, e.g.
    /// `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10,OTHER-UUID:1-3`.
    pub fn parse(s: &str) -> Result<Self> {
        let mut set = Self::new();
        let s = s.trim();
        if s.is_empty() {
            return Ok(set);
        }
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            // Validate with mysql_async's Sid parser, then read intervals ourselves.
            let _ = Sid::from_str(part)
                .map_err(|e| anyhow!("invalid GTID set segment '{}': {e}", part))?;
            let (uuid_str, rest) = part
                .split_once(':')
                .ok_or_else(|| anyhow!("invalid sid '{}'", part))?;
            let uuid = uuid_str.trim().to_ascii_lowercase();
            for interval in rest.split(':') {
                let (start, end) = parse_interval_token(interval)?;
                set.add_interval(&uuid, start, end);
            }
        }
        Ok(set)
    }

    /// Record a fully processed single GTID (`uuid:gno`).
    pub fn add_gtid(&mut self, uuid: &str, gno: u64) {
        if gno == 0 {
            return;
        }
        self.add_interval(&uuid.to_ascii_lowercase(), gno, gno + 1);
    }

    pub fn add_gtid_str(&mut self, gtid: &str) -> Result<()> {
        let (uuid, gno) = parse_single_gtid(gtid)?;
        self.add_gtid(&uuid, gno);
        Ok(())
    }

    pub fn contains(&self, uuid: &str, gno: u64) -> bool {
        let uuid = uuid.to_ascii_lowercase();
        let Some(list) = self.intervals.get(&uuid) else {
            return false;
        };
        list.iter().any(|&(s, e)| gno >= s && gno < e)
    }

    pub fn contains_gtid_str(&self, gtid: &str) -> bool {
        parse_single_gtid(gtid)
            .map(|(u, g)| self.contains(&u, g))
            .unwrap_or(false)
    }

    /// MySQL GTID set string for checkpoint / logging.
    pub fn to_mysql_string(&self) -> String {
        let mut parts = Vec::new();
        for (uuid, list) in &self.intervals {
            if list.is_empty() {
                continue;
            }
            let mut segs = Vec::with_capacity(list.len());
            for &(start, end) in list {
                if end == start + 1 {
                    segs.push(format!("{start}"));
                } else {
                    // MySQL human form is inclusive end: [start, end) → start-(end-1)
                    segs.push(format!("{start}-{}", end - 1));
                }
            }
            parts.push(format!("{uuid}:{}", segs.join(":")));
        }
        parts.join(",")
    }

    /// Convert to `Sid` list for `BinlogStreamRequest::with_gtid_set`.
    pub fn to_sids(&self) -> Result<Vec<Sid<'static>>> {
        let mut out = Vec::new();
        for (uuid_str, list) in &self.intervals {
            let uuid = parse_uuid_bytes(uuid_str)?;
            let mut sid = Sid::new(uuid);
            let mut intervals = Vec::new();
            for &(start, end) in list {
                intervals.push(GnoInterval::new(start, end));
            }
            sid = sid.with_intervals(intervals);
            out.push(sid);
        }
        Ok(out)
    }

    fn add_interval(&mut self, uuid: &str, start: u64, end: u64) {
        if start == 0 || end <= start {
            return;
        }
        let list = self.intervals.entry(uuid.to_string()).or_default();
        list.push((start, end));
        merge_intervals(list);
    }
}

fn parse_interval_token(token: &str) -> Result<(u64, u64)> {
    let token = token.trim();
    if let Some((a, b)) = token.split_once('-') {
        let start: u64 = a
            .parse()
            .map_err(|_| anyhow!("invalid interval start in '{token}'"))?;
        let end_incl: u64 = b
            .parse()
            .map_err(|_| anyhow!("invalid interval end in '{token}'"))?;
        if start == 0 || end_incl < start {
            return Err(anyhow!("invalid interval '{token}'"));
        }
        // Inclusive → half-open
        Ok((start, end_incl + 1))
    } else {
        let start: u64 = token
            .parse()
            .map_err(|_| anyhow!("invalid interval '{token}'"))?;
        if start == 0 {
            return Err(anyhow!("invalid interval '{token}'"));
        }
        Ok((start, start + 1))
    }
}

fn merge_intervals(list: &mut Vec<(u64, u64)>) {
    if list.is_empty() {
        return;
    }
    list.sort_by_key(|&(s, _)| s);
    let mut merged = Vec::with_capacity(list.len());
    let mut cur = list[0];
    for &(s, e) in list.iter().skip(1) {
        if s <= cur.1 {
            cur.1 = cur.1.max(e);
        } else {
            merged.push(cur);
            cur = (s, e);
        }
    }
    merged.push(cur);
    *list = merged;
}

fn parse_uuid_bytes(s: &str) -> Result<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(anyhow!("invalid uuid '{}'", s));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow!("invalid uuid hex '{}'", s))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_and_parse_single() {
        let sid = [
            0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42,
            0x95, 0x62,
        ];
        let g = format_gtid(&sid, 5);
        assert_eq!(g, "3e11fa47-71ca-11e1-9e33-c80aa9429562:5");
        let (u, n) = parse_single_gtid(&g).unwrap();
        assert_eq!(u, "3e11fa47-71ca-11e1-9e33-c80aa9429562");
        assert_eq!(n, 5);
    }

    #[test]
    fn set_add_merge_contiguous() {
        let mut set = ExecutedGtidSet::new();
        let u = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        set.add_gtid(u, 1);
        set.add_gtid(u, 2);
        set.add_gtid(u, 3);
        assert_eq!(set.to_mysql_string(), format!("{u}:1-3"));
        assert!(set.contains(u, 2));
        assert!(!set.contains(u, 4));
    }

    #[test]
    fn set_parse_roundtrip() {
        let s = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-5:10,bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee:1-2";
        let set = ExecutedGtidSet::parse(s).unwrap();
        assert!(set.contains("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 5));
        assert!(set.contains("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 10));
        assert!(!set.contains("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 6));
        assert!(set.contains("bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", 2));
        // Round-trip through string
        let again = ExecutedGtidSet::parse(&set.to_mysql_string()).unwrap();
        assert_eq!(set, again);
    }

    #[test]
    fn to_sids_non_empty() {
        let mut set = ExecutedGtidSet::new();
        set.add_gtid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 1);
        set.add_gtid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 2);
        let sids = set.to_sids().unwrap();
        assert_eq!(sids.len(), 1);
        assert_eq!(sids[0].intervals().len(), 1);
    }

    #[test]
    fn empty_parse() {
        assert!(ExecutedGtidSet::parse("").unwrap().is_empty());
        assert!(ExecutedGtidSet::parse("   ").unwrap().is_empty());
    }

    #[test]
    fn parse_single_rejects_interval_and_zero() {
        assert!(parse_single_gtid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-5").is_err());
        assert!(parse_single_gtid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:0").is_err());
        assert!(parse_single_gtid("not-a-gtid").is_err());
        assert!(parse_single_gtid("").is_err());
    }

    #[test]
    fn add_gtid_str_and_contains_str() {
        let mut set = ExecutedGtidSet::new();
        let g = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE:7";
        set.add_gtid_str(g).unwrap();
        // UUID comparison is case-insensitive
        assert!(set.contains_gtid_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:7"));
        assert!(!set.contains_gtid_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:8"));
        assert!(!set.contains_gtid_str("bad"));
    }

    #[test]
    fn merge_out_of_order_and_overlapping() {
        let mut set = ExecutedGtidSet::new();
        let u = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        set.add_gtid(u, 5);
        set.add_gtid(u, 1);
        set.add_gtid(u, 3);
        set.add_gtid(u, 2);
        set.add_gtid(u, 4);
        assert_eq!(set.to_mysql_string(), format!("{u}:1-5"));
    }

    #[test]
    fn merge_leaves_gaps() {
        let mut set = ExecutedGtidSet::new();
        let u = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        set.add_gtid(u, 1);
        set.add_gtid(u, 2);
        set.add_gtid(u, 10);
        assert_eq!(set.to_mysql_string(), format!("{u}:1-2:10"));
        assert!(!set.contains(u, 3));
        assert!(set.contains(u, 10));
    }

    #[test]
    fn parse_invalid_set_errors() {
        assert!(ExecutedGtidSet::parse("not-valid").is_err());
        assert!(ExecutedGtidSet::parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").is_err());
        assert!(ExecutedGtidSet::parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:0").is_err());
    }

    #[test]
    fn single_gtid_to_sids_interval_half_open() {
        let mut set = ExecutedGtidSet::new();
        set.add_gtid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 5);
        let sids = set.to_sids().unwrap();
        assert_eq!(sids.len(), 1);
        // One half-open interval [5, 6)
        assert_eq!(sids[0].intervals().len(), 1);
    }

    #[test]
    fn preference_parse() {
        assert_eq!(GtidPreference::parse("auto").unwrap(), GtidPreference::Auto);
        assert_eq!(GtidPreference::parse("ON").unwrap(), GtidPreference::On);
        assert_eq!(GtidPreference::parse("off").unwrap(), GtidPreference::Off);
        assert_eq!(GtidPreference::parse("true").unwrap(), GtidPreference::On);
        assert!(GtidPreference::parse("maybe").is_err());
    }

    #[test]
    fn preference_resolve_auto_detects() {
        assert!(GtidPreference::Auto.resolve(true));
        assert!(!GtidPreference::Auto.resolve(false));
        assert!(GtidPreference::On.resolve(true));
        assert!(!GtidPreference::On.resolve(false));
        assert!(!GtidPreference::Off.resolve(true));
        assert!(!GtidPreference::Off.resolve(false));
    }

    #[test]
    fn preference_forced_but_unavailable() {
        assert!(GtidPreference::On.forced_but_unavailable(false));
        assert!(!GtidPreference::On.forced_but_unavailable(true));
        assert!(!GtidPreference::Auto.forced_but_unavailable(false));
        assert!(!GtidPreference::Off.forced_but_unavailable(false));
    }
}
