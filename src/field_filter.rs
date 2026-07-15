//! Row field value filters for `monitor` and `scan`.
//!
//! CLI form: `--field-filter col=value` (repeatable, AND semantics).
//! Value may use `*` / `?` wildcards (same rules as db/table filters).
//!
//! UPDATE rows match if **either** `before_values` or `after_values` satisfies
//! all predicates; INSERT/DELETE use `values`.

use serde_json::Value as JsonValue;

/// One column predicate: `column` must match `expected` (exact or wildcard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPredicate {
    pub column: String,
    pub expected: String,
}

/// Parse `col=value` (first `=` separates name from value; value may contain `=`).
pub fn parse_field_filter(s: &str) -> Result<FieldPredicate, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty --field-filter".into());
    }
    let (col, val) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid --field-filter '{s}': expected COL=VALUE"))?;
    let column = col.trim().to_string();
    if column.is_empty() {
        return Err(format!("invalid --field-filter '{s}': empty column name"));
    }
    // Keep value as-is after first '=', only trim outer whitespace of the whole value side
    Ok(FieldPredicate {
        column,
        expected: val.to_string(),
    })
}

pub fn parse_field_filters(specs: &[String]) -> Result<Vec<FieldPredicate>, String> {
    specs.iter().map(|s| parse_field_filter(s)).collect()
}

/// Whether the event `row` object matches all predicates (empty list → true).
pub fn row_matches_fields(row: &JsonValue, preds: &[FieldPredicate]) -> bool {
    if preds.is_empty() {
        return true;
    }
    if let Some(obj) = row.get("values").and_then(|v| v.as_object()) {
        return object_matches(obj, preds);
    }
    let before = row.get("before_values").and_then(|v| v.as_object());
    let after = row.get("after_values").and_then(|v| v.as_object());
    match (before, after) {
        (Some(b), Some(a)) => object_matches(b, preds) || object_matches(a, preds),
        (Some(b), None) => object_matches(b, preds),
        (None, Some(a)) => object_matches(a, preds),
        _ => false,
    }
}

fn object_matches(
    obj: &serde_json::Map<String, JsonValue>,
    preds: &[FieldPredicate],
) -> bool {
    preds.iter().all(|p| match obj.get(&p.column) {
        Some(v) => value_matches(v, &p.expected),
        None => false,
    })
}

/// Compare a JSON cell to the user-provided expected string.
fn value_matches(v: &JsonValue, expected: &str) -> bool {
    let expected = expected.trim();
    // Null: match empty expected or literal "null"
    if v.is_null() {
        return expected.is_empty() || expected.eq_ignore_ascii_case("null");
    }
    let actual = json_to_match_string(v);
    if expected.contains('*') || expected.contains('?') {
        return crate::config::matches_any(&[expected.to_string()], &actual)
            || wildcard_match(expected, &actual);
    }
    // Numeric: allow "42" == 42 and "42.0" loosely
    if let Some(n) = v.as_i64() {
        if let Ok(e) = expected.parse::<i64>() {
            return n == e;
        }
    }
    if let Some(n) = v.as_u64() {
        if let Ok(e) = expected.parse::<u64>() {
            return n == e;
        }
    }
    if let Some(n) = v.as_f64() {
        if let Ok(e) = expected.parse::<f64>() {
            return (n - e).abs() < f64::EPSILON * 8.0 || n == e;
        }
    }
    if let Some(b) = v.as_bool() {
        return match expected.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => b,
            "false" | "0" | "no" => !b,
            _ => actual == expected,
        };
    }
    actual == expected
}

fn json_to_match_string(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => String::new(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Local wildcard match so field_filter does not depend on private wildmatch.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    crate::config::matches_any(&[pattern.to_string()], text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_basic_and_equals_in_value() {
        let p = parse_field_filter("status=active").unwrap();
        assert_eq!(p.column, "status");
        assert_eq!(p.expected, "active");
        let p = parse_field_filter("note=a=b=c").unwrap();
        assert_eq!(p.column, "note");
        assert_eq!(p.expected, "a=b=c");
    }

    #[test]
    fn parse_rejects_bad() {
        assert!(parse_field_filter("").is_err());
        assert!(parse_field_filter("nocolon").is_err());
        assert!(parse_field_filter("=value").is_err());
    }

    #[test]
    fn insert_values_match() {
        let row = json!({ "values": { "name": "alice", "value": 42 } });
        let preds = parse_field_filters(&["name=alice".into(), "value=42".into()]).unwrap();
        assert!(row_matches_fields(&row, &preds));
        let preds = parse_field_filters(&["name=bob".into()]).unwrap();
        assert!(!row_matches_fields(&row, &preds));
    }

    #[test]
    fn update_matches_before_or_after() {
        let row = json!({
            "before_values": { "status": "pending", "n": 1 },
            "after_values":  { "status": "done", "n": 1 }
        });
        let pending = parse_field_filters(&["status=pending".into()]).unwrap();
        let done = parse_field_filters(&["status=done".into()]).unwrap();
        let other = parse_field_filters(&["status=other".into()]).unwrap();
        assert!(row_matches_fields(&row, &pending));
        assert!(row_matches_fields(&row, &done));
        assert!(!row_matches_fields(&row, &other));
    }

    #[test]
    fn and_requires_all() {
        let row = json!({ "values": { "a": "1", "b": "2" } });
        let preds = parse_field_filters(&["a=1".into(), "b=3".into()]).unwrap();
        assert!(!row_matches_fields(&row, &preds));
    }

    #[test]
    fn empty_preds_always_true() {
        let row = json!({ "values": { "a": 1 } });
        assert!(row_matches_fields(&row, &[]));
    }

    #[test]
    fn wildcard_value() {
        let row = json!({ "values": { "email": "user@example.com" } });
        let preds = parse_field_filters(&["email=*@example.com".into()]).unwrap();
        assert!(row_matches_fields(&row, &preds));
    }

    #[test]
    fn missing_column_fails() {
        let row = json!({ "values": { "a": 1 } });
        let preds = parse_field_filters(&["b=1".into()]).unwrap();
        assert!(!row_matches_fields(&row, &preds));
    }
}
