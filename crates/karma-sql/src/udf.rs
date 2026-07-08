//! Our function registry — the Databricks/Spark functions DataFusion lacks natively,
//! registered on the session so the SQL parser binds them. This is the "own the
//! vocabulary" seam of the front: Carbon's Postgres dialect shim
//! (`lib/warehouse/query/dialect.ts`, *to be deleted* when the engine lands) rewrote
//! these to Postgres; here they become **real functions the engine speaks natively**.
//!
//! Two first-cut proofs of the registry path (Block 2 §4.6, spec Build 4 task 4):
//! - `GET_JSON_OBJECT(json, '$.path')` — a bespoke [`ScalarUDF`] (DataFusion has no
//!   equivalent), mirroring the Spark JSONPath subset the shim supported.
//! - `APPROX_COUNT_DISTINCT(x)` — the Spark spelling, registered as an **alias** of
//!   DataFusion's native `approx_distinct` aggregate (no reimplementation).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_string_array;
use datafusion::error::Result as DFResult;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register every Karma-parity function on `ctx`. Idempotent for the alias (re-registers
/// the native aggregate under the Spark spelling).
pub fn register_karma_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(GetJsonObject::new()));

    // `approx_count_distinct` (Spark) = `approx_distinct` (DataFusion). Add the alias to
    // a clone of the built-in UDAF and re-register; `register_udaf` also indexes aliases.
    let approx = (*approx_distinct_udaf()).clone().with_aliases(["approx_count_distinct"]);
    ctx.register_udaf(approx);
}

/// `GET_JSON_OBJECT(json_text, '$.path')` → the extracted field as text. Spark
/// semantics: a string leaf returns unquoted; objects/arrays return their compact JSON;
/// an absent path, a JSON `null`, or unparseable input all return SQL `NULL`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetJsonObject {
    signature: Signature,
}

impl GetJsonObject {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl Default for GetJsonObject {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for GetJsonObject {
    fn name(&self) -> &str {
        "get_json_object"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let json = args.args[0].clone().into_array(n)?;
        let path = args.args[1].clone().into_array(n)?;
        let json = as_string_array(&json)?;
        let path = as_string_array(&path)?;
        let mut out = StringBuilder::new();
        for i in 0..n {
            if json.is_null(i) || path.is_null(i) {
                out.append_null();
                continue;
            }
            match extract_json_path(json.value(i), path.value(i)) {
                Some(s) => out.append_value(s),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Extract a Spark JSONPath (`$`, `.key`, `[n]`, `['key']`, `["key"]`) from a JSON
/// document. Returns `None` for a missing path, a JSON `null` leaf, or invalid JSON —
/// mapped to SQL `NULL` by the caller. Mirrors the subset in Carbon's
/// `sparkJsonPathToPgPath` (the shim being retired).
fn extract_json_path(json: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let mut rest = path.strip_prefix('$')?;
    let mut cur = &value;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('.') {
            // `.key` — an identifier up to the next `.` or `[`.
            let end = after.find(['.', '[']).unwrap_or(after.len());
            let key = &after[..end];
            if key.is_empty() {
                return None;
            }
            cur = cur.get(key)?;
            rest = &after[end..];
        } else if let Some(after) = rest.strip_prefix('[') {
            // `[n]` or `['key']` / `["key"]`.
            let close = after.find(']')?;
            let inner = after[..close].trim();
            let quoted = (inner.starts_with('\'') && inner.ends_with('\'') && inner.len() >= 2)
                || (inner.starts_with('"') && inner.ends_with('"') && inner.len() >= 2);
            cur = if quoted {
                cur.get(&inner[1..inner.len() - 1])?
            } else {
                cur.get(inner.parse::<usize>().ok()?)?
            };
            rest = &after[close + 1..];
        } else {
            return None;
        }
    }
    match cur {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::extract_json_path;

    #[test]
    fn json_path_subset() {
        let doc = r#"{"sensor":{"id":"A12","bat":0.83},"tags":["x","y"],"n":7}"#;
        assert_eq!(extract_json_path(doc, "$.sensor.id").as_deref(), Some("A12"));
        assert_eq!(extract_json_path(doc, "$.sensor.bat").as_deref(), Some("0.83"));
        assert_eq!(extract_json_path(doc, "$.tags[0]").as_deref(), Some("x"));
        assert_eq!(extract_json_path(doc, "$.tags[1]").as_deref(), Some("y"));
        assert_eq!(extract_json_path(doc, "$['sensor']['id']").as_deref(), Some("A12"));
        assert_eq!(extract_json_path(doc, "$.n").as_deref(), Some("7"));
        // object leaf → compact JSON (keys in insertion order — serde_json preserve_order)
        assert_eq!(extract_json_path(doc, "$.sensor").as_deref(), Some(r#"{"id":"A12","bat":0.83}"#));
        // misses → None
        assert_eq!(extract_json_path(doc, "$.missing"), None);
        assert_eq!(extract_json_path(doc, "$.tags[9]"), None);
        assert_eq!(extract_json_path("not json", "$.a"), None);
    }
}
