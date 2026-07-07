//! Shared **`Expr` → karma-index predicate** translation.
//!
//! Every karma `TableProvider` — the in-memory [`crate::KarmaZoneTable`] and the
//! Parquet-backed `ParquetZoneTable` (crate `karma-parquet`) — prunes with the
//! *same* logic: it must translate DataFusion filters into [`Predicate`]s and IN-lists
//! identically, or the two providers would drift in what they skip (and a governance
//! product cannot have "which engine read it" change the rows). This module is that
//! single source of truth.
//!
//! Translation is deliberately *lossy-safe*: anything we cannot turn into a prunable
//! constraint is simply dropped, and DataFusion re-applies the real filter on the
//! surviving rows (we report [`Inexact`](TableProviderFilterPushDown::Inexact)). So a
//! translation gap can only make us read *more*, never return wrong rows.

use std::collections::{HashMap, HashSet};

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};

use karma_index::{surviving_zones_indexed, Predicate, Value, ZoneBlooms, ZoneMap};

/// Translate a DataFusion literal into a karma-index [`Value`].
///
/// Exact by construction: decimals keep their unscaled integer + scale (never widen
/// to `F64`), and temporals normalize to the zone-map's canonical units (`Date` =
/// days, `Time`/`Timestamp` = microseconds). A unit conversion that would overflow
/// `i64` yields `None` → the predicate is dropped → conservative (never a bad skip).
pub fn scalar_to_value(s: &ScalarValue) -> Option<Value> {
    Some(match s {
        ScalarValue::Int64(Some(v)) => Value::I64(*v),
        ScalarValue::Int32(Some(v)) => Value::I64(*v as i64),
        ScalarValue::Float64(Some(v)) => Value::F64(*v),
        ScalarValue::Float32(Some(v)) => Value::F64(*v as f64),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Value::Bytes(v.clone().into_bytes()),
        ScalarValue::Boolean(Some(v)) => Value::Bool(*v),
        ScalarValue::Decimal128(Some(v), _p, scale) => Value::Decimal { unscaled: *v, scale: *scale as i32 },
        ScalarValue::Date32(Some(d)) => Value::Date(*d),
        ScalarValue::Date64(Some(ms)) => Value::Date((*ms / 86_400_000) as i32),
        ScalarValue::TimestampSecond(Some(t), _tz) => Value::Timestamp(t.checked_mul(1_000_000)?),
        ScalarValue::TimestampMillisecond(Some(t), _tz) => Value::Timestamp(t.checked_mul(1_000)?),
        ScalarValue::TimestampMicrosecond(Some(t), _tz) => Value::Timestamp(*t),
        ScalarValue::TimestampNanosecond(Some(t), _tz) => Value::Timestamp(t.div_euclid(1_000)),
        ScalarValue::Time32Second(Some(t)) => Value::Time((*t as i64).checked_mul(1_000_000)?),
        ScalarValue::Time32Millisecond(Some(t)) => Value::Time((*t as i64).checked_mul(1_000)?),
        ScalarValue::Time64Microsecond(Some(t)) => Value::Time(*t),
        ScalarValue::Time64Nanosecond(Some(t)) => Value::Time(t.div_euclid(1_000)),
        _ => return None,
    })
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn make_predicate(field_id: i32, op: Operator, v: Value) -> Option<Predicate> {
    Some(match op {
        Operator::Eq => Predicate::Eq(field_id, v),
        Operator::Lt => Predicate::Lt(field_id, v),
        Operator::LtEq => Predicate::LtEq(field_id, v),
        Operator::Gt => Predicate::Gt(field_id, v),
        Operator::GtEq => Predicate::GtEq(field_id, v),
        _ => return None, // NotEq / others: a zone map can't prune these
    })
}

/// Flatten one filter into prunable constraints: comparison predicates (splitting
/// top-level `AND`s and normalizing `col <op> literal` / `literal <op> col`) and
/// `col IN (literal…)` lists. Anything else is left for DataFusion's re-check.
fn collect(
    field_ids: &HashMap<String, i32>,
    expr: &Expr,
    preds: &mut Vec<Predicate>,
    inlists: &mut Vec<(i32, Vec<Value>)>,
) {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if *op == Operator::And {
                collect(field_ids, left, preds, inlists);
                collect(field_ids, right, preds, inlists);
                return;
            }
            // Normalize to `column <op> value`, flipping if the literal is on the
            // left (`5 < x` ≡ `x > 5`).
            let (col, op, scalar): (&Column, Operator, &ScalarValue) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(s, _)) => (c, *op, s),
                (Expr::Literal(s, _), Expr::Column(c)) => (c, flip_op(*op), s),
                _ => return,
            };
            if let (Some(&fid), Some(value)) = (field_ids.get(col.name.as_str()), scalar_to_value(scalar)) {
                if let Some(p) = make_predicate(fid, op, value) {
                    preds.push(p);
                }
            }
        }
        Expr::InList(il) if !il.negated => {
            if let Expr::Column(c) = il.expr.as_ref() {
                if let Some(&fid) = field_ids.get(c.name.as_str()) {
                    let vals: Vec<Value> = il
                        .list
                        .iter()
                        .filter_map(|e| match e {
                            Expr::Literal(s, _) => scalar_to_value(s),
                            _ => None,
                        })
                        .collect();
                    if !vals.is_empty() && vals.len() == il.list.len() {
                        inlists.push((fid, vals));
                    }
                }
            }
        }
        _ => {}
    }
}

/// The **indices** (positions in `zone_map.zones`) of the zones that survive every
/// filter — the zones a scan must still read. Comparison predicates are ANDed (a
/// zone must survive each); an `IN (v₁…vₙ)` survives if it survives `= vⱼ` for some
/// `j` (OR). Untranslatable filters are ignored here (DataFusion re-checks the rows).
pub fn surviving_zone_indices(
    zone_map: &ZoneMap,
    blooms: Option<&ZoneBlooms>,
    field_ids: &HashMap<String, i32>,
    filters: &[Expr],
) -> Vec<usize> {
    let mut preds = Vec::new();
    let mut inlists: Vec<(i32, Vec<Value>)> = Vec::new();
    for f in filters {
        collect(field_ids, f, &mut preds, &mut inlists);
    }
    let zone_id = |i: usize| zone_map.zones[i].zone_id;
    let mut alive: Vec<usize> = (0..zone_map.zones.len()).collect();

    for p in &preds {
        let surviving: HashSet<u32> = surviving_zones_indexed(zone_map, blooms, p).into_iter().collect();
        alive.retain(|&i| surviving.contains(&zone_id(i)));
    }
    for (field, vals) in &inlists {
        let mut surviving: HashSet<u32> = HashSet::new();
        for v in vals {
            let pred = Predicate::Eq(*field, v.clone());
            surviving.extend(surviving_zones_indexed(zone_map, blooms, &pred));
        }
        alive.retain(|&i| surviving.contains(&zone_id(i)));
    }
    alive
}

/// Pushdown verdict per filter for `TableProvider::supports_filters_pushdown`:
/// `Inexact` where we can extract at least one prunable constraint (we prune,
/// DataFusion re-checks the rows); `Unsupported` otherwise.
pub fn filters_pushdown(
    field_ids: &HashMap<String, i32>,
    filters: &[&Expr],
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|f| {
            let (mut preds, mut inlists) = (Vec::new(), Vec::new());
            collect(field_ids, f, &mut preds, &mut inlists);
            if preds.is_empty() && inlists.is_empty() {
                TableProviderFilterPushDown::Unsupported
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}
