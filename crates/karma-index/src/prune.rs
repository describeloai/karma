//! Turn a predicate + a [`ZoneMap`] into the set of zones an engine must still
//! read. This is the payoff of the index: skip zones that *provably* cannot match.
//!
//! **The one invariant that matters:** pruning is *conservative*. A zone is skipped
//! only when the zone map **proves** no row in it can satisfy the predicate. If the
//! bounds are missing, the types are incomparable, or a float bound is `NaN`, the
//! zone **survives** (is read). A false skip would silently drop real rows — fatal
//! in a governance product — so every "unsure" answer resolves to "read it".

use std::cmp::Ordering;

use crate::bloom::ZoneBlooms;
use crate::zonemap::{Value, ZoneMap, ZoneStats};

/// A single-column comparison predicate over an Iceberg field ID.
#[derive(Clone, Debug)]
pub enum Predicate {
    Eq(i32, Value),
    Lt(i32, Value),
    LtEq(i32, Value),
    Gt(i32, Value),
    GtEq(i32, Value),
}

impl Predicate {
    pub fn field(&self) -> i32 {
        match self {
            Predicate::Eq(f, _)
            | Predicate::Lt(f, _)
            | Predicate::LtEq(f, _)
            | Predicate::Gt(f, _)
            | Predicate::GtEq(f, _) => *f,
        }
    }
}

/// Total-ish ordering of two bounds. `None` = "cannot be compared" (different
/// variants, a `Null` bound, or a `NaN` float) → callers treat `None` as
/// "don't skip".
fn cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::I64(x), Value::I64(y)) => Some(x.cmp(y)),
        (Value::F64(x), Value::F64(y)) => x.partial_cmp(y), // NaN → None
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)), // byte order == UTF-8 code-point order
        _ => None,
    }
}

/// Can this zone be skipped for this predicate? Only `true` when *proven*.
pub fn can_skip(zone: &ZoneStats, pred: &Predicate) -> bool {
    let col = match zone.column(pred.field()) {
        Some(c) => c,
        None => return false, // no stats for this column → cannot prove anything
    };
    // A zone with no non-null values can never satisfy a value comparison
    // (SQL three-valued logic: `x <op> v` is never TRUE when x IS NULL).
    if col.value_count == 0 {
        return true;
    }
    let (lo, hi) = (&col.min, &col.max);
    match pred {
        // v < lo  OR  v > hi  → no equal value in [lo, hi]
        Predicate::Eq(_, v) => {
            cmp(v, lo) == Some(Ordering::Less) || cmp(v, hi) == Some(Ordering::Greater)
        }
        // col <  v : matchable iff lo <  v  → skip iff lo >= v
        Predicate::Lt(_, v) => matches!(cmp(lo, v), Some(Ordering::Greater | Ordering::Equal)),
        // col <= v : matchable iff lo <= v  → skip iff lo >  v
        Predicate::LtEq(_, v) => cmp(lo, v) == Some(Ordering::Greater),
        // col >  v : matchable iff hi >  v  → skip iff hi <= v
        Predicate::Gt(_, v) => matches!(cmp(hi, v), Some(Ordering::Less | Ordering::Equal)),
        // col >= v : matchable iff hi >= v  → skip iff hi <  v
        Predicate::GtEq(_, v) => cmp(hi, v) == Some(Ordering::Less),
    }
}

/// The `zone_id`s that survive pruning (must be read). The complement of these is
/// safe to skip.
pub fn surviving_zones(zm: &ZoneMap, pred: &Predicate) -> Vec<u32> {
    zm.zones
        .iter()
        .filter(|z| !can_skip(z, pred))
        .map(|z| z.zone_id)
        .collect()
}

/// Like [`surviving_zones`], but also consults a [`ZoneBlooms`] index: for an
/// equality predicate, a zone is *additionally* skipped when its Bloom proves the
/// value absent. Blooms only tighten pruning — a missing Bloom leaves the zone
/// map's verdict untouched, and a Bloom never keeps a zone the zone map skipped.
pub fn surviving_zones_indexed(zm: &ZoneMap, blooms: Option<&ZoneBlooms>, pred: &Predicate) -> Vec<u32> {
    zm.zones
        .iter()
        .filter(|z| {
            if can_skip(z, pred) {
                return false;
            }
            if let (Predicate::Eq(field, v), Some(bl)) = (pred, blooms) {
                if !bl.might_contain(z.zone_id, *field, v) {
                    return false; // Bloom proves the value is absent from this zone
                }
            }
            true
        })
        .map(|z| z.zone_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zonemap::{ColumnStats, ZoneStats};

    // zone 0: col1 ∈ [0,10], col2(str) ∈ ["alpha","mid"] ; zone 1: col1 ∈ [20,30]
    fn zm() -> ZoneMap {
        ZoneMap::new(vec![
            ZoneStats {
                zone_id: 0,
                row_offset: 0,
                row_count: 100,
                columns: vec![
                    ColumnStats { field_id: 1, min: Value::I64(0), max: Value::I64(10), null_count: 0, value_count: 100 },
                    ColumnStats { field_id: 2, min: Value::str("alpha"), max: Value::str("mid"), null_count: 0, value_count: 100 },
                ],
            },
            ZoneStats {
                zone_id: 1,
                row_offset: 100,
                row_count: 50,
                columns: vec![ColumnStats { field_id: 1, min: Value::I64(20), max: Value::I64(30), null_count: 0, value_count: 50 }],
            },
        ])
    }

    #[test]
    fn gt_prunes_low_zone() {
        assert_eq!(surviving_zones(&zm(), &Predicate::Gt(1, Value::I64(15))), vec![1]);
        assert_eq!(surviving_zones(&zm(), &Predicate::Gt(1, Value::I64(5))), vec![0, 1]);
    }

    #[test]
    fn eq_hits_only_containing_zone() {
        assert_eq!(surviving_zones(&zm(), &Predicate::Eq(1, Value::I64(25))), vec![1]);
        assert_eq!(surviving_zones(&zm(), &Predicate::Eq(1, Value::I64(15))), Vec::<u32>::new());
        assert_eq!(surviving_zones(&zm(), &Predicate::Eq(1, Value::I64(0))), vec![0]); // boundary
    }

    #[test]
    fn lt_and_lteq_boundaries() {
        assert_eq!(surviving_zones(&zm(), &Predicate::Lt(1, Value::I64(0))), Vec::<u32>::new());
        assert_eq!(surviving_zones(&zm(), &Predicate::Lt(1, Value::I64(5))), vec![0]);
        assert_eq!(surviving_zones(&zm(), &Predicate::LtEq(1, Value::I64(0))), vec![0]);
    }

    #[test]
    fn string_bounds_prune() {
        // "zzz" > zone0's max "mid" → zone0 pruned. zone1 has no col-2 stats → it
        // survives (conservative), so the result is [1], NOT [].
        assert_eq!(surviving_zones(&zm(), &Predicate::Eq(2, Value::str("zzz"))), vec![1]);
        // "beta" ∈ ["alpha","mid"] → zone0 survives; zone1 conservative → [0, 1].
        assert_eq!(surviving_zones(&zm(), &Predicate::Eq(2, Value::str("beta"))), vec![0, 1]);
    }

    #[test]
    fn missing_column_is_conservative() {
        // zone 1 has no stats for field 2 → it must survive (never skip on ignorance).
        let survivors = surviving_zones(&zm(), &Predicate::Eq(2, Value::str("zzz")));
        assert!(survivors.contains(&1));
    }

    #[test]
    fn all_null_zone_is_skipped() {
        let zm = ZoneMap::new(vec![ZoneStats {
            zone_id: 7,
            row_offset: 0,
            row_count: 10,
            columns: vec![ColumnStats { field_id: 1, min: Value::Null, max: Value::Null, null_count: 10, value_count: 0 }],
        }]);
        assert_eq!(surviving_zones(&zm, &Predicate::Eq(1, Value::I64(1))), Vec::<u32>::new());
    }
}
