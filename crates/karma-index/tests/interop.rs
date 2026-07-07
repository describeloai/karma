//! Reverse cross-read direction: Rust reads a Puffin file written by the
//! *independent* Python impl (`interop/karma_index.py`) and checks the decoded
//! zone map against the canonical fixture. Together with `interop/interop_check.py`
//! (Rust -> Python) this closes the loop: each engine reads the other's bytes.
//!
//! The Python fixture is produced by `python interop/interop_check.py`. If it
//! hasn't been generated yet this test skips (so `cargo test` stays green on a
//! fresh checkout); CI / the full-spectrum run generates it first.

use karma_index::{
    read_puffin, ColumnStats, Value, ZoneBlooms, ZoneMap, ZoneStats, BLOOM_BLOB_TYPE, ZONEMAP_BLOB_TYPE,
};
use std::path::{Path, PathBuf};

// MUST match interop/fixtures/zonemap.expected.json (independently — drift is
// caught by the cross-read assertions on both sides).
fn canonical() -> ZoneMap {
    ZoneMap::new(vec![
        ZoneStats {
            zone_id: 0,
            row_offset: 0,
            row_count: 100,
            columns: vec![
                ColumnStats { field_id: 1, min: Value::I64(0), max: Value::I64(10), null_count: 3, value_count: 97 },
                ColumnStats { field_id: 2, min: Value::str("alpha"), max: Value::str("mid"), null_count: 0, value_count: 100 },
                ColumnStats { field_id: 5, min: Value::F64(-1.5), max: Value::F64(3.25), null_count: 0, value_count: 100 },
                ColumnStats { field_id: 6, min: Value::Null, max: Value::Null, null_count: 100, value_count: 0 },
                ColumnStats { field_id: 7, min: Value::Bool(false), max: Value::Bool(true), null_count: 0, value_count: 100 },
            ],
        },
        ZoneStats {
            zone_id: 1,
            row_offset: 100,
            row_count: 50,
            columns: vec![ColumnStats { field_id: 1, min: Value::I64(20), max: Value::I64(30), null_count: 0, value_count: 50 }],
        },
        ZoneStats {
            zone_id: 2,
            row_offset: 150,
            row_count: 25,
            columns: vec![
                ColumnStats { field_id: 10, min: Value::Decimal { unscaled: -10050, scale: 2 }, max: Value::Decimal { unscaled: 999_999, scale: 2 }, null_count: 0, value_count: 25 },
                ColumnStats { field_id: 11, min: Value::Date(-1), max: Value::Date(19_723), null_count: 0, value_count: 25 },
                ColumnStats { field_id: 12, min: Value::Time(0), max: Value::Time(86_399_999_999), null_count: 0, value_count: 25 },
                ColumnStats { field_id: 13, min: Value::Timestamp(1_672_531_200_000_000), max: Value::Timestamp(1_704_067_200_000_000), null_count: 0, value_count: 25 },
            ],
        },
    ])
}

#[test]
fn rust_reads_python_written_puffin() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../interop/fixtures/zonemap.python.puffin");
    if !path.exists() {
        eprintln!("skip: Python fixture not generated yet — run `python interop/interop_check.py` ({})", path.display());
        return;
    }
    let bytes = std::fs::read(&path).expect("read python fixture");
    let pf = read_puffin(&bytes).expect("valid Puffin from Python");
    let meta = pf.first_of_type(ZONEMAP_BLOB_TYPE).expect("karma-zonemap-v1 blob present");
    assert_eq!(meta.fields, vec![1, 2, 5, 6, 7, 10, 11, 12, 13], "blob fields");
    assert_eq!(meta.snapshot_id, -1);
    let zm = ZoneMap::decode(pf.blob_bytes(meta).unwrap()).expect("decode python-written zone map");
    assert_eq!(zm, canonical(), "Python-written zone map decoded by Rust must equal the canonical");
}

#[test]
fn rust_reads_python_written_bloom() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../interop/fixtures");
    let (py, rust) = (dir.join("bloom.python.puffin"), dir.join("bloom.rust.puffin"));
    if !py.exists() || !rust.exists() {
        eprintln!("skip: bloom fixtures not generated — run gen_fixture + interop_check.py");
        return;
    }
    let decode = |p: &Path| -> ZoneBlooms {
        let bytes = std::fs::read(p).unwrap();
        let pf = read_puffin(&bytes).unwrap();
        let m = pf.first_of_type(BLOOM_BLOB_TYPE).expect("karma-bloom-v1 blob present");
        ZoneBlooms::decode(pf.blob_bytes(m).unwrap()).expect("decode bloom")
    };
    // Rust decodes the Python-written bloom, and it is structurally identical to
    // the one Rust wrote (both built from the same fixture).
    let (zb_py, zb_rust) = (decode(&py), decode(&rust));
    assert_eq!(zb_py, zb_rust, "Python-written bloom must decode identically to Rust's");
    // No false negatives on known-present values (an absent-value assertion would
    // be flaky — a Bloom may report a false positive).
    assert!(zb_py.might_contain(0, 3, &Value::str("u-05")));
    assert!(zb_py.might_contain(1, 3, &Value::str("v-07")));
}
