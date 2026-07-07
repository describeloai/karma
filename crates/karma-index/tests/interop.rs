//! Reverse cross-read direction: Rust reads a Puffin file written by the
//! *independent* Python impl (`interop/karma_index.py`) and checks the decoded
//! zone map against the canonical fixture. Together with `interop/interop_check.py`
//! (Rust -> Python) this closes the loop: each engine reads the other's bytes.
//!
//! The Python fixture is produced by `python interop/interop_check.py`. If it
//! hasn't been generated yet this test skips (so `cargo test` stays green on a
//! fresh checkout); CI / the full-spectrum run generates it first.

use karma_index::{
    read_puffin, ColumnStats, Value, ZoneMap, ZoneStats, ZONEMAP_BLOB_TYPE,
};
use std::path::PathBuf;

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
    assert_eq!(meta.fields, vec![1, 2, 5, 6, 7], "blob fields");
    assert_eq!(meta.snapshot_id, -1);
    let zm = ZoneMap::decode(pf.blob_bytes(meta).unwrap()).expect("decode python-written zone map");
    assert_eq!(zm, canonical(), "Python-written zone map decoded by Rust must equal the canonical");
}
