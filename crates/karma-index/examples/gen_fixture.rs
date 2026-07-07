//! Emit the canonical cross-read fixture: a Puffin file containing one
//! `karma-zonemap-v1` blob, written by the Rust reference impl. The independent
//! Python impl (`interop/karma_index.py`) reads this and checks it byte-for-byte
//! against `interop/fixtures/zonemap.expected.json`.
//!
//! Run from the repo root: `cargo run --example gen_fixture`.
//!
//! NOTE: `canonical()` here MUST match `zonemap.expected.json`. It is deliberately
//! duplicated (not shared) so the two sides are *independent* — the cross-read
//! checks fail loudly if they ever drift, which is exactly the guarantee we want.

use karma_index::{
    write_puffin, BlobToWrite, Bloom, BloomEntry, ColumnStats, Value, ZoneBlooms, ZoneMap, ZoneStats,
    BLOOM_BLOB_TYPE, ZONEMAP_BLOB_TYPE,
};
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../interop/fixtures")
}

fn canonical() -> (ZoneMap, Vec<i32>) {
    let zm = ZoneMap::new(vec![
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
    ]);
    (zm, vec![1, 2, 5, 6, 7])
}

fn main() {
    let (zm, fields) = canonical();
    let file = write_puffin(
        &[BlobToWrite {
            blob_type: ZONEMAP_BLOB_TYPE.into(),
            fields,
            snapshot_id: -1,
            sequence_number: -1,
            data: zm.encode(),
            properties: None,
        }],
        None,
    );

    let out = fixtures_dir().join("zonemap.rust.puffin");
    std::fs::create_dir_all(out.parent().unwrap()).expect("create fixtures dir");
    std::fs::write(&out, &file).expect("write fixture");
    println!("wrote {} ({} bytes)", out.display(), file.len());

    gen_bloom_fixture();
}

/// Build the `karma-bloom-v1` fixture from `interop/fixtures/bloom.expected.json`
/// (shared with the Python side) and write `bloom.rust.puffin`.
fn gen_bloom_fixture() {
    let text = std::fs::read_to_string(fixtures_dir().join("bloom.expected.json")).expect("read bloom.expected.json");
    let j: serde_json::Value = serde_json::from_str(&text).expect("parse bloom.expected.json");
    let bpv = j["bits_per_value"].as_u64().unwrap() as usize;

    let mut entries = Vec::new();
    let mut fields: Vec<i32> = Vec::new();
    for e in j["entries"].as_array().unwrap() {
        let zone_id = e["zone_id"].as_u64().unwrap() as u32;
        let field_id = e["field_id"].as_i64().unwrap() as i32;
        let values: Vec<Value> = e["values"].as_array().unwrap().iter().map(|v| Value::str(v.as_str().unwrap())).collect();
        if !fields.contains(&field_id) {
            fields.push(field_id);
        }
        entries.push(BloomEntry { zone_id, field_id, bloom: Bloom::build(&values, bpv) });
    }
    let zb = ZoneBlooms::new(entries);

    let file = write_puffin(
        &[BlobToWrite {
            blob_type: BLOOM_BLOB_TYPE.into(),
            fields,
            snapshot_id: -1,
            sequence_number: -1,
            data: zb.encode(),
            properties: None,
        }],
        None,
    );
    let out = fixtures_dir().join("bloom.rust.puffin");
    std::fs::write(&out, &file).expect("write bloom fixture");
    println!("wrote {} ({} bytes)", out.display(), file.len());
}
