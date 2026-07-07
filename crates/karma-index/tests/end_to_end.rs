//! End-to-end: build a zone map, store it as a Puffin `karma-zonemap-v1` blob,
//! read the file back, decode the blob, and prune with a predicate — the full
//! "write an index, use it to skip work" loop that the format exists for.

use karma_index::{
    read_puffin, surviving_zones, write_puffin, BlobToWrite, ColumnStats, ColumnTypes, IcebergType, Predicate,
    Value, ZoneMap, ZoneStats, ZONEMAP_BLOB_TYPE,
};

fn build_zone_map() -> ZoneMap {
    ZoneMap::new(vec![
        ZoneStats {
            zone_id: 0,
            row_offset: 0,
            row_count: 1024,
            columns: vec![ColumnStats {
                field_id: 42,
                min: Value::I64(0),
                max: Value::I64(999),
                null_count: 0,
                value_count: 1024,
            }],
        },
        ZoneStats {
            zone_id: 1,
            row_offset: 1024,
            row_count: 1024,
            columns: vec![ColumnStats {
                field_id: 42,
                min: Value::I64(5000),
                max: Value::I64(6000),
                null_count: 0,
                value_count: 1024,
            }],
        },
    ])
}

#[test]
fn write_read_prune_full_loop() {
    let zm = build_zone_map();
    let types = ColumnTypes::from([(42, IcebergType::Long)]);

    // Store the zone map as a Puffin blob (as it would sit beside an Iceberg data file).
    let file = write_puffin(
        &[BlobToWrite {
            blob_type: ZONEMAP_BLOB_TYPE.into(),
            fields: vec![42],
            snapshot_id: -1,
            sequence_number: -1,
            data: zm.encode(&types),
            properties: None,
        }],
        None,
    );

    // Read it back generically (as any Puffin reader would).
    let pf = read_puffin(&file).unwrap();
    let meta = pf.first_of_type(ZONEMAP_BLOB_TYPE).expect("zone-map blob present");
    assert_eq!(meta.fields, vec![42]);
    let decoded = ZoneMap::decode(pf.blob_bytes(meta).unwrap(), &types).unwrap();
    assert_eq!(decoded, zm);

    // Use it: `WHERE field42 > 4000` can only be in zone 1 — zone 0 is skipped.
    assert_eq!(surviving_zones(&decoded, &Predicate::Gt(42, Value::I64(4000))), vec![1]);
    // `WHERE field42 = 500` can only be in zone 0.
    assert_eq!(surviving_zones(&decoded, &Predicate::Eq(42, Value::I64(500))), vec![0]);
    // `WHERE field42 = 4500` is in neither zone — everything skipped.
    assert_eq!(
        surviving_zones(&decoded, &Predicate::Eq(42, Value::I64(4500))),
        Vec::<u32>::new()
    );
}
