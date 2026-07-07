"""Full-spectrum cross-read check.

Direction Rust -> Python:
  * read the Rust-written Puffin generically (as any Puffin reader would),
  * confirm the blob directory (type / fields / snapshot-id),
  * decode the `karma-zonemap-v1` payload and check it against the canonical
    `zonemap.expected.json`,
  * assert the Rust payload bytes are BYTE-IDENTICAL to this independent Python
    impl's own encoding of the same canonical (proves the spec is unambiguous).

It also writes `zonemap.python.puffin` so the Rust side can do the reverse
direction (Python -> Rust) in `cargo test --test interop`.

Pure stdlib. Exits non-zero on any mismatch.
"""

import json
import sys
from pathlib import Path

import karma_index as ki

HERE = Path(__file__).parent
FIX = HERE / "fixtures"


def value_from_json(obj):
    (kind, val), = obj.items()
    if kind == "i64":
        return ("i64", int(val))
    if kind == "f64":
        return ("f64", float(val))
    if kind == "str":
        return ("bytes", val.encode("utf-8"))
    if kind == "bool":
        return ("bool", bool(val))
    if kind == "null":
        return ("null", None)
    raise ValueError("bad value tag %r" % (kind,))


def load_canonical():
    exp = json.loads((FIX / "zonemap.expected.json").read_text(encoding="utf-8"))
    zones = []
    for z in exp["zones"]:
        cols = [
            {
                "field_id": c["field_id"],
                "null_count": c["null_count"],
                "value_count": c["value_count"],
                "min": value_from_json(c["min"]),
                "max": value_from_json(c["max"]),
            }
            for c in z["columns"]
        ]
        zones.append({"zone_id": z["zone_id"], "row_offset": z["row_offset"], "row_count": z["row_count"], "columns": cols})
    return exp["blob"], zones


def main():
    blob, zones = load_canonical()
    payload = ki.encode_zonemap(zones)

    # Emit the Python-written fixture for the reverse (Python -> Rust) direction.
    py_file = ki.write_puffin(
        [{
            "type": ki.ZONEMAP_BLOB_TYPE,
            "fields": blob["fields"],
            "snapshot_id": blob["snapshot_id"],
            "sequence_number": blob["sequence_number"],
            "data": payload,
        }]
    )
    (FIX / "zonemap.python.puffin").write_bytes(py_file)

    # Rust -> Python.
    rust_path = FIX / "zonemap.rust.puffin"
    if not rust_path.exists():
        print("FAIL: %s missing — run `cargo run --example gen_fixture` first" % rust_path, file=sys.stderr)
        return 1
    rust = rust_path.read_bytes()

    fm, data = ki.read_puffin(rust)
    assert len(fm["blobs"]) == 1, "expected exactly one blob, got %d" % len(fm["blobs"])
    m = fm["blobs"][0]
    assert m["type"] == ki.ZONEMAP_BLOB_TYPE, m["type"]
    assert m["fields"] == blob["fields"], m["fields"]
    assert m["snapshot-id"] == blob["snapshot_id"], m["snapshot-id"]
    assert m["sequence-number"] == blob["sequence_number"], m["sequence-number"]

    rust_payload = ki.blob_bytes(data, m)
    decoded = ki.decode_zonemap(rust_payload)
    assert decoded == zones, "decoded zone-map differs from canonical:\n  got %r\n  exp %r" % (decoded, zones)

    # The strong claim: two independent impls produce the SAME payload bytes.
    assert rust_payload == payload, (
        "payload bytes differ between Rust and Python — the format is ambiguous!\n"
        "  rust[%d]  py[%d]" % (len(rust_payload), len(payload))
    )

    # Container sanity: standard Puffin, so any Puffin reader can find our blobs.
    assert rust[:4] == ki.MAGIC and rust[-4:] == ki.MAGIC

    print("OK  Rust -> Python cross-read")
    print("    blob type=%s fields=%s" % (m["type"], m["fields"]))
    print("    zones=%d, payload=%d bytes — BYTE-IDENTICAL across Rust & Python" % (len(decoded), len(payload)))
    print("    wrote %s (%d bytes) for the reverse Python -> Rust check" % (FIX / "zonemap.python.puffin", len(py_file)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
