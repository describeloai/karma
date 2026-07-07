"""karma-index — an INDEPENDENT reference implementation, in pure-stdlib Python.

Written straight from the spec (Puffin file format + RFC-0001 `karma-zonemap-v1`),
NOT ported from the Rust crate. Its whole purpose is to be a *second engine*: if
this reads what Rust writes (and vice-versa) with byte-identical payloads, the
format is real and RFC-0001 is unambiguous. No third-party deps (no pyiceberg /
pyarrow) — just `struct` + `json`.

Value model (a Python tuple): ('null', None) | ('bool', bool) | ('i64', int) |
('f64', float) | ('bytes', bytes). Strings ride as ('bytes', <utf-8>).
"""

import json
import struct

MAGIC = b"PFA1"  # 0x50 0x46 0x41 0x31, at file start and twice in the footer.

# ─────────────────────────── Puffin container ────────────────────────────────


def write_puffin(blobs, file_properties=None):
    """blobs: list of dicts {type, fields, snapshot_id, sequence_number, data(bytes),
    compression_codec?, properties?}. Returns the full Puffin file as bytes."""
    out = bytearray()
    out += MAGIC  # head magic
    metas = []
    for b in blobs:
        offset = len(out)
        out += b["data"]
        m = {
            "type": b["type"],
            "fields": b["fields"],
            "snapshot-id": b["snapshot_id"],
            "sequence-number": b["sequence_number"],
            "offset": offset,
            "length": len(b["data"]),
        }
        if b.get("compression_codec"):
            m["compression-codec"] = b["compression_codec"]
        if b.get("properties"):
            m["properties"] = b["properties"]
        metas.append(m)
    fm = {"blobs": metas}
    if file_properties:
        fm["properties"] = file_properties
    payload = json.dumps(fm, separators=(",", ":")).encode("utf-8")
    # Footer: Magic  Payload  PayloadSize(i32 LE)  Flags(4B, uncompressed)  Magic
    out += MAGIC
    out += payload
    out += struct.pack("<i", len(payload))
    out += b"\x00\x00\x00\x00"
    out += MAGIC
    return bytes(out)


def read_puffin(data):
    """Returns (file_metadata_dict, data). Validates magics/flags/size per spec."""
    n = len(data)
    if n < 20:
        raise ValueError("truncated Puffin file")
    if data[:4] != MAGIC or data[-4:] != MAGIC:
        raise ValueError("bad magic")
    flags = data[n - 8 : n - 4]
    if flags[0] & 1:
        raise ValueError("compressed footer not supported in v1")
    size = struct.unpack("<i", data[n - 12 : n - 8])[0]
    if size < 0:
        raise ValueError("bad footer size")
    payload_end = n - 12
    payload_start = payload_end - size
    if payload_start < 4 or data[payload_start - 4 : payload_start] != MAGIC:
        raise ValueError("bad footer magic")
    fm = json.loads(data[payload_start:payload_end])
    return fm, data


def blob_bytes(data, meta):
    return data[meta["offset"] : meta["offset"] + meta["length"]]


# ───────────────────── karma-zonemap-v1 blob codec ───────────────────────────

ZONEMAP_BLOB_TYPE = "karma-zonemap-v1"
_VERSION = 1


def _enc_value(v):
    kind, val = v
    if kind == "null":
        return b"\x00"
    if kind == "bool":
        return b"\x01" + (b"\x01" if val else b"\x00")
    if kind == "i64":
        return b"\x02" + struct.pack("<q", val)
    if kind == "f64":
        return b"\x03" + struct.pack("<d", val)
    if kind == "bytes":
        return b"\x04" + struct.pack("<I", len(val)) + val
    raise ValueError("bad Value kind %r" % (kind,))


def encode_zonemap(zones):
    """zones: list of {zone_id, row_offset, row_count, columns:[{field_id, null_count,
    value_count, min, max}]} where min/max are Value tuples. Returns payload bytes."""
    out = bytearray()
    out += struct.pack("<B", _VERSION)
    out += struct.pack("<I", len(zones))
    for z in zones:
        out += struct.pack("<I", z["zone_id"])
        out += struct.pack("<Q", z["row_offset"])
        out += struct.pack("<Q", z["row_count"])
        out += struct.pack("<I", len(z["columns"]))
        for c in z["columns"]:
            out += struct.pack("<i", c["field_id"])
            out += struct.pack("<Q", c["null_count"])
            out += struct.pack("<Q", c["value_count"])
            out += _enc_value(c["min"])
            out += _enc_value(c["max"])
    return bytes(out)


class _Cur:
    def __init__(self, b):
        self.b = b
        self.p = 0

    def take(self, n):
        if self.p + n > len(self.b):
            raise ValueError("truncated zone-map payload")
        s = self.b[self.p : self.p + n]
        self.p += n
        return s

    def u8(self):
        return self.take(1)[0]

    def u32(self):
        return struct.unpack("<I", self.take(4))[0]

    def i32(self):
        return struct.unpack("<i", self.take(4))[0]

    def u64(self):
        return struct.unpack("<Q", self.take(8))[0]

    def value(self):
        tag = self.u8()
        if tag == 0:
            return ("null", None)
        if tag == 1:
            return ("bool", self.take(1)[0] != 0)
        if tag == 2:
            return ("i64", struct.unpack("<q", self.take(8))[0])
        if tag == 3:
            return ("f64", struct.unpack("<d", self.take(8))[0])
        if tag == 4:
            ln = self.u32()
            return ("bytes", bytes(self.take(ln)))
        raise ValueError("unknown Value tag %d" % tag)


def decode_zonemap(payload):
    c = _Cur(payload)
    ver = c.u8()
    if ver != _VERSION:
        raise ValueError("unsupported zone-map version %d" % ver)
    zones = []
    for _ in range(c.u32()):
        zid = c.u32()
        ro = c.u64()
        rc = c.u64()
        cols = []
        for _ in range(c.u32()):
            fid = c.i32()
            nc = c.u64()
            vc = c.u64()
            mn = c.value()
            mx = c.value()
            cols.append({"field_id": fid, "null_count": nc, "value_count": vc, "min": mn, "max": mx})
        zones.append({"zone_id": zid, "row_offset": ro, "row_count": rc, "columns": cols})
    return zones
