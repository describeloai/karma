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


# ────────────────────── XXH64 (seed 0) — independent impl ─────────────────────
# Straight from the xxHash spec. Verified against the canonical vector
# XXH64("", 0) == 0xEF46DB3751D8E999. Matches Rust's twox-hash byte-for-byte.

_M64 = 0xFFFFFFFFFFFFFFFF
_P1 = 0x9E3779B185EBCA87
_P2 = 0xC2B2AE3D27D4EB4F
_P3 = 0x165667B19E3779F9
_P4 = 0x85EBCA77C2B2AE63
_P5 = 0x27D4EB2F165667C5


def _rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & _M64


def _round(acc, inp):
    acc = (acc + (inp * _P2 & _M64)) & _M64
    acc = _rotl(acc, 31)
    return (acc * _P1) & _M64


def _merge(acc, val):
    acc ^= _round(0, val)
    return ((acc * _P1) + _P4) & _M64


def xxh64(data, seed=0):
    data = bytes(data)
    n = len(data)
    i = 0
    if n >= 32:
        v1 = (seed + _P1 + _P2) & _M64
        v2 = (seed + _P2) & _M64
        v3 = seed & _M64
        v4 = (seed - _P1) & _M64
        while i + 32 <= n:
            v1 = _round(v1, int.from_bytes(data[i:i + 8], "little")); i += 8
            v2 = _round(v2, int.from_bytes(data[i:i + 8], "little")); i += 8
            v3 = _round(v3, int.from_bytes(data[i:i + 8], "little")); i += 8
            v4 = _round(v4, int.from_bytes(data[i:i + 8], "little")); i += 8
        h = (_rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18)) & _M64
        h = _merge(h, v1); h = _merge(h, v2); h = _merge(h, v3); h = _merge(h, v4)
    else:
        h = (seed + _P5) & _M64
    h = (h + n) & _M64
    while i + 8 <= n:
        h ^= _round(0, int.from_bytes(data[i:i + 8], "little"))
        h = ((_rotl(h, 27) * _P1) + _P4) & _M64
        i += 8
    if i + 4 <= n:
        h ^= (int.from_bytes(data[i:i + 4], "little") * _P1) & _M64
        h = ((_rotl(h, 23) * _P2) + _P3) & _M64
        i += 4
    while i < n:
        h ^= (data[i] * _P5) & _M64
        h = (_rotl(h, 11) * _P1) & _M64
        i += 1
    h ^= h >> 33; h = (h * _P2) & _M64
    h ^= h >> 29; h = (h * _P3) & _M64
    h ^= h >> 32
    return h


# ─────────────────── karma-bloom-v1 (split-block Bloom filter) ─────────────────

BLOOM_BLOB_TYPE = "karma-bloom-v1"
_SALT = [0x47B6137B, 0x44974D91, 0x8824AD5B, 0xA2B7289D, 0x705495C7, 0x2DF1424B, 0x9EFC4947, 0x5C6BFB31]
_M32 = 0xFFFFFFFF


class Bloom:
    def __init__(self, num_blocks):
        self.blocks = [[0] * 8 for _ in range(max(1, num_blocks))]

    def _block_index(self, h):
        return ((h >> 32) * len(self.blocks)) >> 32

    def insert_hash(self, h):
        idx = self._block_index(h)
        x = h & _M32
        for i in range(8):
            bit = ((x * _SALT[i]) & _M32) >> 27
            self.blocks[idx][i] |= 1 << bit

    def check_hash(self, h):
        idx = self._block_index(h)
        x = h & _M32
        for i in range(8):
            bit = ((x * _SALT[i]) & _M32) >> 27
            if not (self.blocks[idx][i] & (1 << bit)):
                return False
        return True

    def might_contain_str(self, s):
        return self.check_hash(xxh64(s.encode("utf-8")))

    def to_bytes(self):
        out = bytearray()
        for blk in self.blocks:
            for w in blk:
                out += struct.pack("<I", w)
        return bytes(out)

    @staticmethod
    def from_words(words):
        b = Bloom(len(words) // 8)
        for bi in range(len(b.blocks)):
            b.blocks[bi] = list(words[bi * 8:(bi + 1) * 8])
        return b


def build_bloom_str(values, bits_per_value):
    """Build an SBBF over a list of strings (Value::Bytes(utf-8))."""
    n = len(values)
    num_blocks = max(1, -(-(n * bits_per_value) // 256))  # ceil(n*bpv / 256)
    b = Bloom(num_blocks)
    for v in values:
        b.insert_hash(xxh64(v.encode("utf-8")))
    return b


def encode_zoneblooms(entries):
    """entries: list of {zone_id, field_id, bloom(Bloom)}. Returns payload bytes."""
    out = bytearray()
    out += struct.pack("<B", 1)  # version
    out += struct.pack("<I", len(entries))
    for e in entries:
        out += struct.pack("<I", e["zone_id"])
        out += struct.pack("<i", e["field_id"])
        out += struct.pack("<I", len(e["bloom"].blocks))
        out += e["bloom"].to_bytes()
    return bytes(out)


def decode_zoneblooms(payload):
    c = _Cur(payload)
    ver = c.u8()
    if ver != 1:
        raise ValueError("unsupported bloom version %d" % ver)
    entries = []
    for _ in range(c.u32()):
        zid = c.u32()
        fid = c.i32()
        nb = c.u32()
        words = [c.u32() for _ in range(nb * 8)]
        entries.append({"zone_id": zid, "field_id": fid, "bloom": Bloom.from_words(words)})
    return entries
