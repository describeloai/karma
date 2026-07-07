//! A minimal, spec-compliant reader/writer for the **Puffin** file format —
//! Iceberg's container for statistics/index blobs.
//!
//! Layout (per the Iceberg Puffin spec):
//! ```text
//!   Magic  Blob₁ Blob₂ … Blobₙ  Footer
//!   Footer := Magic  FooterPayload(JSON)  FooterPayloadSize(i32 LE)  Flags(4B)  Magic
//! ```
//! `Magic` = `PFA1` (`0x50 0x46 0x41 0x31`). Each blob's bytes live in the body;
//! the footer JSON (`FileMetadata`) records every blob's `type`, `fields`,
//! `offset`, `length`, etc. so a reader can locate blobs without parsing them.
//!
//! v1 scope: **uncompressed** footer (`Flags = 0`) and uncompressed blob payloads
//! (`compression-codec` omitted). Any other Puffin reader (pyiceberg, iceberg-rust)
//! can still list `karma-*` blobs from a file we write — that cross-readability is
//! the whole point of riding Puffin instead of inventing a container.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Puffin magic: the four bytes `PFA1`, present at the file start and twice in the footer.
pub const MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];

#[derive(Debug, thiserror::Error)]
pub enum PuffinError {
    #[error("not a Puffin file: bad magic (expected PFA1)")]
    BadMagic,
    #[error("truncated Puffin file")]
    Truncated,
    #[error("footer payload is compressed; not supported in v1")]
    CompressedFooter,
    #[error("invalid footer JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("blob bytes out of bounds")]
    BlobOutOfBounds,
}

/// A blob's entry in the footer's `FileMetadata.blobs`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BlobMetadata {
    #[serde(rename = "type")]
    pub blob_type: String,
    /// Iceberg field IDs this blob is computed over.
    pub fields: Vec<i32>,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    pub offset: i64,
    pub length: i64,
    #[serde(rename = "compression-codec", skip_serializing_if = "Option::is_none")]
    pub compression_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Default)]
struct FileMetadata {
    blobs: Vec<BlobMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<BTreeMap<String, String>>,
}

/// A blob to append to a Puffin file (payload already serialized by its own codec).
#[derive(Clone, Debug)]
pub struct BlobToWrite {
    pub blob_type: String,
    pub fields: Vec<i32>,
    /// Snapshot / sequence this blob is bound to. A standalone index not yet tied
    /// to a snapshot uses `-1` (mirrors the spec's `deletion-vector-v1`).
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub data: Vec<u8>,
    pub properties: Option<BTreeMap<String, String>>,
}

/// Serialize a complete Puffin file from a set of blobs.
pub fn write_puffin(blobs: &[BlobToWrite], file_properties: Option<BTreeMap<String, String>>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC); // head magic

    let mut metas = Vec::with_capacity(blobs.len());
    for b in blobs {
        let offset = out.len() as i64;
        out.extend_from_slice(&b.data);
        metas.push(BlobMetadata {
            blob_type: b.blob_type.clone(),
            fields: b.fields.clone(),
            snapshot_id: b.snapshot_id,
            sequence_number: b.sequence_number,
            offset,
            length: b.data.len() as i64,
            compression_codec: None,
            properties: b.properties.clone(),
        });
    }

    let payload = serde_json::to_vec(&FileMetadata { blobs: metas, properties: file_properties })
        .expect("FileMetadata is always serializable");

    // Footer: Magic  Payload  PayloadSize(i32 LE)  Flags(4B, uncompressed)  Magic
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&(payload.len() as i32).to_le_bytes());
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&MAGIC);
    out
}

/// A parsed Puffin file: the blob directory plus a borrow of the raw bytes so
/// blob payloads can be sliced out on demand.
pub struct PuffinFile<'a> {
    data: &'a [u8],
    pub blobs: Vec<BlobMetadata>,
    pub properties: BTreeMap<String, String>,
}

/// Parse a Puffin file: validate the magics, read the trailing footer, and decode
/// the blob directory. Does NOT decode blob payloads (that's each codec's job).
pub fn read_puffin(data: &[u8]) -> Result<PuffinFile<'_>, PuffinError> {
    let n = data.len();
    // Smallest legal file: head magic (4) + footer[ magic(4) + payload + size(4) + flags(4) + magic(4) ].
    if n < 20 {
        return Err(PuffinError::Truncated);
    }
    if data[..4] != MAGIC || data[n - 4..] != MAGIC {
        return Err(PuffinError::BadMagic);
    }
    let flags = &data[n - 8..n - 4];
    if flags[0] & 1 == 1 {
        return Err(PuffinError::CompressedFooter);
    }
    let size = i32::from_le_bytes([data[n - 12], data[n - 11], data[n - 10], data[n - 9]]);
    if size < 0 {
        return Err(PuffinError::Truncated);
    }
    let payload_end = n - 12;
    let payload_start = payload_end.checked_sub(size as usize).ok_or(PuffinError::Truncated)?;
    let magic_start = payload_start.checked_sub(4).ok_or(PuffinError::Truncated)?;
    if data[magic_start..payload_start] != MAGIC {
        return Err(PuffinError::BadMagic);
    }
    let fm: FileMetadata = serde_json::from_slice(&data[payload_start..payload_end])?;
    Ok(PuffinFile {
        data,
        blobs: fm.blobs,
        properties: fm.properties.unwrap_or_default(),
    })
}

impl<'a> PuffinFile<'a> {
    /// The raw payload bytes of a blob, bounds-checked against the file.
    pub fn blob_bytes(&self, m: &BlobMetadata) -> Result<&'a [u8], PuffinError> {
        if m.offset < 0 || m.length < 0 {
            return Err(PuffinError::BlobOutOfBounds);
        }
        let start = m.offset as usize;
        let end = start.checked_add(m.length as usize).ok_or(PuffinError::BlobOutOfBounds)?;
        self.data.get(start..end).ok_or(PuffinError::BlobOutOfBounds)
    }

    /// Find the first blob of a given type.
    pub fn first_of_type(&self, blob_type: &str) -> Option<&BlobMetadata> {
        self.blobs.iter().find(|b| b.blob_type == blob_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_two_blobs() {
        let blobs = vec![
            BlobToWrite {
                blob_type: "karma-zonemap-v1".into(),
                fields: vec![1, 2],
                snapshot_id: -1,
                sequence_number: -1,
                data: b"first-blob-bytes".to_vec(),
                properties: None,
            },
            BlobToWrite {
                blob_type: "karma-bloom-v1".into(),
                fields: vec![3],
                snapshot_id: 7,
                sequence_number: 4,
                data: b"second".to_vec(),
                properties: Some(BTreeMap::from([("k".into(), "v".into())])),
            },
        ];
        let bytes = write_puffin(&blobs, None);

        // File starts and ends with the magic.
        assert_eq!(&bytes[..4], &MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], &MAGIC);

        let pf = read_puffin(&bytes).unwrap();
        assert_eq!(pf.blobs.len(), 2);
        assert_eq!(pf.blob_bytes(&pf.blobs[0]).unwrap(), b"first-blob-bytes");
        assert_eq!(pf.blob_bytes(&pf.blobs[1]).unwrap(), b"second");
        assert_eq!(pf.blobs[0].fields, vec![1, 2]);
        assert_eq!(pf.blobs[1].snapshot_id, 7);
        assert_eq!(pf.first_of_type("karma-bloom-v1").unwrap().fields, vec![3]);
    }

    #[test]
    fn rejects_non_puffin() {
        assert!(matches!(read_puffin(b"not a puffin file!!!"), Err(PuffinError::BadMagic)));
        assert!(matches!(read_puffin(b"tiny"), Err(PuffinError::Truncated)));
    }
}
