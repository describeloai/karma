//! Read **only** a chosen set of Parquet row groups.
//!
//! This is where the index cashes out: the `parquet` reader is told exactly which
//! row groups to decode (`with_row_groups`), so the column chunks of the *pruned*
//! row groups are never fetched from the underlying store. Generic over
//! [`ChunkReader`] so the same code path serves a real `File` in production and an
//! instrumented reader in tests (which asserts the skipped row group's bytes are
//! never requested).

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::file::reader::ChunkReader;

use crate::KarmaParquetError;

/// Decode `row_groups` (by ordinal) from `reader`, returning the file's Arrow schema
/// and the batches — in row-group then row order. An empty `row_groups` reads nothing
/// (zero batches); duplicates/ordering are honoured by the parquet reader as given.
pub fn read_row_groups<R: ChunkReader + 'static>(
    reader: R,
    row_groups: &[usize],
) -> Result<(SchemaRef, Vec<RecordBatch>), KarmaParquetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let schema = builder.schema().clone();
    let rb_reader = builder.with_row_groups(row_groups.to_vec()).build()?;
    let mut batches = Vec::new();
    for b in rb_reader {
        batches.push(b?);
    }
    Ok((schema, batches))
}

/// The Arrow schema of a Parquet file, without reading any row-group data.
pub fn read_schema<R: ChunkReader + 'static>(reader: R) -> Result<SchemaRef, KarmaParquetError> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(reader)?.schema().clone())
}
