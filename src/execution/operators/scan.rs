use crate::execution::operators::Operator;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

pub struct Scan {
    schema: SchemaRef,
}

impl Scan {
    pub fn new() -> Self {
        Self {
            schema: SchemaRef::from(Schema::empty()),
        }
    }
}

impl Operator<(String, usize), Arc<RecordBatch>> for Scan {
    fn execute(&mut self, input: (String, usize)) -> anyhow::Result<Arc<RecordBatch>> {
        let (file_path, chunk_size) = input;
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(chunk_size);
        self.schema = builder.schema().clone();
        let mut reader = builder.build()?;

        Ok(Arc::new(reader.nth(0).unwrap()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_kernel() -> anyhow::Result<()> {
        let batch_size = 500;

        let mut scan = Scan::new();
        let batch = scan.execute((
            "samples/sample-data/parquet/userdata1.parquet".to_string(),
            batch_size,
        ))?;

        assert_eq!(batch.num_columns(), 13);
        assert_eq!(batch.num_rows(), batch_size);

        Ok(())
    }
}
