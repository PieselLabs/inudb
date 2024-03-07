use crate::execution::operators::Operator;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

pub struct Scan<'i> {
    successor: Box<dyn Operator<Arc<RecordBatch>> + 'i>,
    schema: SchemaRef,
}

impl<'i> Scan<'i> {
    pub fn new(successor: Box<dyn Operator<Arc<RecordBatch>> + 'i>) -> Self {
        Self {
            successor,
            schema: SchemaRef::from(Schema::empty()),
        }
    }
}

impl Operator<(String, usize)> for Scan<'_> {
    fn execute(&mut self, input: (String, usize)) -> anyhow::Result<()> {
        let (file_path, chunk_size) = input;
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(chunk_size);
        self.schema = builder.schema().clone();
        let reader = builder.build()?;

        for b in reader {
            self.successor.execute(Arc::new(b?))?;
        }

        Ok(())
    }

    fn all_inputs_received(&mut self) -> anyhow::Result<()> {
        self.successor.all_inputs_received()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::operators::collect::Collect;

    #[test]
    fn test_scan_kernel() -> anyhow::Result<()> {
        let batch_size = 500;

        let mut res = Vec::new();

        {
            let collect = Box::new(Collect::new(&mut res));
            let mut scan = Scan::new(collect);
            let batch = scan.execute((
                "samples/sample-data/parquet/userdata1.parquet".to_string(),
                batch_size,
            ))?;
        }

        assert_eq!(res.len(), 2);

        for b in res {
            assert_eq!(b.num_columns(), 13);
            assert_eq!(b.num_rows(), batch_size);
        }

        Ok(())
    }
}
