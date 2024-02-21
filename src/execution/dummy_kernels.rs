use crate::execution::kernel::Kernel;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub struct CollectKernel<'i> {
    res: &'i mut Vec<RecordBatch>,
}

impl<'i> CollectKernel<'i> {
    fn new(res: &'i mut Vec<RecordBatch>) -> Self {
        Self { res }
    }
}

impl Kernel<RecordBatch> for CollectKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: RecordBatch) -> anyhow::Result<()> {
        self.res.push(input);
        Ok(())
    }
}

pub struct ScanKernel<'s> {
    schema: SchemaRef,
    children: Vec<Box<dyn Kernel<RecordBatch> + 's>>,
}

impl<'s> ScanKernel<'s> {
    fn new(children: Vec<Box<dyn Kernel<RecordBatch> + 's>>) -> Self {
        Self {
            schema: SchemaRef::from(Schema::empty()),
            children,
        }
    }
}

impl Kernel<(String, usize)> for ScanKernel<'_> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&mut self, input: (String, usize)) -> anyhow::Result<()> {
        let (file_path, chunk_size) = input;
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(chunk_size);
        self.schema = builder.schema().clone();
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            for child in &mut self.children {
                child.execute(batch.clone())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_kernel() {
        let mut res: Vec<RecordBatch> = Vec::new();
        let batch_size = 500;
        {
            let collect = CollectKernel::new(&mut res);
            let mut scan = ScanKernel::new(vec![Box::new(collect)]);
            let _ = scan.execute((
                "samples/sample-data/parquet/userdata1.parquet".to_string(),
                batch_size,
            ));
        }

        assert_eq!(res.len(), 1000 / batch_size);
        for batch in res {
            assert_eq!(batch.num_columns(), 13);
            assert_eq!(batch.num_rows(), batch_size);
        }
    }
}
