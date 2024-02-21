use std::fs::File;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::file::reader::FileReader;
use crate::execution::kernel::Kernel;

pub struct GeneratorKernel<'i> {
    children: Vec<Box<dyn Kernel<usize> + 'i>>,
}

impl<'i> GeneratorKernel<'i> {
    fn new(children: Vec<Box<dyn Kernel<usize> + 'i>>) -> Box<Self> {
        Box::new(GeneratorKernel { children })
    }
}

impl Kernel<()> for GeneratorKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, inputs: ()) {
        for i in 0..10 {
            for k in &mut self.children {
                k.execute(i);
            }
        }
    }
}

pub struct FilterKernel<'i> {
    children: Vec<Box<dyn Kernel<(bool, usize)> + 'i>>,
    val: usize,
}

impl<'i> FilterKernel<'i> {
    fn new(val: usize, children: Vec<Box<dyn Kernel<(bool, usize)> + 'i>>) -> Box<Self> {
        Box::new(FilterKernel { children, val })
    }
}

impl Kernel<usize> for FilterKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: usize) {
        for k in &mut self.children {
            k.execute((input > self.val, input))
        }
    }
}

pub struct CollectKernel<'i> {
    res: &'i mut Vec<usize>,
}

impl<'i> CollectKernel<'i> {
    fn new(res: &'i mut Vec<usize>) -> Box<Self> {
        Box::new(CollectKernel { res })
    }
}

impl Kernel<(bool, usize)> for CollectKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: (bool, usize)) {
        if let (true, val) = input {
            self.res.push(val);
        }
    }
}

pub struct ScanKernel<'s> {
    schema: SchemaRef,
    res: &'s mut Vec<RecordBatch>,
}

impl<'s> ScanKernel<'s> {
    fn new(res: &'s mut Vec<RecordBatch>) -> Box<Self> {
        Box::new(ScanKernel {schema: arrow::datatypes::SchemaRef::from(arrow::datatypes::Schema::empty()), res })
    }
}

impl Kernel<(String, usize)> for ScanKernel<'_> {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn execute(&mut self, input: (String, usize)) {
        let (file_path, chunk_size) = input;
        let file = File::open(file_path).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        self.schema = builder.schema().clone();
        let mut reader = builder.build().unwrap();
        while let Some(batch) = reader.next() {
            self.res.push(batch.unwrap())
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kernels() {
        let mut res = Vec::new();
        {
            let collect = CollectKernel::new(&mut res);
            let filter = FilterKernel::new(4, vec![collect]);
            let mut gen = GeneratorKernel::new(vec![filter]);
            gen.execute(());
        }

        assert_eq!(res, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_scan_kernel() {
        let mut res: Vec<RecordBatch> = Vec::new();
        {
            let mut scan = ScanKernel::new(&mut res);
            scan.execute(("samples/sample-data/parquet/userdata1.parquet".to_string(), 1000));
        }

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].num_columns(), 13);
        assert_eq!(res[0].num_rows(), 1000);
    }
}
