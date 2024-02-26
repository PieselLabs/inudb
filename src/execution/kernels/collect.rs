use crate::execution::kernels::Kernel;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

pub struct Collect<'i> {
    res: &'i mut Vec<RecordBatch>,
}

impl<'i> Collect<'i> {
    pub(crate) fn new(res: &'i mut Vec<RecordBatch>) -> Self {
        Self { res }
    }
}

impl Kernel<RecordBatch> for Collect<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: RecordBatch) -> anyhow::Result<()> {
        self.res.push(input);
        Ok(())
    }
}
