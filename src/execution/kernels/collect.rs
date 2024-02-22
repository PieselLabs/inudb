use crate::execution::kernels::kernel::Kernel;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

pub struct CollectKernel<'i> {
    res: &'i mut Vec<RecordBatch>,
}

impl<'i> CollectKernel<'i> {
    pub(crate) fn new(res: &'i mut Vec<RecordBatch>) -> Self {
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
