use crate::execution::kernel::Kernel;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub struct GeneratorKernel {
    children: Vec<Box<dyn Kernel>>,
}

impl Kernel for GeneratorKernel {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&self, inputs: Vec<Arc<dyn Any>>) {
        for k in &self.children {
            k.execute(vec![Arc::new(RecordBatch::new_empty(self.schema()))]);
        }
    }
}
