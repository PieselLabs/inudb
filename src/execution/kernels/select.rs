use crate::execution::kernels::Kernel;
use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

pub struct Select<'s> {
    res: &'s mut Vec<RecordBatch>,
}

impl<'s> Select<'s> {
    pub(crate) fn new(res: &'s mut Vec<RecordBatch>) -> Self {
        Self { res }
    }
}

impl Kernel<(RecordBatch, Vec<usize>)> for Select<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: (RecordBatch, Vec<usize>)) -> anyhow::Result<()> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        let (batch, indexes) = input;
        for i in 0..batch.num_columns() {
            let column = batch
                .column(i)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let mut data = Vec::new();
            for i in &indexes {
                data.push(column.value(*i));
            }
            arrays.push(Arc::new(Int32Array::from(data)));
        }
        let new_batch = RecordBatch::try_new(batch.schema(), arrays)?;
        self.res.push(new_batch);
        Ok(())
    }
}
