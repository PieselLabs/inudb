use crate::execution::operators::Operator;
use arrow::array::{Array, Int32Array, RecordBatch};
use std::sync::Arc;

pub struct Select {}

impl Select {
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Operator<(Vec<usize>, Arc<RecordBatch>), Arc<RecordBatch>> for Select {
    fn execute(
        &mut self,
        input: (Vec<usize>, Arc<RecordBatch>),
    ) -> anyhow::Result<Arc<RecordBatch>> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        let (indexes, batch) = input;
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
        let new_batch = Arc::new(RecordBatch::try_new(batch.schema(), arrays)?);
        Ok(new_batch)
    }
}
