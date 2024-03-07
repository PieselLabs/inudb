use crate::execution::operators::Operator;
use arrow::array::RecordBatch;
use std::sync::Arc;

pub struct Collect<'i> {
    res: &'i mut Vec<Arc<RecordBatch>>,
}

impl<'i> Collect<'i> {
    pub(crate) fn new(res: &'i mut Vec<Arc<RecordBatch>>) -> Self {
        Self { res }
    }
}

impl Operator<Arc<RecordBatch>> for Collect<'_> {
    fn execute(&mut self, input: Arc<RecordBatch>) -> anyhow::Result<()> {
        let batch = input;
        self.res.push(batch);
        Ok(())
    }

    fn all_inputs_received(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
