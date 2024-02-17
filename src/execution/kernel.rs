use arrow::datatypes::SchemaRef;
use std::any::Any;
use std::sync::Arc;

pub trait Kernel {
    fn schema(&self) -> SchemaRef;

    fn execute(&self, inputs: Vec<Arc<dyn Any>>) -> Option<Arc<dyn Any>>;
}
