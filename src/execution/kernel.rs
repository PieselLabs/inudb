use arrow::datatypes::SchemaRef;

pub trait Kernel<T> {
    fn schema(&self) -> SchemaRef;

    fn execute(&mut self, input: T);
}
