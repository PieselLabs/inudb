mod collect;
mod filter;
mod scan;
mod select;

use arrow::datatypes::SchemaRef;

pub trait Kernel<T> {
    fn schema(&self) -> SchemaRef;

    fn execute(&mut self, input: T) -> anyhow::Result<()>;
}