mod collect;
mod filter;
mod scan;
mod select;

pub trait Operator<In> {
    fn execute(&mut self, input: In) -> anyhow::Result<()>;

    fn all_inputs_received(&mut self) -> anyhow::Result<()>;
}
