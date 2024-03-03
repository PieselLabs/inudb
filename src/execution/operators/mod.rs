mod filter;
mod pipe;
mod scan;
mod select;

pub trait Operator<In, Out> {
    fn execute(&mut self, input: In) -> anyhow::Result<Out>;
}
