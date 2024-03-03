use crate::execution::operators::Operator;
use std::marker::PhantomData;

pub struct Pipe<Lhs: Operator<In, Inter>, Rhs: Operator<Inter, Out>, In, Inter, Out> {
    lhs: Lhs,
    rhs: Rhs,
    _in: PhantomData<In>,
    _inter: PhantomData<Inter>,
    _out: PhantomData<Out>,
}

impl<In, Inter, Out, Lhs: Operator<In, Inter>, Rhs: Operator<Inter, Out>> Operator<In, Out>
    for Pipe<Lhs, Rhs, In, Inter, Out>
{
    fn execute(&mut self, input: In) -> anyhow::Result<Out> {
        let inter = self.lhs.execute(input)?;
        self.rhs.execute(inter)
    }
}
