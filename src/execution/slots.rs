use crate::execution::kernel::Kernel;

pub struct Out<'k, T> {
    kernel: &'k mut dyn Kernel<T>,
}
