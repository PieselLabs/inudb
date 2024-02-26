use crate::execution::kernels::Kernel;

pub struct Out<'k, T> {
    kernel: &'k mut dyn Kernel<T>,
}
