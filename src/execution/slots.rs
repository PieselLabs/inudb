use crate::execution::kernels::kernel::Kernel;

pub struct Out<'k, T> {
    kernel: &'k mut dyn Kernel<T>,
}
