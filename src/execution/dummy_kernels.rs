use crate::execution::kernel::Kernel;
use arrow::datatypes::SchemaRef;

pub struct GeneratorKernel<'i> {
    children: Vec<Box<dyn Kernel<usize> + 'i>>,
}

impl<'i> GeneratorKernel<'i> {
    fn new(children: Vec<Box<dyn Kernel<usize> + 'i>>) -> Box<Self> {
        Box::new(GeneratorKernel { children })
    }
}

impl Kernel<()> for GeneratorKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, inputs: ()) {
        for i in 0..10 {
            for k in &mut self.children {
                k.execute(i);
            }
        }
    }
}

pub struct FilterKernel<'i> {
    children: Vec<Box<dyn Kernel<(bool, usize)> + 'i>>,
    val: usize,
}

impl<'i> FilterKernel<'i> {
    fn new(val: usize, children: Vec<Box<dyn Kernel<(bool, usize)> + 'i>>) -> Box<Self> {
        Box::new(FilterKernel { children, val })
    }
}

impl Kernel<usize> for FilterKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: usize) {
        for k in &mut self.children {
            k.execute((input > self.val, input))
        }
    }
}

pub struct CollectKernel<'i> {
    res: &'i mut Vec<usize>,
}

impl<'i> CollectKernel<'i> {
    fn new(res: &'i mut Vec<usize>) -> Box<Self> {
        Box::new(CollectKernel { res })
    }
}

impl Kernel<(bool, usize)> for CollectKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: (bool, usize)) {
        if let (true, val) = input {
            self.res.push(val);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kernels() {
        let mut res = Vec::new();
        {
            let collect = CollectKernel::new(&mut res);
            let filter = FilterKernel::new(4, vec![collect]);
            let mut gen = GeneratorKernel::new(vec![filter]);
            gen.execute(());
        }

        assert_eq!(res, vec![5, 6, 7, 8, 9]);
    }
}
