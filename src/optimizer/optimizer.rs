use crate::catalog::Catalog;
use crate::logical_plan::{Dag, LogicalPlan};
use crate::optimizer::errors::OptimizerError;

pub struct Optimizer<'c> {
    catalog: &'c dyn Catalog,
}

impl<'c> Optimizer<'c> {
    pub fn new(catalog: &'c dyn Catalog) -> Self {
        Optimizer { catalog }
    }

    pub fn run(dag: &mut Dag<LogicalPlan>) -> Result<(), OptimizerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DummyCatalog;

    #[test]
    fn test_optimizer() {
        let catalog = DummyCatalog::new();

        let _optimizer = Optimizer::new(&catalog);
    }
}
