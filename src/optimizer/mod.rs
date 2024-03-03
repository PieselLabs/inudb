mod errors;

use crate::catalog::Catalog;

pub struct Optimizer<'c> {
    catalog: &'c dyn Catalog,
}

impl<'c> Optimizer<'c> {
    pub fn new(catalog: &'c dyn Catalog) -> Self {
        Optimizer { catalog }
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
