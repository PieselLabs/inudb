use crate::catalog::catalog::Catalog;
use crate::catalog::errors::CatalogError;
use arrow::datatypes::Schema;
use std::collections::HashMap;

pub struct DummyCatalog {
    tables: HashMap<String, Schema>,
}

impl DummyCatalog {
    pub fn new() -> Self {
        DummyCatalog {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: &str, schema: Schema) {
        self.tables.insert(name.to_string(), schema);
    }
}

impl Catalog for DummyCatalog {
    fn get_schema(&self, table_name: &str) -> Result<Schema, CatalogError> {
        if let Some(schema) = self.tables.get(table_name) {
            Ok(schema.clone())
        } else {
            Err(CatalogError::TableNotFound(table_name.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog() {
        let mut catalog = DummyCatalog::new();

        assert_eq!(
            catalog.get_schema("table"),
            Err(CatalogError::TableNotFound("table".to_string()))
        );

        catalog.add_table("table", Schema::empty());
        assert_eq!(catalog.get_schema("table"), Ok(Schema::empty()));
    }
}
