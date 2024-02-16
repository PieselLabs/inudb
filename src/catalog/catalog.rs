use crate::catalog::errors::CatalogError;
use arrow::datatypes::Schema;

pub trait Catalog {
    fn get_schema(&self, table_name: &str) -> Result<Schema, CatalogError>;
}
