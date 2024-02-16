use crate::catalog::errors::CatalogError;
use arrow::datatypes::SchemaRef;

pub trait Catalog {
    fn get_schema(&self, table_name: &str) -> Result<SchemaRef, CatalogError>;
}
