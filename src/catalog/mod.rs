mod dummy_catalog;
mod errors;

use crate::catalog::CatalogError;
use arrow::datatypes::SchemaRef;

pub use dummy_catalog::*;
pub use errors::*;

pub trait Catalog {
    fn get_schema(&self, table_name: &str) -> Result<SchemaRef, CatalogError>;
}
