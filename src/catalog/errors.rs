use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableNotFound(String),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogError::TableNotFound(table_name) => {
                write!(
                    f,
                    "Catalog Error: Table with name {} not found in catalog",
                    table_name
                )
            }
        }
    }
}

impl Error for CatalogError {}
