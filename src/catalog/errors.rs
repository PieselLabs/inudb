use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    TableNotFound(String),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TableNotFound(table_name) => {
                write!(
                    f,
                    "Catalog Error: Table with name {table_name} not found in catalog"
                )
            }
        }
    }
}

impl Error for CatalogError {}
