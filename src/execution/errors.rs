use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionError {
    TableNotFound(String),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl Error for ExecutionError {}
