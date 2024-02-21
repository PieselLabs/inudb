use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptimizerError {}

impl fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!();
    }
}

impl Error for OptimizerError {}
