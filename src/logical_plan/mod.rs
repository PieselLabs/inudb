pub use crate::dag::*;

pub mod dag_builder;
pub mod expr;

use arrow::datatypes::SchemaRef;
use expr::Expr;

#[derive(PartialEq, Eq, Debug)]
pub struct TableScan {
    pub table_name: String,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Projection {
    pub expr: Vec<Expr>,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Filter {
    pub expr: Box<Expr>,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Eq, Debug)]
pub enum LogicalPlan {
    TableScan(TableScan),
    Projection(Projection),
    Filter(Filter),
}

impl LogicalPlan {
    pub fn get_schema(&self) -> SchemaRef {
        match self {
            Self::TableScan(scan) => scan.schema.clone(),
            Self::Projection(proj) => proj.schema.clone(),
            Self::Filter(filter) => filter.schema.clone(),
        }
    }
}
