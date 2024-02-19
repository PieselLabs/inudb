use super::expr::Expr;
use arrow::datatypes::SchemaRef;
#[derive(PartialEq, Debug)]
pub struct TableScan {
    pub table_name: String,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Debug)]
pub struct Projection {
    pub expr: Vec<Expr>,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Debug)]
pub struct Filter {
    pub expr: Vec<Expr>,
    pub schema: SchemaRef,
}

#[derive(PartialEq, Debug)]
pub enum LogicalPlan {
    TableScan(TableScan),
    Projection(Projection),
    Filter(Filter),
}

impl LogicalPlan {
    pub fn get_schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::TableScan(scan) => scan.schema.clone(),
            LogicalPlan::Projection(proj) => proj.schema.clone(),
            LogicalPlan::Filter(filter) => filter.schema.clone(),
        }
    }
}
