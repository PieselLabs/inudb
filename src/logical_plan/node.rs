use super::expr::Expr;
pub type NodeId = usize;

#[derive(PartialEq, Debug)]
pub struct TableScan {
    pub table_name: String,
}

#[derive(PartialEq, Debug)]
pub struct Projection {
    pub expr: Vec<Expr>,
    pub input: NodeId,
}

#[derive(PartialEq, Debug)]
pub enum PlanNode {
    TableScan(TableScan),
    Projection(Projection),
}


// select a, b from t;
// #0 = TableScan("t")
// #1 = Project({"a", "b"}, #0)


// select a, b, c from t join k on t.a = k.a;