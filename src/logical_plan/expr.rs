#[derive(PartialEq, Debug)]
pub enum BinaryOp {
    AND,
    OR,
    LT,
    GT,
}

#[derive(PartialEq, Debug)]
pub struct BinaryExpr {
    pub lhs: Box<Expr>,
    pub binary_op: BinaryOp,
    pub rhs: Box<Expr>,
}

#[derive(PartialEq, Debug)]
pub struct ColumnExpr {
    pub name: String,
}


#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum Expr {
    Binary(BinaryExpr),
    Column(ColumnExpr)
}