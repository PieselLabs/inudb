#[derive(PartialEq, Debug)]
pub struct BinaryExpr {
    lhs: Box<Expr>,
    rhs: Box<Expr>,
}


#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum Expr {
    BinaryExpr(BinaryExpr)
}