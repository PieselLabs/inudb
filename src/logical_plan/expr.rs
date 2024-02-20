use sqlparser::ast;
use sqlparser::ast::BinaryOperator;

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
pub struct IdentExpr {
    pub name: String,
}

#[derive(PartialEq, Debug)]
pub struct IntegerLiteralExpr {
    pub value: i32,
}


#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum Expr {
    Binary(BinaryExpr),
    Ident(IdentExpr),
    IntegerLiteral(IntegerLiteralExpr),
}

pub struct VisitExpression {}

impl VisitExpression {
    pub fn visit(expr: &ast::Expr) -> Box<Expr> {
        match expr {
            ast::Expr::BinaryOp { left, op, right} => {
               Box::from(Expr::Binary(BinaryExpr {lhs : Self::visit(left), binary_op : Self::visit_binary_op(op), rhs : Self::visit(right)}))
            }
            ast::Expr::Identifier(ident) => {
                Box::from(Expr::Ident(IdentExpr {name : ident.value.to_string() }))
            }
            ast::Expr::Value(value) => {
                match value {
                    ast::Value::Number(number, flag) => {
                        Box::from(Expr::IntegerLiteral(IntegerLiteralExpr {value : number.parse().unwrap()}))
                    }
                    _ => unimplemented!()
                }
            }
            _ => unimplemented!()
        }
    }

    fn visit_binary_op(binary_op: &BinaryOperator) -> BinaryOp {
        match binary_op {
            BinaryOperator::Gt => { BinaryOp::GT }
            BinaryOperator::Lt => { BinaryOp::LT }
            BinaryOperator::And => { BinaryOp::AND }
            BinaryOperator::Or => { BinaryOp::OR }
            _ => unimplemented!()
        }
    }
}