use sqlparser::ast;
use sqlparser::ast::BinaryOperator;

#[derive(PartialEq, Eq, Debug)]
pub enum BinaryOp {
    And,
    Or,
    Lt,
    Gt,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Binary {
    pub lhs: Box<Expr>,
    pub op: BinaryOp,
    pub rhs: Box<Expr>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Ident {
    pub name: String,
}

#[derive(PartialEq, Eq, Debug)]
pub struct IntegerLiteral {
    pub value: i32,
}

#[allow(dead_code)]
#[derive(PartialEq, Eq, Debug)]
pub enum Expr {
    Binary(Binary),
    Ident(Ident),
    IntegerLiteral(IntegerLiteral),
}

pub struct VisitExpression {}

impl VisitExpression {
    pub fn visit(expr: &ast::Expr) -> Expr {
        match expr {
            ast::Expr::BinaryOp { left, op, right } => Expr::Binary(Binary {
                lhs: Box::new(Self::visit(left)),
                op: Self::visit_binary_op(op),
                rhs: Box::new(Self::visit(right)),
            }),
            ast::Expr::Identifier(ident) => Expr::Ident(Ident {
                name: ident.value.to_string(),
            }),
            ast::Expr::Value(value) => match value {
                ast::Value::Number(number, flag) => Expr::IntegerLiteral(IntegerLiteral {
                    value: number.parse().unwrap(),
                }),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    fn visit_binary_op(binary_op: &BinaryOperator) -> BinaryOp {
        match binary_op {
            BinaryOperator::Gt => BinaryOp::Gt,
            BinaryOperator::Lt => BinaryOp::Lt,
            BinaryOperator::And => BinaryOp::And,
            BinaryOperator::Or => BinaryOp::Or,
            _ => unimplemented!(),
        }
    }
}
