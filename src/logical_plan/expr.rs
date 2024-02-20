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
pub struct ColumnExpr {
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
    Column(ColumnExpr),
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
                Box::from(Expr::Column(ColumnExpr {name : ident.value.to_string() }))
            }
            // ast::Expr::CompoundIdentifier(_) => {}
            // ast::Expr::JsonAccess { .. } => {}
            // ast::Expr::CompositeAccess { .. } => {}
            // ast::Expr::IsFalse(_) => {}
            // ast::Expr::IsNotFalse(_) => {}
            // ast::Expr::IsTrue(_) => {}
            // ast::Expr::IsNotTrue(_) => {}
            // ast::Expr::IsNull(_) => {}
            // ast::Expr::IsNotNull(_) => {}
            // ast::Expr::IsUnknown(_) => {}
            // ast::Expr::IsNotUnknown(_) => {}
            // ast::Expr::IsDistinctFrom(_, _) => {}
            // ast::Expr::IsNotDistinctFrom(_, _) => {}
            // ast::Expr::InList { .. } => {}
            // ast::Expr::InSubquery { .. } => {}
            // ast::Expr::InUnnest { .. } => {}
            // ast::Expr::Between { .. } => {}
            // ast::Expr::BinaryOp { .. } => {}
            // ast::Expr::Like { .. } => {}
            // ast::Expr::ILike { .. } => {}
            // ast::Expr::SimilarTo { .. } => {}
            // ast::Expr::RLike { .. } => {}
            // ast::Expr::AnyOp { .. } => {}
            // ast::Expr::AllOp { .. } => {}
            // ast::Expr::UnaryOp { .. } => {}
            // ast::Expr::Convert { .. } => {}
            // ast::Expr::Cast { .. } => {}
            // ast::Expr::TryCast { .. } => {}
            // ast::Expr::SafeCast { .. } => {}
            // ast::Expr::AtTimeZone { .. } => {}
            // ast::Expr::Extract { .. } => {}
            // ast::Expr::Ceil { .. } => {}
            // ast::Expr::Floor { .. } => {}
            // ast::Expr::Position { .. } => {}
            // ast::Expr::Substring { .. } => {}
            // ast::Expr::Trim { .. } => {}
            // ast::Expr::Overlay { .. } => {}
            // ast::Expr::Collate { .. } => {}
            // ast::Expr::Nested(_) => {}
            ast::Expr::Value(value) => {
                match value {
                    ast::Value::Number(number, flag) => {
                        Box::from(Expr::IntegerLiteral(IntegerLiteralExpr {value : number.parse().unwrap()}))
                    }
                    // Value::SingleQuotedString(_) => {}
                    // Value::DollarQuotedString(_) => {}
                    // Value::EscapedStringLiteral(_) => {}
                    // Value::SingleQuotedByteStringLiteral(_) => {}
                    // Value::DoubleQuotedByteStringLiteral(_) => {}
                    // Value::RawStringLiteral(_) => {}
                    // Value::NationalStringLiteral(_) => {}
                    // Value::HexStringLiteral(_) => {}
                    // Value::DoubleQuotedString(_) => {}
                    // Value::Boolean(_) => {}
                    // Value::Null => {}
                    // Value::Placeholder(_) => {}
                    // Value::UnQuotedString(_) => {}
                    _ => unimplemented!()
                }
            }
            // ast::Expr::IntroducedString { .. } => {}
            // ast::Expr::TypedString { .. } => {}
            // ast::Expr::MapAccess { .. } => {}
            // ast::Expr::Function(_) => {}
            // ast::Expr::AggregateExpressionWithFilter { .. } => {}
            // ast::Expr::Case { .. } => {}
            // ast::Expr::Exists { .. } => {}
            // ast::Expr::Subquery(_) => {}
            // ast::Expr::ArraySubquery(_) => {}
            // ast::Expr::ListAgg(_) => {}
            // ast::Expr::ArrayAgg(_) => {}
            // ast::Expr::GroupingSets(_) => {}
            // ast::Expr::Cube(_) => {}
            // ast::Expr::Rollup(_) => {}
            // ast::Expr::Tuple(_) => {}
            // ast::Expr::Struct { .. } => {}
            // ast::Expr::Named { .. } => {}
            // ast::Expr::ArrayIndex { .. } => {}
            // ast::Expr::Array(_) => {}
            // ast::Expr::Interval(_) => {}
            // ast::Expr::MatchAgainst { .. } => {}
            // ast::Expr::Wildcard => {}
            // ast::Expr::QualifiedWildcard(_) => {}
            _ => unimplemented!()
        }
    }

    fn visit_binary_op(binary_op: &BinaryOperator) -> BinaryOp {
        match binary_op {
            // BinaryOperator::Plus => {}
            // BinaryOperator::Minus => {}
            // BinaryOperator::Multiply => {}
            // BinaryOperator::Divide => {}
            // BinaryOperator::Modulo => {}
            // BinaryOperator::StringConcat => {}
            BinaryOperator::Gt => { BinaryOp::GT }
            BinaryOperator::Lt => { BinaryOp::LT }
            // BinaryOperator::GtEq => {}
            // BinaryOperator::LtEq => {}
            // BinaryOperator::Spaceship => {}
            // BinaryOperator::Eq => {}
            // BinaryOperator::NotEq => {}
            BinaryOperator::And => { BinaryOp::AND }
            BinaryOperator::Or => { BinaryOp::OR }
            // BinaryOperator::Xor => {}
            // BinaryOperator::BitwiseOr => {}
            // BinaryOperator::BitwiseAnd => {}
            // BinaryOperator::BitwiseXor => {}
            // BinaryOperator::DuckIntegerDivide => {}
            // BinaryOperator::MyIntegerDivide => {}
            // BinaryOperator::Custom(_) => {}
            // BinaryOperator::PGBitwiseXor => {}
            // BinaryOperator::PGBitwiseShiftLeft => {}
            // BinaryOperator::PGBitwiseShiftRight => {}
            // BinaryOperator::PGExp => {}
            // BinaryOperator::PGOverlap => {}
            // BinaryOperator::PGRegexMatch => {}
            // BinaryOperator::PGRegexIMatch => {}
            // BinaryOperator::PGRegexNotMatch => {}
            // BinaryOperator::PGRegexNotIMatch => {}
            // BinaryOperator::PGLikeMatch => {}
            // BinaryOperator::PGILikeMatch => {}
            // BinaryOperator::PGNotLikeMatch => {}
            // BinaryOperator::PGNotILikeMatch => {}
            // BinaryOperator::PGStartsWith => {}
            // BinaryOperator::PGCustomBinaryOperator(_) => {}
            _ => unimplemented!()
        }
    }
}