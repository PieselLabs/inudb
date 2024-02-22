use crate::execution::kernels::kernel::Kernel;
use crate::logical_plan::expr::{Binary, BinaryOp, Expr, Ident, IntegerLiteral};
use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::SchemaRef;

pub struct FilterKernel<'f> {
    res: &'f mut Vec<usize>,
    expression: &'f Box<Expr>,
}

impl<'f> FilterKernel<'f> {
    pub(crate) fn new(res: &'f mut Vec<usize>, expression: &'f Box<Expr>) -> Self {
        Self { res, expression }
    }
}

impl Kernel<RecordBatch> for FilterKernel<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: RecordBatch) -> anyhow::Result<()> {
        let execute_expression = ExecuteExpression::new(&input);
        for i in 0..input.num_rows() {
            if execute_expression.predicate(self.expression, i) {
                self.res.push(i);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct ExecuteExpression<'e> {
    record_batch: &'e RecordBatch,
}

impl<'e> ExecuteExpression<'e> {
    pub fn new(record_batch: &'e RecordBatch) -> Self {
        Self { record_batch }
    }

    fn visit_expression(self, expression: &Expr, index: usize) -> bool {
        match expression {
            Expr::Binary(binary) => match binary.op {
                BinaryOp::And | BinaryOp::Or => self.visit_logical_binary(binary, index),
                BinaryOp::Lt | BinaryOp::Gt => self.visit_compare_binary(binary, index),
            },
            _ => unimplemented!(),
        }
    }

    fn visit_logical_binary(self, expr: &Binary, index: usize) -> bool {
        let lhs = self.visit_expression(&expr.lhs, index);
        let rhs = self.visit_expression(&expr.rhs, index);
        match expr.op {
            BinaryOp::And => lhs && rhs,
            BinaryOp::Or => lhs || rhs,
            _ => panic!("Binary op must be logical type"),
        }
    }

    fn visit_compare_binary(self, expr: &Binary, index: usize) -> bool {
        let lhs = self.visit_binary_value(&expr.lhs, index);
        let rhs = self.visit_binary_value(&expr.rhs, index);
        match expr.op {
            BinaryOp::Lt => lhs < rhs,
            BinaryOp::Gt => lhs > rhs,
            _ => panic!("Binary op must be compare type"),
        }
    }

    fn visit_binary_value(self, expr: &Expr, index: usize) -> i32 {
        match expr {
            Expr::Binary(_) => {
                panic!("visit_binary_value")
            }
            Expr::Ident(ident) => self.visit_ident(ident, index),
            Expr::IntegerLiteral(literal) => Self::visit_integer_literal(literal),
        }
    }

    fn visit_integer_literal(expr: &IntegerLiteral) -> i32 {
        expr.value
    }

    fn visit_ident(self, expr: &Ident, index: usize) -> i32 {
        self.record_batch
            .column_by_name(&*expr.name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(index)
    }

    pub fn predicate(self, expression: &Box<Expr>, index: usize) -> bool {
        self.visit_expression(expression, index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::expr::{Binary, BinaryOp, Ident, IntegerLiteral};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_filter_kernel() {
        let batch_size = 500;
        let mut indeces: Vec<usize> = Vec::new();
        let filter_expr = Box::from(Expr::Binary(Binary {
            lhs: Box::from(Expr::Binary(Binary {
                lhs: Box::from(Expr::Ident(Ident {
                    name: "id".to_string(),
                })),
                op: BinaryOp::Gt,
                rhs: Box::from(Expr::IntegerLiteral(IntegerLiteral { value: 10 })),
            })),
            op: BinaryOp::And,
            rhs: Box::from(Expr::Binary(Binary {
                lhs: Box::from(Expr::Ident(Ident {
                    name: "id".to_string(),
                })),
                op: BinaryOp::Lt,
                rhs: Box::from(Expr::IntegerLiteral(IntegerLiteral { value: 50 })),
            })),
        }));

        let id_array = Int32Array::from((0..batch_size).collect::<Vec<i32>>());
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        let mut filter = FilterKernel::new(&mut indeces, &filter_expr);
        let _ = filter.execute(batch);
        assert_eq!(indeces.len(), 39)
    }
}
