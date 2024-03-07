use crate::execution::operators::Operator;
use crate::logical_plan::expr::{Binary, BinaryOp, Expr, Ident, IntegerLiteral};
use arrow::array::{Array, Int32Array, RecordBatch};
use std::sync::Arc;

pub struct Filter<'i> {
    successor: Box<dyn Operator<(Vec<usize>, Arc<RecordBatch>)> + 'i>,
    expression: Box<Expr>,
}

impl<'i> Filter<'i> {
    pub(crate) fn new(
        expression: Box<Expr>,
        successor: Box<dyn Operator<(Vec<usize>, Arc<RecordBatch>)> + 'i>,
    ) -> Self {
        Self {
            expression,
            successor,
        }
    }
}

impl Operator<Arc<RecordBatch>> for Filter<'_> {
    fn execute(&mut self, input: Arc<RecordBatch>) -> anyhow::Result<()> {
        let exec = FilterExec::new(&input);

        let mut indices: Vec<usize> = Vec::new();
        let map = exec.evaluate(&self.expression);

        for (i, elem) in map.iter().enumerate() {
            if *elem {
                indices.push(i);
            }
        }

        self.successor.execute((indices, input.clone()))?;

        Ok(())
    }

    fn all_inputs_received(&mut self) -> anyhow::Result<()> {
        self.successor.all_inputs_received()
    }
}

#[derive(Clone, Copy)]
struct FilterExec<'e> {
    record_batch: &'e RecordBatch,
}

impl<'e> FilterExec<'e> {
    pub const fn new(record_batch: &'e RecordBatch) -> Self {
        Self { record_batch }
    }

    fn visit_expression(self, expression: &Expr) -> Vec<bool> {
        match expression {
            Expr::Binary(binary) => match binary.op {
                BinaryOp::And | BinaryOp::Or => self.visit_logical_binary(binary),
                BinaryOp::Lt | BinaryOp::Gt => self.visit_compare_binary(binary),
            },
            _ => unimplemented!(),
        }
    }

    fn visit_logical_binary(self, expr: &Binary) -> Vec<bool> {
        let lhs = self.visit_expression(&expr.lhs);
        let rhs = self.visit_expression(&expr.rhs);
        let mut result = Vec::with_capacity(lhs.len());
        for i in 0..lhs.len() {
            match expr.op {
                BinaryOp::And => result.push(lhs[i] && rhs[i]),
                BinaryOp::Or => result.push(lhs[i] || rhs[i]),
                _ => panic!("Binary op must be logical type"),
            }
        }
        result
    }

    fn visit_compare_binary(self, expr: &Binary) -> Vec<bool> {
        let lhs = self.visit_binary_value(&expr.lhs);
        let rhs = self.visit_binary_value(&expr.rhs);
        let mut result = Vec::with_capacity(lhs.len());
        for i in 0..lhs.len() {
            match expr.op {
                BinaryOp::Lt => result.push(lhs[i] < rhs[i]),
                BinaryOp::Gt => result.push(lhs[i] > rhs[i]),
                _ => panic!("Binary op must be compare type"),
            }
        }
        result
    }

    fn visit_binary_value(self, expr: &Expr) -> Vec<i32> {
        match expr {
            Expr::Binary(_) => {
                panic!("visit_binary_value")
            }
            Expr::Ident(ident) => self.visit_ident(ident),
            Expr::IntegerLiteral(literal) => self.visit_integer_literal(literal),
        }
    }

    fn visit_integer_literal(self, expr: &IntegerLiteral) -> Vec<i32> {
        let len = self.record_batch.num_rows();
        let mut result: Vec<i32> = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(expr.value);
        }
        result
    }

    fn visit_ident(self, expr: &Ident) -> Vec<i32> {
        let column = self
            .record_batch
            .column_by_name(&expr.name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut result: Vec<i32> = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            result.push(column.value(i));
        }
        result
    }

    fn evaluate(self, expression: &Expr) -> Vec<bool> {
        self.visit_expression(expression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::operators::collect::Collect;
    use crate::execution::operators::select::Select;
    use crate::logical_plan::expr::{Binary, BinaryOp, Ident, IntegerLiteral};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_filter_kernel() -> anyhow::Result<()> {
        let batch_size = 500;
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
        let batch =
            Arc::new(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap());

        let mut res = Vec::new();

        {
            let collect = Box::new(Collect::new(&mut res));
            let select = Box::new(Select::new(collect));
            let mut filter = Filter::new(filter_expr, select);

            filter.execute(batch)?;
        }

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].num_rows(), 39);

        Ok(())
    }
}
