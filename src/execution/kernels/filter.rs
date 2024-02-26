use crate::execution::kernels::Kernel;
use crate::logical_plan::expr::{Binary, BinaryOp, Expr, Ident, IntegerLiteral};
use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::SchemaRef;

pub struct Filter<'f> {
    children: Vec<Box<dyn Kernel<(RecordBatch, Vec<usize>)> + 'f>>,
    expression: &'f Expr,
}

impl<'f> Filter<'f> {
    pub(crate) fn new(
        children: Vec<Box<dyn Kernel<(RecordBatch, Vec<usize>)> + 'f>>,
        expression: &'f Expr,
    ) -> Self {
        Self {
            children,
            expression,
        }
    }
}

impl Kernel<RecordBatch> for Filter<'_> {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&mut self, input: RecordBatch) -> anyhow::Result<()> {
        let exec = FilterExec::new(&input);
        for child in &mut self.children {
            let mut indecies: Vec<usize> = Vec::new();
            let map = exec.evaluate(self.expression);
            for (i, elem) in map.iter().enumerate() {
                if *elem {
                    indecies.push(i);
                }
            }
            child.execute((input.clone(), indecies))?;
        }
        Ok(())
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
    use crate::execution::kernels::select::Select;
    use crate::logical_plan::expr::{Binary, BinaryOp, Ident, IntegerLiteral};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_filter_kernel() {
        let batch_size = 500;
        let mut result: Vec<RecordBatch> = Vec::new();
        {
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

            let select = Select::new(&mut result);
            let mut filter = Filter::new(vec![Box::new(select)], &filter_expr);
            let _ = filter.execute(batch);
        }
        assert_eq!(result.len(), 1);

        let batch = result.first().unwrap();
        assert_eq!(batch.num_rows(), 39);
        assert_eq!(batch.num_columns(), 1);
    }
}
