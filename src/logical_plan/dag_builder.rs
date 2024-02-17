use crate::dag::Dag;
use crate::logical_plan::expr::Expr;
use crate::logical_plan::logical_plan::{LogicalPlan, Projection, TableScan};
use crate::logical_plan::NodeId;
use arrow::datatypes::SchemaRef;

struct DagBuilder<'d> {
    dag: &'d mut Dag<LogicalPlan>,
}

impl<'d> DagBuilder<'d> {
    pub fn new(dag: &'d mut Dag<LogicalPlan>) -> Self {
        DagBuilder { dag }
    }

    pub fn create_scan(&mut self, table_name: String, schema: SchemaRef) -> NodeId {
        self.dag
            .new_node(LogicalPlan::TableScan(TableScan { table_name, schema }))
    }

    pub fn create_project(&mut self, expr: Vec<Expr>, input: NodeId) -> NodeId {
        // TODO(vlad): infer output schema based on input schema using expression
        let prev = self.dag.get_node(input);
        let schema = prev.get_schema();

        let res = self
            .dag
            .new_node(LogicalPlan::Projection(Projection { expr, schema }));
        self.dag.add_input(res, input);
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn test_dag_builder() {
        // TODO(vlad): add tests for schema inference for projection/join and other node types
        let mut dag = Dag::new();

        let mut builder = DagBuilder::new(&mut dag);

        let scan = builder.create_scan("table".to_string(), Arc::new(Schema::empty()));
        let project = builder.create_project(Vec::new(), scan);

        assert_eq!(
            dag.get_node(scan),
            &LogicalPlan::TableScan(TableScan {
                table_name: "table".to_string(),
                schema: Arc::new(Schema::empty()),
            })
        );
        assert_eq!(
            dag.get_node(project),
            &LogicalPlan::Projection(Projection {
                expr: Vec::new(),
                schema: Arc::new(Schema::empty()),
            })
        );

        assert_eq!(dag.get_inputs(scan).len(), 0);
        assert_eq!(dag.get_inputs(project)[0], scan);

        println!("{:?}", dag);
    }
}
