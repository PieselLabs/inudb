use crate::logical_plan::dag::Dag;
use crate::logical_plan::expr::Expr;
use crate::logical_plan::node::{NodeId, PlanNode, Projection, TableScan};
use arrow::datatypes::SchemaRef;

struct DagBuilder<'d> {
    dag: &'d mut Dag,
}

impl<'dag> DagBuilder<'dag> {
    pub fn new(dag: &'dag mut Dag) -> Self {
        DagBuilder { dag }
    }

    pub fn create_scan(&mut self, table_name: String, schema: SchemaRef) -> NodeId {
        self.dag
            .new_node(PlanNode::TableScan(TableScan { table_name }), schema)
    }

    pub fn create_project(&mut self, expr: Vec<Expr>, input: NodeId) -> NodeId {
        // TODO(vlad): infer output schema based on input schema using expression
        let input_schema = self.dag.get_output_schema(input);

        let res = self.dag.new_node(
            PlanNode::Projection(Projection { expr, input }),
            input_schema,
        );
        self.dag.add_usage(input, res);
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
            &PlanNode::TableScan(TableScan {
                table_name: "table".to_string()
            })
        );
        assert_eq!(
            dag.get_node(project),
            &PlanNode::Projection(Projection {
                expr: Vec::new(),
                input: scan
            })
        );

        println!("{:?}", dag);
    }
}
