use crate::logical_plan::dag::Dag;
use crate::logical_plan::expr::Expr;
use crate::logical_plan::node::{NodeId, PlanNode, Projection, TableScan};

struct DagBuilder<'dag> {
    dag: &'dag mut Dag,
}

impl <'dag> DagBuilder <'dag> {
    pub fn new(dag: &'dag mut Dag) -> Self {
        DagBuilder {dag}
    }

    pub fn create_scan(&mut self, table_name: String) -> NodeId {
        self.dag.new_node(PlanNode::TableScan(TableScan{table_name}))
    }

    pub fn create_project(&mut self, expr: Vec<Expr>, input: NodeId) -> NodeId {
        let res = self.dag.new_node(PlanNode::Projection(Projection{expr, input}));
        self.dag.add_usage(input, res);
        res
    }
}

#[test]
fn test_dag_builder() {
    let mut dag = Dag::new();

    let mut builder = DagBuilder::new(&mut dag);

    let scan = builder.create_scan("table".to_string());
    let project = builder.create_project(Vec::new(), scan);

    assert_eq!(dag.get_node(scan), &PlanNode::TableScan(TableScan{table_name: "table".to_string()}));
    assert_eq!(dag.get_node(project), &PlanNode::Projection(Projection{expr: Vec::new(), input: scan}));

    println!("{:?}", dag);
}