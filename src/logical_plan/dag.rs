use crate::logical_plan::node::{NodeId, PlanNode};
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;

#[derive(Debug)]
pub struct Dag {
    nodes: Vec<PlanNode>,
    usages: Vec<HashSet<NodeId>>,
    output_schemas: Vec<SchemaRef>,
}

impl Dag {
    pub fn new() -> Self {
        Dag {
            nodes: Vec::new(),
            usages: Vec::new(),
            output_schemas: Vec::new(),
        }
    }

    pub fn get_node(&self, id: NodeId) -> &PlanNode {
        &self.nodes[id]
    }

    pub fn new_node(&mut self, node: PlanNode, output_schema: SchemaRef) -> NodeId {
        self.nodes.push(node);
        self.output_schemas.push(output_schema.clone());
        self.usages.push(HashSet::new());
        self.nodes.len() - 1
    }

    pub fn get_output_schema(&self, id: NodeId) -> SchemaRef {
        self.output_schemas[id].clone()
    }

    pub fn add_usage(&mut self, used: NodeId, user: NodeId) {
        self.usages[used].insert(user);
    }

    #[allow(dead_code)]
    pub fn del_usage(&mut self, used: NodeId, user: NodeId) {
        self.usages[used].remove(&user);
    }
}
