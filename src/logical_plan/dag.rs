use std::collections::HashSet;
use crate::logical_plan::node::{NodeId, PlanNode};

#[derive(Debug)]
pub struct Dag {
    nodes: Vec<PlanNode>,
    usages: Vec<HashSet<NodeId>>
}

impl Dag {
    pub fn new() -> Self {
        Dag {nodes: Vec::new(), usages: Vec::new() }
    }

    pub fn get_node(&self, id: NodeId) -> &PlanNode {
        &self.nodes[id]
    }

    pub fn new_node(&mut self, node: PlanNode) -> NodeId {
        self.nodes.push(node);
        self.usages.push(HashSet::new());
        self.nodes.len() - 1
    }

    pub fn add_usage(&mut self, used: NodeId, user: NodeId) {
        self.usages[used].insert(user);
    }

    #[allow(dead_code)]
    pub fn del_usage(&mut self, used: NodeId, user: NodeId) {
        self.usages[used].remove(&user);
    }
}