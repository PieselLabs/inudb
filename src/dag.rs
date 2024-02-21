use std::collections::HashSet;

pub type NodeId = usize;

#[derive(Debug)]
pub struct Dag<Node> {
    nodes: Vec<Node>,
    usages: Vec<HashSet<NodeId>>,
    inputs: Vec<Vec<NodeId>>,
}

impl<Node> Dag<Node> {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            usages: Vec::new(),
            inputs: Vec::new(),
        }
    }

    pub fn get_node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    pub fn new_node(&mut self, node: Node) -> NodeId {
        self.nodes.push(node);
        self.usages.push(HashSet::new());
        self.inputs.push(Vec::new());
        self.nodes.len() - 1
    }

    pub fn add_input(&mut self, node: NodeId, input: NodeId) {
        self.inputs[node].push(input);
        self.usages[input].insert(node);
    }

    pub fn get_inputs(&self, node: NodeId) -> &Vec<NodeId> {
        &self.inputs[node]
    }
}
