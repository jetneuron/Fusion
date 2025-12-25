#[macro_use]
extern crate serde_derive;

use std::fmt::Debug;

use petgraph::Graph;
use petgraph::dot::Dot;
use serde_derive::{Deserialize, Serialize};

mod engine;
pub mod graph;
pub mod network;
pub mod runtime;
pub mod storage;
pub mod task;
pub mod utils;

#[derive(
    Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug, Default,
)]
struct Vertex<'a> {
    label: &'a str,
    x: i32,
    y: i32,
}

#[derive(
    Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug, Default,
)]
struct Edge<'a> {
    label: &'a str,
}

type ComputingGraph<'a> = Graph<Vertex<'a>, Edge<'a>>;

pub struct EngineBuilder {
    id: String,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            id: String::default(),
        }
    }

    pub fn id(mut self, id: &str) -> Self {
        self.id = String::from(id);
        self
    }

    pub fn build(&self) -> GraphComputingEngine {
        GraphComputingEngine {}
    }
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GraphComputingEngine {}

impl GraphComputingEngine {
    pub fn setup(&self) {
        println!("Engine was running.")
    }
}

impl GraphComputingEngine {
    fn new() -> Self {
        GraphComputingEngine {}
    }

    fn submit_computing(&self, graph: ComputingGraph) {
        let dot = Dot::new(&graph);
        println!("{:?}", dot);
    }
}

#[test]
fn test_engine_launch() {
    let engine = EngineBuilder::default().id("test-engine").build();
    engine.setup();

    let mut graph = Graph::<Vertex, Edge>::new();
    let n1 = graph.add_node(Vertex {
        label: "1",
        ..Vertex::default()
    });
    let n2 = graph.add_node(Vertex {
        label: "2",
        ..Vertex::default()
    });

    graph.add_edge(
        n1,
        n2,
        Edge {
            label: "e1",
            ..Edge::default()
        },
    );

    engine.submit_computing(graph);
}
