use fusion_unit_sdk::graph::types::{ComputingEdge, ComputingUnit, UnitConfig};
use petgraph::visit::IntoNeighborsDirected;
use petgraph::{Directed, Direction, Graph};
use serde_json::json;

#[test]
fn it_works() {
    let mut graph: Graph<ComputingUnit, ComputingEdge, Directed> = Graph::new();
    let unit1 = graph.add_node(
        ComputingUnit::new("unit1", "source")
            .with_name("name1")
            .with_config(json!({})),
    );
    let unit2 = graph.add_node(ComputingUnit::new("unit2", "source"));
    let unit3 = graph.add_node(ComputingUnit::new("unit3", "source"));
    let unit4 = graph.add_node(ComputingUnit::new("unit4", "source"));

    let e1 = graph.add_edge(unit1, unit2, ComputingEdge::new("unit1", "unit2"));
    let e2 = graph.add_edge(unit1, unit3, ComputingEdge::new("unit1", "unit3"));
    let e3 = graph.add_edge(unit3, unit4, ComputingEdge::new("unit3", "unit4"));

    let node_count = graph.node_count();
    let edge_count = graph.edge_count();

    println!("node_count = {}, edge_count = {}", node_count, edge_count);

    let outgoing_neighbors = graph.neighbors_directed(unit1, Direction::Outgoing);
    // 打印扇出节点
    for neighbor in outgoing_neighbors {
        println!("Node A has an outgoing edge to node: {:?}", graph[neighbor]);
    }
}
