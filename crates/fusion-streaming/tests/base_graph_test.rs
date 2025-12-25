use fusion_streaming::graph::core::{LogicalGraph, PetGraph};

#[test]
fn test() {
    use std::net::UdpSocket;
    let socket = UdpSocket::bind("0.0.0.0:12342").unwrap();
    socket
        .send_to("&buf[0..count]".as_ref(), "224.0.0.1:12341")
        .unwrap();
}

#[test]
fn graph_serialize_deserialize() {
    let graph: LogicalGraph = "name: sdasd\nsss: ss".to_string().into();
    println!("graph = {:?}", graph);

    let serialize_type = graph.get_serialize_type().unwrap();

    let cloned_graph = graph.clone();
    let graph_str: String = cloned_graph.into();
    println!("graph serializer type: {:?}\n{}", serialize_type, graph_str);

    println!("json: \n{}\n", graph.to_json().unwrap());
    println!("yaml: \n{}\n", graph.to_yaml().unwrap());

    let petgraph: PetGraph = graph.into();
    println!("{:?}", petgraph);
}

#[test]
fn graph_from_file_path() {
    let graph: LogicalGraph = "file:///Users/nigel/Workspace/code/nigel/Fusion Pro/graph-computing/tests/graphs/example_read_unit.yaml".into();
    println!("graph = {:?}", graph);
    println!("graph_json = \n{}", &graph.to_json().unwrap());
    println!("graph_yaml = \n{}", &graph.to_yaml().unwrap());

    let id2_opt = graph.get_unit("uid2");
    println!("id2 = {:?}", id2_opt);
    let edge = graph.get_edge("eid1");
    println!("edge = {:?}", edge);
}

// #[test]
// fn add_unit_to_graph() {
//     let mut graph = ComputingGraph::new();
//
//     graph = graph.add_unit(ComputingUnit::new("id1", ""));
//     let edge1 = ComputingEdge::new("id1", "id2")
//         .with_config(EdgeConfig::new());
//
//     let mut edge1_updated = edge1.clone();
//     edge1_updated = edge1_updated.with_config(EdgeConfig::new().with_label("test_label"));
//
//     graph = graph.add_edge(edge1);
//     graph = graph.update_edge(edge1_updated);
//     println!("{}", graph.to_yaml().unwrap());
// }
