use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use petgraph::{Directed, Graph};
use url::Url;

use fusion_unit_sdk::graph::types::{
    ComputingEdge, ComputingUnit, GraphDescription, SerializeType,
};

pub type PetGraph = Graph<ComputingUnit, ComputingEdge, Directed>;

///
/// Core Computing Graph.
///
///
/// ```
/// use fusion_streaming::graph::core::LogicalGraph;
/// let graph: LogicalGraph = "name: foo".to_string().into();
///
/// let graph_json: String = graph.to_json().unwrap();
/// let graph_yaml: String = graph.to_yaml().unwrap();
/// ```
///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct LogicalGraph {
    /// name of current graph
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    /// serializer type. [`SerializeType`]
    #[serde(skip)]
    pub(crate) serialize_type: Option<SerializeType>,

    /// description of current graph
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) graph_description: Option<GraphDescription>,
    /// units that represent nodes of the graph.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) units: Option<Vec<ComputingUnit>>,
    /// edges between units of the graph
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) edges: Option<Vec<ComputingEdge>>,
}

impl LogicalGraph {
    pub fn new() -> Self {
        Default::default()
    }

    fn with_serialize_type(mut self, r#type: SerializeType) -> Self {
        self.serialize_type = Some(r#type);
        self
    }

    pub fn get_serialize_type(&self) -> Option<SerializeType> {
        self.serialize_type.clone()
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn to_yaml(&self) -> serde_yaml::Result<String> {
        serde_yaml::to_string(self)
    }

    pub fn add_unit(mut self, unit: ComputingUnit) -> Self {
        match self.units {
            None => {
                self.units = Some(vec![unit]);
            }
            Some(mut units) => {
                units.push(unit);
                self.units = Some(units);
            }
        }
        self
    }

    pub fn get_unit<T>(&self, id: T) -> Option<&ComputingUnit>
    where
        T: Into<String> + Clone,
    {
        match &self.units {
            None => None,
            Some(units) => {
                // find the unit which unit id matched.
                units.iter().find(|t| id.clone().into().eq(t.get_id()))
            }
        }
    }

    pub fn add_edge(mut self, edge: ComputingEdge) -> Self {
        match self.edges {
            None => self.edges = Some(vec![edge]),
            Some(mut edges) => {
                edges.push(edge);
                self.edges = Some(edges);
            }
        }
        self
    }

    pub fn get_edge<T>(&self, id: T) -> Option<&ComputingEdge>
    where
        T: Into<String> + Clone,
    {
        match &self.edges {
            None => None,
            Some(edges) => edges.iter().find(|e| id.clone().into().eq(e.get_id())),
        }
    }

    pub fn update_edge(mut self, edge: ComputingEdge) -> Self {
        match self.edges {
            None => {
                panic!();
            }
            Some(mut edges) => {
                if let Some(el) = edges.iter_mut().find(|e| e.get_id() == edge.get_id()) {
                    *el = edge;
                }
                self.edges = Some(edges);
            }
        }
        self
    }

    fn from_str_content(value: &str) -> LogicalGraph {
        // whether json or yaml formatted
        match serde_yaml::from_str::<LogicalGraph>(value) {
            Ok(json_graph) => json_graph.with_serialize_type(SerializeType::Yaml),
            Err(e1) => match serde_json::from_str::<LogicalGraph>(value) {
                Ok(yaml_graph) => yaml_graph.with_serialize_type(SerializeType::Json),
                Err(err) => {
                    panic!("error: {:?}, e1: {:?}, content: \n{}", e1, err, value)
                }
            },
        }
    }
}

impl From<&str> for LogicalGraph {
    fn from(value: &str) -> Self {
        // whether io schema, such as: file://; ftp://; https://
        match Url::parse(value) {
            Ok(parsed) => {
                let schema = parsed.scheme();
                let cloned_parsed = parsed.clone();
                match schema {
                    "file" => match cloned_parsed.to_file_path() {
                        Ok(path) => match File::open(path) {
                            Ok(mut file) => {
                                let mut contents = String::new();
                                file.read_to_string(&mut contents).unwrap();
                                Self::from_str_content(contents.as_str())
                            }
                            Err(_) => panic!("could not read file: {}", value),
                        },
                        Err(_) => Self::from_str_content(value),
                    },
                    &_ => Self::from_str_content(value),
                }
            }
            Err(_) => Self::from_str_content(value),
        }
    }
}

impl From<String> for LogicalGraph {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

impl From<LogicalGraph> for String {
    fn from(value: LogicalGraph) -> Self {
        let serialize = value.get_serialize_type().expect("");
        match serialize {
            SerializeType::Json => serde_json::to_string(&value).expect(""),
            SerializeType::Yaml => serde_yaml::to_string(&value).expect(""),
        }
    }
}

/// transfer [`LogicalGraph`] to [`Graph<ComputingUnit, ComputingEdge, Directed>`] which type alias [`PetGraph`]
/// ```
/// use fusion_streaming::graph::core::{LogicalGraph, PetGraph};
/// let graph = LogicalGraph::from("");
///
/// let petgraph: PetGraph = graph.into();
/// ```
///
impl From<LogicalGraph> for PetGraph {
    fn from(graph: LogicalGraph) -> Self {
        let mut petgraph = Graph::new();

        let mut index_map = HashMap::new();
        if let Some(units) = graph.units {
            for unit in units {
                let unit_id = unit.get_id().clone();
                let node_index = petgraph.add_node(unit);
                index_map.insert(unit_id, node_index);
            }
        }

        if let Some(edges) = graph.edges {
            for edge in edges {
                let source = edge.get_source();
                let target = edge.get_target();

                let source_node = index_map.get(&source);
                let target_node = index_map.get(&target);

                if let (Some(source_idx), Some(target_idx)) = (source_node, target_node) {
                    petgraph.add_edge(source_idx.clone(), target_idx.clone(), edge);
                }
            }
        }
        petgraph
    }
}
