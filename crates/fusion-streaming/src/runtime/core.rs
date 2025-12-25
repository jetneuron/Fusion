use crate::graph::core::{LogicalGraph, PetGraph};
use crate::runtime::physical::PhysicalTask;
use crate::runtime::plugin::PluginManager;
use fusion_unit_sdk::graph::types::{TaskContext, EdgeCondition, EdgeConfig, Watermark};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use log::{debug, info, log, trace};
use mlua::ffi::lua;
use mlua::{FromLua, Lua, Table, UserData, UserDataMethods, Value};
use petgraph::data::DataMap;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::visit::{Dfs, IntoNeighborsDirected, NodeRef};
use protobuf::EnumOrUnknown;
use serde::Deserializer;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tera::Tera;
use tokio::sync::Mutex;
use tokio::time::Instant;
use url::form_urlencoded::parse;

#[derive(Default)]
pub(crate) struct GraphContext {
    pub(crate) lua: Arc<Mutex<Lua>>,
    pub(crate) tera: Arc<Mutex<Tera>>,
}

pub struct PhysicalGraph {
    pub(crate) logical_graph: LogicalGraph,
    pub(crate) plugin_manager: Arc<Mutex<PluginManager>>,
    pub(crate) graph_lua: Arc<Mutex<Lua>>,
}

impl PhysicalGraph {
    pub fn new(
        logical_graph: LogicalGraph,
        plugin_manager: Arc<Mutex<PluginManager>>,
        graph_lua: Arc<Mutex<Lua>>,
    ) -> Self {
        PhysicalGraph {
            logical_graph,
            plugin_manager,
            graph_lua,
        }
    }

    pub async fn execute(&self) {
        #[cfg(feature = "trace-logical")]
        trace!("Prepare transfer logical graph as pet graph");
        let pet_graph: PetGraph = (&self.logical_graph).clone().into();
        #[cfg(feature = "trace-logical")]
        trace!("{}", self.logical_graph.to_yaml().unwrap());

        let mut context = GraphContext::default();
        context.lua = Arc::clone(&self.graph_lua);
        let graph_context = Arc::new(Mutex::new(context));

        let start_nodes: Vec<NodeIndex> = pet_graph
            .node_indices()
            .filter(|&node| {
                let neighbors = pet_graph.neighbors_directed(node, petgraph::Direction::Incoming);
                neighbors.count() == 0
            })
            .collect();

        let mut physical_map = HashMap::new();
        let indices = pet_graph.node_indices();
        for index in indices {
            let outgoing_count = pet_graph
                .neighbors_directed(index, petgraph::Direction::Outgoing)
                .count();
            let incoming_count = pet_graph
                .neighbors_directed(index, petgraph::Direction::Incoming)
                .count();

            if let Some(unit) = pet_graph.node_weight(index) {
                let unit_id = unit.get_id();
                let unit_type = unit.get_type();
                #[cfg(feature = "trace-physical")]
                trace!(
                    "Initialize physical, id[{unit_id}], type={unit_type}, incoming={incoming_count}, outgoing={outgoing_count}"
                );
                let r#type = unit.get_type();
                let version = unit.get_version();

                let create_result = {
                    let mgr = self.plugin_manager.lock().await;
                    let mut unit_hold_by_logical = unit.clone();
                    unit_hold_by_logical.update_neighbors(outgoing_count, incoming_count);
                    mgr.create_logical_task(unit_hold_by_logical).await
                };

                let logical_task = create_result.expect(
                    format!(
                        "Could not create logical task, type = {}, version = {}",
                        r#type, version
                    )
                    .as_str(),
                );

                let watermark = {
                    // todo: estimate the watermark level
                    if outgoing_count > 0 {
                        Watermark::new(100, 95, 85, incoming_count as i8)
                    } else {
                        Watermark::new(
                            u64::MAX,
                            u64::MAX - 100,
                            u64::MAX - 200,
                            incoming_count as i8,
                        )
                    }
                };
                let physical: PhysicalTask = PhysicalTask::new_with_watermark(
                    logical_task,
                    watermark,
                    graph_context.clone(),
                );
                physical.update_unit(unit.clone()).await;
                physical.init_script_env().await;
                physical_map.insert(index, Arc::new(Mutex::new(physical)));
            }
        }

        let mut join_handles = Vec::new();
        let mut connected_map = HashSet::new();
        for start_node in start_nodes.clone() {
            let mut dfs = Dfs::new(&pet_graph, start_node);
            while let Some(node) = dfs.next(&pet_graph) {
                let curr_node_id = &node.id();
                let current_node = physical_map
                    .get(&node)
                    .expect(format!("Could not find node [{:?}] in map", curr_node_id).as_str());
                let outgoing = pet_graph.neighbors_directed(node, petgraph::Direction::Outgoing);
                for neighbor in outgoing {
                    if connected_map.contains(&(node, neighbor)) {
                        // already connected
                        continue;
                    }

                    let edge_index = pet_graph
                        .find_edge(curr_node_id.clone(), neighbor)
                        .expect("Could not find edge in graph");
                    let edge = pet_graph
                        .edge_weight(edge_index)
                        .expect("Fail to get edge weight");

                    // parse edge condition
                    let edge_condition = edge
                        .get_config()
                        .map(|ec| {
                            serde_json::from_value::<EdgeCondition>(ec["condition"].clone())
                                .map(|s| s)
                        })
                        .filter(|t| t.is_ok())
                        .map(|t| t.unwrap());

                    let target_physical = physical_map
                        .get(&neighbor)
                        .expect(format!("Could not find node [{:?}] in map", neighbor).as_str());

                    let curr_physical = current_node.lock().await;

                    let handle = curr_physical.link(target_physical, edge_condition);
                    join_handles.extend(handle);
                    connected_map.insert((node, neighbor));
                }
            }
        }

        for start_node in start_nodes {
            let current_node = physical_map
                .get(&start_node)
                .expect("Could not find node in map");
            let physical_node = current_node.lock().await;

            let unit = pet_graph
                .node_weight(start_node)
                .expect("Could not find node weight");
            let id = unit.get_id();
            join_handles.push(physical_node.launch());
            #[cfg(feature = "trace-physical")]
            info!("Launched physical id [{}]", id);
        }

        let clock = Instant::now();
        if let Err(err) = futures::future::try_join_all(join_handles).await {
            log::error!("Task Failed! {:?}", err);
        };

        info!(
            "Execute graph had been finished. Elapsed {}ms",
            clock.elapsed().as_millis()
        );
    }
}

pub struct LuaContext {
    context: TaskContext,
}
impl LuaContext {
    pub fn wrap(context: TaskContext) -> Self {
        Self { context }
    }
}

impl UserData for LuaContext {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method_mut("send", |lua, mut this, row: LuaRow| async move {
            let row = row.row.lock().await;
            let cloned_row = row.clone();
            &this.context.send(cloned_row).await;
            Ok(())
        });

        methods.add_async_method_mut("newRow", |lua, mut this, args: ()| async move {
            Ok(LuaRow::new())
        });
    }
}

#[derive(Clone)]
pub struct LuaRow {
    row: Arc<Mutex<Row>>,
    offset: u64,
    field_index: Arc<Mutex<HashMap<String, usize>>>,
}

impl LuaRow {
    pub(crate) async fn wrap(row: Arc<Mutex<Row>>) -> Self {
        let mut lua_row = LuaRow::new();
        let r = row.lock().await;
        let columns = &r.columns;
        let mut field_idx = HashMap::new();
        for (idx, column) in columns.iter().enumerate() {
            field_idx.insert(column.field.clone(), idx);
        }
        lua_row.field_index = Arc::new(Mutex::new(field_idx));
        lua_row.offset = r.offset;
        lua_row.row = row.clone();
        lua_row
    }

    fn new() -> Self {
        Self {
            row: Arc::new(Mutex::new(Row::new())),
            field_index: Arc::new(Mutex::new(HashMap::new())),
            offset: 0,
        }
    }

    async fn update_column(&mut self, field: mlua::String, value: Value) -> mlua::Result<()> {
        let mut col = Column::new();
        col.field = field.to_str().expect("").to_string();
        match value {
            Value::Nil => {
                col.dt = EnumOrUnknown::new(DataType::unknown);
                col.is_null = true;
            }
            Value::Boolean(val) => {
                col.dt = EnumOrUnknown::new(DataType::bool);
                col.bool_val = val;
            }
            Value::LightUserData(val) => unimplemented!("unimplemented type: LightUserData"),
            Value::Integer(val) => {
                col.dt = EnumOrUnknown::new(DataType::i64);
                col.i64_val = val;
            }
            Value::Number(val) => {
                col.dt = EnumOrUnknown::new(DataType::f64);
                col.f64_val = val;
            }
            #[cfg(any(feature = "luau", doc))]
            #[cfg_attr(docsrs, doc(cfg(feature = "luau")))]
            Value::Vector(val) => unimplemented!("unimplemented type: Vector"),
            Value::String(val) => {
                col.dt = EnumOrUnknown::new(DataType::str);
                col.str_val = val.to_str().expect("Fail to as valid string").to_string();
            }
            Value::Table(_) => unimplemented!("unimplemented type: Table"),
            Value::Function(_) => unimplemented!("unimplemented type: Function"),
            Value::Thread(_) => unimplemented!("unimplemented type: Thread"),
            Value::UserData(_) => unimplemented!("unimplemented type: UserData"),
            #[cfg(any(feature = "luau", doc))]
            #[cfg_attr(docsrs, doc(cfg(feature = "luau")))]
            Value::Buffer(_) => unimplemented!("unimplemented type: Buffer"),
            Value::Error(_) => unimplemented!("unimplemented type: Error"),
            _ => unimplemented!("unimplemented type: Other"),
        }

        let f_index_map = self.field_index.lock().await;
        let f_idx = f_index_map.get(&col.field);
        match f_idx {
            None => {
                // new column
                let mut row = self.row.lock().await;
                row.columns.push(col);
            }
            Some(idx) => {
                let mut row = self.row.lock().await;
                row.columns[*idx] = col;
            }
        }
        Ok(())
    }
}

impl UserData for LuaRow {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_meta_function(
            mlua::MetaMethod::Index,
            |lua, (row, key): (LuaRow, String)| async move {
                if key.eq("offset") {
                    return Ok(Value::Integer(row.offset as i64));
                }
                let index = row.field_index.lock().await;
                match index.get(&key) {
                    None => Ok(Value::Nil),
                    Some(idx) => {
                        let r = row.row.lock().await;
                        let c = &r.columns[*idx];
                        match c.dt.unwrap() {
                            DataType::unknown => Ok(Value::Nil),
                            DataType::i32 => Ok(Value::Integer(mlua::Integer::from(c.i32_val))),
                            DataType::i64 => Ok(Value::Integer(mlua::Integer::from(c.i64_val))),
                            DataType::f32 => Ok(Value::Number(mlua::Number::from(c.f32_val))),
                            DataType::f64 => Ok(Value::Number(mlua::Number::from(c.f64_val))),
                            DataType::str => Ok(Value::String(
                                lua.create_string(c.str_val.as_bytes())
                                    .expect("Fail to create string"),
                            )),
                            DataType::bool => Ok(Value::Boolean(c.bool_val)),
                            DataType::bytes => unimplemented!("unimplemented type: Bytes"),
                            DataType::json => Ok(Value::String(
                                lua.create_string(c.str_val.as_bytes())
                                    .expect("Fail to create string"),
                            )),
                        }
                    }
                }
            },
        );

        methods.add_async_meta_function(
            mlua::MetaMethod::NewIndex,
            |lua, (mut row, key, value): (LuaRow, mlua::String, Value)| async move {
                row.update_column(key, value).await
            },
        );

        methods.add_method("clone", |lua, this, ()| {
            let cloned_lua_row = this.clone();
            Ok(cloned_lua_row)
        });
    }
}

impl FromLua for LuaRow {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        if let mlua::Value::UserData(ud) = value {
            Ok(ud.borrow::<LuaRow>()?.clone())
        } else {
            Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "LuaRow".to_string(),
                message: Some("Expected a UserData".to_string()),
            })
        }
    }
}
