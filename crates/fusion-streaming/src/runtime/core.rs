use crate::graph::core::{LogicalGraph, PetGraph};
use crate::runtime::physical::PhysicalTask;
use crate::runtime::plugin::PluginManager;
use crate::runtime::scripts::{GraphLua, GraphTera};
use fusion_unit_sdk::graph::types::{EdgeCondition, EdgeConfig, TaskContext, Watermark};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use fusion_unit_sdk::runtime::state::GraphStates;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use itertools::{cloned, Itertools};
use log::{debug, info, log, trace};
use mlua::ffi::lua;
use mlua::{FromLua, Lua, Table, UserData, UserDataMethods, Value};
use petgraph::data::DataMap;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::visit::{Dfs, IntoNeighborsDirected, NodeRef};
use protobuf::EnumOrUnknown;
use serde::Deserializer;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tera::{Context, Tera};
use tokio::sync::Mutex;
use tokio::time::Instant;
use url::form_urlencoded::parse;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LaunchEnv {
    params: Option<serde_json::Value>,
    env: Option<serde_json::Value>,
}

impl LaunchEnv {
    pub fn as_tera_context(&self) -> Context {
        let mut context = Context::new();
        if let Some(serde_json::Value::Object(map)) = self.params.clone() {
            for (k, v) in map {
                if let serde_json::Value::String(str) = v {
                    context.insert(k, &str);
                }
            }
        };
        if let Some(serde_json::Value::Object(map)) = self.env.clone() {
            for (k, v) in map {
                if let serde_json::Value::String(str) = v {
                    context.insert(k, &str);
                }
            }
        }
        context
    }

    pub fn update_params(&mut self, params: Option<serde_json::Value>) {
        self.params = params;
    }

    pub fn update_env(&mut self, env: Option<serde_json::Value>) {
        self.env = env;
    }

    pub async fn runtime_env(&mut self, tera: Arc<Mutex<Tera>>) -> UnitResult<()> {
        let before_env = self.env.clone();
        if let Some(serde_json::Value::Object(map)) = before_env {
            let runtime = Self::calculate_runtime(tera, map).await?;
            self.env = Some(runtime);
        }
        Ok(())
    }

    pub async fn runtime_params(&mut self, tera: Arc<Mutex<Tera>>) -> UnitResult<()> {
        let before_params = self.params.clone();
        if let Some(serde_json::Value::Object(map)) = before_params {
            let runtime = Self::calculate_runtime(tera, map).await?;
            self.params = Some(runtime);
        }
        Ok(())
    }

    async fn calculate_runtime(
        tera: Arc<Mutex<Tera>>,
        map: serde_json::map::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, UnitError> {
        let mut runtime = serde_json::Value::default();
        for (key, value) in map {
            match value {
                serde_json::Value::Null => {}
                serde_json::Value::Bool(val) => runtime[key] = serde_json::Value::Bool(val),
                serde_json::Value::Number(num) => runtime[key] = serde_json::Value::from(num),
                serde_json::Value::String(str) => {
                    let mut tera = tera.lock().await;
                    let runtime_value =
                        tera.render_str(&str, &tera::Context::new())
                            .map_err(|err| {
                                UnitError::config_parse_error(format!(
                                    "Fail to parse env runtime: {}, origin_value = {}, err = {}",
                                    key,
                                    str,
                                    err.to_string()
                                ))
                            })?;
                    runtime[key] = serde_json::Value::from(runtime_value);
                }
                serde_json::Value::Array(arr) => {
                    runtime[key] = serde_json::Value::Array(arr);
                }
                serde_json::Value::Object(obj) => {
                    runtime[key] = serde_json::Value::Object(obj);
                }
            };
        }
        Ok(runtime)
    }

    pub fn merge(mut self, other: Option<LaunchEnv>) -> LaunchEnv {
        if let Some(other) = other {
            if let Some(params) = other.params {
                let after_merged = match self.params {
                    None => params,
                    Some(mut exists_params) => {
                        Self::merge_values(&mut exists_params, params);
                        exists_params
                    }
                };
                self.params = Some(after_merged);
            }
            if let Some(env) = other.env {
                let after_merged = match self.env {
                    None => env,
                    Some(mut exists_env) => {
                        Self::merge_values(&mut exists_env, env);
                        exists_env
                    }
                };
                self.env = Some(after_merged);
            }
        }
        self
    }

    fn merge_values(a: &mut serde_json::Value, b: serde_json::Value) {
        match (a, b) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                for (k, v) in b {
                    if let Some(existing) = a.get_mut(&k) {
                        Self::merge_values(existing, v);
                    } else {
                        a.insert(k, v);
                    }
                }
            }
            (a, b) => *a = b,
        }
    }
}

pub struct PhysicalGraph {
    pub(crate) logical_graph: LogicalGraph,
    pub(crate) plugin_manager: Arc<Mutex<PluginManager>>,
    pub(crate) graph_lua: Arc<Mutex<Lua>>,
    pub(crate) tera: Arc<Mutex<Tera>>,
}

impl PhysicalGraph {
    pub fn new(
        logical_graph: LogicalGraph,
        plugin_manager: Arc<Mutex<PluginManager>>,
        graph_lua: Arc<Mutex<Lua>>,
        tera: Arc<Mutex<Tera>>,
    ) -> Self {
        PhysicalGraph {
            logical_graph,
            plugin_manager,
            graph_lua,
            tera,
        }
    }

    pub async fn execute(&self, launch_env: Option<LaunchEnv>) -> UnitResult<()> {
        let mut runtime_launch_env = if let Some(env) = self.logical_graph.env.clone() {
            env.merge(launch_env)
        } else {
            launch_env.unwrap_or_default()
        };
        runtime_launch_env.runtime_env(self.tera.clone()).await?;
        runtime_launch_env.runtime_params(self.tera.clone()).await?;

        #[cfg(feature = "trace-physical")]
        debug!("launch_env: {}", json!(runtime_launch_env).to_string());

        #[cfg(feature = "trace-logical")]
        {
            trace!("Prepare transfer logical graph as pet graph");
            trace!("{}", self.logical_graph.to_yaml().unwrap());
        }

        let graph_id = self.logical_graph.get_id();
        let mut pet_graph: PetGraph = (&self.logical_graph).clone().into();
        let states = GraphStates::new(graph_id);
        states.register(GraphLua(self.graph_lua.clone()))?;
        states.register(GraphTera(self.tera.clone()))?;

        let start_nodes: Vec<NodeIndex> = pet_graph
            .node_indices()
            .filter(|&node| {
                let neighbors = pet_graph.neighbors_directed(node, petgraph::Direction::Incoming);
                neighbors.count() == 0
            })
            .collect();

        let mut physical_map = HashMap::new();
        let indices = pet_graph.node_indices();
        let tera_context = runtime_launch_env.as_tera_context();
        for index in indices {
            let outgoing_count = pet_graph
                .neighbors_directed(index, petgraph::Direction::Outgoing)
                .count();
            let incoming_count = pet_graph
                .neighbors_directed(index, petgraph::Direction::Incoming)
                .count();

            if let Some(mut unit) = pet_graph.node_weight_mut(index) {
                let unit_id = unit.get_id();
                let unit_type = unit.get_type();
                #[cfg(feature = "trace-physical")]
                trace!(
                    "Initialize physical, id[{unit_id}], type={unit_type}, incoming={incoming_count}, outgoing={outgoing_count}"
                );

                let logical_task = {
                    let mgr = self.plugin_manager.lock().await;
                    unit.update_neighbors(outgoing_count, incoming_count);

                    if let Some(conf) = unit.get_config() {
                        let runtime_config = crate::utils::context_var_util::calculate_runtime(
                            self.tera.clone(),
                            tera_context.clone(),
                            conf,
                        )
                        .await?;
                        unit.replace_config(runtime_config);
                    }

                    unit.with_states(states.clone());
                    let cloned_unit = unit.clone();
                    mgr.create_logical_task(cloned_unit).await?
                };

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
                let physical: PhysicalTask =
                    PhysicalTask::new_with_watermark(logical_task, watermark, states.clone());
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
        Ok(())
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
            this.context.send(cloned_row).await;
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
