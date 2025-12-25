use crate::runtime::core::{LuaContext, LuaRow};
use crate::utils::script::{Script, ScriptType};
use fusion_derive::{MapLogicTask, SinkLogicTask, SrcLogicTask};
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use fusion_unit_sdk::row::utils::RAW_STR;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::units::compute_unit::UnitCreator;
use libc::glob;
use log::{info, warn};
use mlua::{Function, Lua, Table, UserData, UserDataMethods};
use protobuf::EnumOrUnknown;
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default, SrcLogicTask)]
pub struct DebugInputUnitTask {
    /// iterator times
    iter_times: i64,
    /// generate column count
    column_count: i64,
    /// emit data interval, millis
    interval: u64,
}

impl InitUnit for DebugInputUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();

        // default value
        self.iter_times = 3i64;
        self.column_count = 2i64;
        self.interval = 0u64;

        conf.map(|c| {
            // read `times` definition from config, default is 3.
            self.iter_times = c["times"].as_i64().unwrap_or_else(|| self.iter_times);
            // read `column_count` definition from config, default is 2.
            self.column_count = c["column_count"]
                .as_i64()
                .unwrap_or_else(|| self.column_count);
            // read `interval` definition from config, default is 0, means emit data immediately.
            self.interval = c["interval"].as_u64().unwrap_or_else(|| self.interval);
        });
        Ok(())
    }
}

impl SourceUnit for DebugInputUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let id = Option::from(&ctx.unit)
            .map(|u| u.get_id().clone())
            .unwrap_or(String::default());

        let iter_times = self.iter_times;
        let column_count = self.column_count;
        let interval_millis = self.interval;

        Ok(async move {
            for _row_idx in 0..iter_times {
                let mut row = Row::new();

                for col_idx in 0..column_count {
                    let mut c = Column::new();
                    c.index = col_idx as u32;
                    c.field = format!("c{}", col_idx);
                    let mut rng = rand::thread_rng();
                    c.i32_val = rng.gen_range(10000..99999);
                    c.dt = EnumOrUnknown::from(DataType::i32);
                    row.columns.push(c);
                }

                ctx.send(row).await;
                if interval_millis > 0 {
                    tokio::time::sleep(Duration::from_millis(interval_millis)).await;
                }
            }
            Ok(())
        })
    }
}

#[derive(Default, MapLogicTask)]
pub struct DebugMapUnitTask {}

impl InitUnit for DebugMapUnitTask {}

impl MapUnit for DebugMapUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let c = &row.columns;
        Ok(async move {
            ctx.send(row).await;
            Ok(())
        })
    }
}

#[derive(Default, MapLogicTask)]
pub struct MapUnitTask {
    script: Script,
    lua: Arc<Mutex<Lua>>,
}

impl InitUnit for MapUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();
        conf.map(|c| {
            self.script.code = c["script"].as_str().unwrap_or_default().to_string();
            self.script.script_type = c["type"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap_or(ScriptType::Lua);

            match self.script.script_type {
                ScriptType::Lua => {
                    self.init_lua();
                }
            }
        });
        Ok(())
    }
}

const FUSION_LUA_FUNC_NAME: &str = "__fusion_lua_map_func";
impl MapUnitTask {
    fn init_lua(&mut self) {
        let lua = Lua::new();

        let script_code = self.script.code.clone();
        let func_name = FUSION_LUA_FUNC_NAME;
        let chunk = format!(
            r#"
function {func_name}(ctx, data)
  {}
  return true
end"#,
            script_code.lines().collect::<Vec<&str>>().join("\n  ")
        );

        log::debug!("lua code: {}", &chunk);
        lua.load(chunk)
            .exec()
            .expect("failed to eval script function");
        self.lua = Arc::new(Mutex::new(lua));
    }

    async fn init_compute_table<'a>(&self, row: Row) -> Table {
        let row_table = {
            let lua = self.lua.lock().await;
            lua.create_table().expect("failed to create lua table")
        };
        for column in row.columns.clone() {
            let field = column.field;
            match column.dt.unwrap() {
                DataType::unknown => {
                    panic!("unknown data type");
                }
                DataType::i32 => row_table.set(field, column.i32_val).unwrap(),
                DataType::i64 => row_table.set(field, column.i64_val).unwrap(),
                DataType::f32 => row_table.set(field, column.f32_val).unwrap(),
                DataType::f64 => row_table.set(field, column.f64_val).unwrap(),
                DataType::str => row_table.set(field, column.str_val).unwrap(),
                DataType::bool => row_table.set(field, column.bool_val).unwrap(),
                DataType::bytes => row_table.set(field, column.bytes_val).unwrap(),
                DataType::json => row_table.set(field, column.str_val).unwrap(),
            }
        }
        row_table
    }
}

impl MapUnit for MapUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> Result<
        impl Future<Output = Result<(), fusion_unit_sdk::runtime::UnitError>> + Send,
        anyhow::Error,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(Box::pin(async move {
            let func = {
                let lua = self.lua.lock().await;
                let globals = lua.globals();
                globals
                    .get::<Function>(FUSION_LUA_FUNC_NAME)
                    .expect("concatenate a function")
            };

            let ctx = LuaContext::wrap(ctx.clone());
            let arc_row = Arc::new(Mutex::new(row));
            let lua_row = LuaRow::wrap(arc_row).await;
            match func.call_async::<bool>((ctx, lua_row)).await {
                Ok(_) => {}
                Err(err) => {
                    println!("{}", err);
                }
            };
            Ok(())
        }))
    }
}

#[derive(Default)]
pub struct Stats {
    start_time: u64,
    total: u64,
}

#[derive(Default, SinkLogicTask)]
pub struct DebugOutputUnitTask {
    hide_header: bool,
    hide_console: bool,
    show_report: bool,
    stats: Arc<Mutex<Stats>>,
}

impl InitUnit for DebugOutputUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();
        if let Some(c) = conf {
            self.hide_header = c["hide_header"].as_bool().unwrap_or(false);
            self.hide_console = c["hide_console"].as_bool().unwrap_or(false);
            self.show_report = c["show_report"].as_bool().unwrap_or(false);
        }
        Ok(())
    }

    fn on_start<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let stats_arc = self.stats.clone();
        Box::pin(async move {
            let now = SystemTime::now();
            let mut stats = stats_arc.lock().await;
            if stats.start_time == 0 {
                stats.start_time = now
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as u64;
            }
        })
    }
}

impl MapUnit for DebugOutputUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let hide_header = self.hide_header;
        let hide_console = self.hide_console;
        let stats = self.stats.clone();
        Ok(async move {
            if !hide_console {
                let id = ctx.unit.get_id();
                let offset = row.offset;
                if offset == 1 && !hide_header {
                    let columns = &row.columns;
                    let headers = columns
                        .iter()
                        .map(|c| c.field.clone())
                        .collect::<Vec<_>>()
                        .join("\t");
                    if row.mask == RAW_STR {
                        println!(
                            "\x1b[31m[{}->{}]\t#offset\x1b[0m\t{}",
                            &row.source, id, "RAW_STR"
                        );
                    } else {
                        println!(
                            "\x1b[31m[{}->{}]\t#offset\x1b[0m\t{}",
                            &row.source, id, &headers
                        );
                    }
                }

                if row.mask == RAW_STR {
                    println!(
                        "\x1b[31m[{}->{}]\t#{}\x1b[0m\t{}",
                        &row.source,
                        id,
                        offset,
                        String::from_utf8(row.raw.clone()).unwrap()
                    );
                } else {
                    println!(
                        "\x1b[31m[{}->{}]\t#{}\x1b[0m\t{}",
                        &row.source, id, offset, row
                    );
                }
            }
            stats.lock().await.total += 1;
            ctx.send(row).await;
            Ok(())
        })
    }

    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let show_report = self.show_report;
        let id = ctx.unit.get_id();
        let stats_arc = self.stats.clone();

        #[cfg(feature = "trace-logical")]
        warn!("[{}] receive special mask: EOF. FROM [{}]", id, row.source);

        Ok(async move {
            let cts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            if show_report {
                let stats = stats_arc.lock().await;
                let total = stats.total;
                let elapsed = cts - stats.start_time;
                info!(
                    "task id [{}] finished (EOF), processed rows: [{}], elapsed = {}ms",
                    id, total, elapsed
                );
            }
            Ok(())
        })
    }
}
