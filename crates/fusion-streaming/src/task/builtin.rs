use crate::runtime::core::{LuaContext, LuaFrame};
use crate::runtime::scripts;
use fusion_derive::{MapLogicTask, SinkLogicTask, SrcLogicTask};
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Frame};
use fusion_unit_sdk::frame::types::RAW_STR;
use fusion_unit_sdk::runtime::logical::LogicalTaskMeta;
use fusion_unit_sdk::runtime::script::{Script, Scripter, script_registry};
use fusion_unit_sdk::runtime::script_engine_factory::Product;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::units::compute_unit::UnitCreator;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
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

/// Data generation mode for DebugInputUnitTask.
#[derive(Default)]
enum GenMode {
    #[default]
    Random,
    Ascending,
    Descending,
}

impl GenMode {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "ascending" | "asc" => Self::Ascending,
            "descending" | "desc" => Self::Descending,
            _ => Self::Random,
        }
    }
}

#[derive(Default, SrcLogicTask)]
pub struct DebugInputUnitTask {
    meta: UnitMeta,
    iter_times: i64,
    column_count: i64,
    interval: u64,
    /// Data generation mode: random | ascending | descending.
    gen_mode: GenMode,
}

impl InitUnit for DebugInputUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();

        self.iter_times = 3i64;
        self.column_count = 2i64;
        self.interval = 0u64;

        conf.map(|c| {
            self.iter_times = c["times"].as_i64().unwrap_or(self.iter_times);
            self.column_count = c["column_count"].as_i64().unwrap_or(self.column_count);
            self.interval = c["interval"].as_u64().unwrap_or(self.interval);
            self.gen_mode = c["gen_mode"]
                .as_str()
                .map(GenMode::from_str)
                .unwrap_or_default();
        });
        Ok(())
    }
}

impl SourceUnit for DebugInputUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let iter_times = self.iter_times;
        let column_count = self.column_count;
        let interval_millis = self.interval;
        let gen_mode = match self.gen_mode {
            GenMode::Random => 0,
            GenMode::Ascending => 1,
            GenMode::Descending => 2,
        };

        Ok(async move {
            for row_idx in 0..iter_times {
                let mut frame = Frame::new();
                for col_idx in 0..column_count {
                    let mut c = Column::new();
                    c.index = col_idx as u32;
                    c.field = format!("c{}", col_idx);
                    let val: i32 = match gen_mode {
                        1 => (row_idx + 1) as i32 * ((col_idx + 1) as i32 * 1000),
                        2 => (iter_times - row_idx) as i32 * ((col_idx + 1) as i32 * 1000),
                        _ => rand::thread_rng().gen_range(10000..99999),
                    };
                    c.i32_val = val;
                    c.dt = EnumOrUnknown::from(DataType::i32);
                    frame.columns.push(c);
                }
                ctx.send(frame).await;
                if interval_millis > 0 {
                    tokio::time::sleep(Duration::from_millis(interval_millis)).await;
                }
            }
            Ok(())
        })
    }
}

#[derive(Default, MapLogicTask)]
pub struct DebugMapUnitTask {
    meta: UnitMeta,
}

impl InitUnit for DebugMapUnitTask {}

impl MapUnit for DebugMapUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let c = &frame.columns;
        Ok(async move {
            ctx.send(frame).await;
            Ok(())
        })
    }
}

#[derive(Default, MapLogicTask)]
pub struct MapUnitTask {
    meta: UnitMeta,
    script: Script,
    lua: Arc<Mutex<Lua>>,
    scripter: Option<Arc<Mutex<Box<dyn Scripter + Send>>>>,
}

impl InitUnit for MapUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();
        if let Some(result) = conf.map::<UnitResult<()>, _>(|c| {
            let script = c.require_string("$script")?;
            let states = unit
                .get_runtime_states()
                .ok_or_else(|| UnitError::ScriptInitErr(String::from("runtime states unready")))?;
            let script_type = c
                .extract_string("$script_type")?
                .map(|v| v.to_lowercase())
                .unwrap_or_else(|| String::from("lua"));

            self.scripter = Some(script_registry::create_scripter(
                &script_type,
                script,
                states,
            ));
            Ok(())
        }) {
            result?;
        }
        Ok(())
    }
}

impl MapUnitTask {
    async fn init_compute_table<'a>(&self, frame: Frame) -> Table {
        let row_table = {
            let lua = self.lua.lock().await;
            lua.create_table().expect("failed to create lua table")
        };
        for column in frame.columns.clone() {
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
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> Result<impl Future<Output = Result<(), UnitError>> + Send, anyhow::Error>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(Box::pin(async move {
            let scripter = self
                .scripter
                .as_ref()
                .ok_or(UnitError::Unknown(String::from("Scripter not initialized")))?;
            let states = ctx.states.clone();
            let id = self.meta.get_id();
            // Take the eval future under the lock, then drop the guard
            // before awaiting — parallel workers don't serialize here.
            let eval_fut = {
                let scripter = scripter.lock().await;
                scripter.frame_eval(&id, states, ctx, frame)
            };
            eval_fut.await?;
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
    meta: UnitMeta,
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
        frame: Frame,
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
                let offset = frame.offset;
                if offset == 1 && !hide_header {
                    let columns = &frame.columns;
                    let mut headers = String::new();
                    let mut types = String::new();

                    for column in columns {
                        headers.push_str(&format!("{}\t", column.field));
                        types.push_str(&format!(
                            "{:?}\t",
                            column.dt.enum_value_or(DataType::unknown)
                        ));
                    }
                    if !headers.is_empty() {
                        headers.remove(headers.len() - 1);
                        types.remove(types.len() - 1);
                    }
                    if frame.mask == RAW_STR {
                        println!(
                            "\x1b[31m[{}->{}]\t#frame\x1b[0m\t{}",
                            &frame.source, id, "RAW_STR"
                        );
                    } else {
                        println!(
                            "\x1b[31m[{}->{}]\t#frame\x1b[0m\t{}",
                            &frame.source, id, &headers
                        );
                        println!(
                            "\x1b[31m[{}->{}]\t#type\x1b[0m\t{}",
                            &frame.source, id, &types
                        )
                    }
                }

                if frame.mask == RAW_STR {
                    println!(
                        "\x1b[31m[{}->{}]\t#{}\x1b[0m\t{}",
                        &frame.source,
                        id,
                        offset,
                        String::from_utf8(frame.raw.clone()).unwrap()
                    );
                } else {
                    println!(
                        "\x1b[31m[{}->{}]\t#{}\x1b[0m\t{}",
                        &frame.source, id, offset, frame
                    );
                }
            }
            stats.lock().await.total += 1;
            ctx.send(frame).await;
            Ok(())
        })
    }

    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
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
        warn!("[{}] receive special mask: EOF. FROM [{}]", id, frame.source);

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
                    "task id [{}] finished (EOF), processed frames: [{}], elapsed = {}ms",
                    id, total, elapsed
                );
            }
            Ok(())
        })
    }
}
