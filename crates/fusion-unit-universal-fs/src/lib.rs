pub mod core;
pub mod error;

#[cfg(feature = "local-fs")]
pub mod local_filesystem;

use crate::core::{IntoUniversalIO, UniversalIO, UniversalIOConfig};
use crate::error::IOError;
use anyhow::Context;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use serde_json::Value;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::mpsc;

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(UniversalFsUnitPlugin {})
}

pub struct UniversalFsUnitPlugin {}

impl GraphUnitPlugin for UniversalFsUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        UniversalFsUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());

        unit_manifest
    }
    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

pub type BoxedUniversalIO =
    Box<dyn UniversalIO<Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>>>;

#[derive(Default, LogicalTask)]
pub struct UniversalFsUnitTask {
    uri: String,
    formatter: Option<String>,
    separator: Option<String>,
    universal_config: Option<UniversalIOConfig>,
    sink_io_inst: Option<BoxedUniversalIO>,
}

impl InitUnit for UniversalFsUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let is_sink = unit.is_sink();
        unit.get_config()
            .map::<UnitResult<()>, _>(|mut c| {
                self.uri = c.require_string("uri")?;
                self.formatter = c.extract_string("formatter")?;
                self.separator = c.extract_string("separator")?;
                if is_sink {
                    c["write_mode"] = Value::Bool(true);
                }
                self.universal_config = Some(c);
                Ok(())
            })
            .unwrap_or(Ok(()))?;

        if is_sink {
            let source = (
                self.uri.clone(),
                self.universal_config
                    .clone()
                    .unwrap_or(UniversalIOConfig::default()),
            );
            let io: BoxedUniversalIO = source.into_universal_io()?;
            self.sink_io_inst = Some(io);
        }
        Ok(())
    }
}

impl SourceUnit for UniversalFsUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let source = (
            self.uri.clone(),
            self.universal_config
                .clone()
                .unwrap_or(UniversalIOConfig::default()),
        );

        let buffer_size = 256;
        let (tx, mut rx) = mpsc::channel(buffer_size);
        tokio::task::spawn_blocking::<_, anyhow::Result<()>>(move || {
            let universal_io: BoxedUniversalIO = source.into_universal_io()?;
            for row_result in universal_io
                .iter_rows()
                .map_err(|err| UnitError::IOError(err.to_string()))?
            {
                let row =
                    row_result.map_err(|err| UnitError::InvalidateRowFormat(err.to_string()))?;
                tx.blocking_send(row)?;
            }
            Ok(())
        });
        Ok(async move {
            while let Some(row) = rx.recv().await {
                ctx.send(row).await;
            }
            Ok(())
        })
    }
}

impl MapUnit for UniversalFsUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(async move {
            let io = self
                .sink_io_inst
                .as_ref()
                .with_context(|| "I/O Instance unready.")?;
            io.write_row(row)?;
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
        Ok(async move {
            let io = self
                .sink_io_inst
                .as_ref()
                .with_context(|| "I/O Instance unready.")?;
            io.close()?;
            Ok(())
        })
    }
}
