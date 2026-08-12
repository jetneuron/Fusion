use crate::graph::types::{ComputingUnit, TaskContext};
use crate::proto::transfer::Row;
use crate::runtime::UnitResult;
use std::future::Future;
use std::pin::Pin;

pub trait LogicalExecuteContext {}

pub trait LogicalTaskMeta {
    fn get_id(&self) -> String {
        unimplemented!("You must implement this func for get the task unique id")
    }

    fn set_id(&mut self, id: String) {
        unimplemented!("You must implement this func for set the task unique id")
    }
}

///
/// Logical task which can describe the business node execution.
///
pub trait LogicalTask: LogicalTaskMeta {
    /// create logical task by provided computing unit configuration.
    fn create(unit: ComputingUnit) -> UnitResult<Box<dyn LogicalTask + Send + Sync>>
    where
        Self: Sized;

    /// internal launch source to emit data.
    fn internal_launch<'life0, 'async_trait>(
        &'life0 self,
        context: *const TaskContext,
    ) -> anyhow::Result<Pin<Box<dyn Future<Output = UnitResult<()>> + Send + 'async_trait>>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;

    /// receive upstream data then emit to downstream after processed.
    fn internal_compute<'life0, 'async_trait>(
        &'life0 self,
        row: *const Row,
        context: *const TaskContext,
    ) -> anyhow::Result<Pin<Box<dyn Future<Output = UnitResult<()>> + Send + 'async_trait>>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;

    /// stream event
    fn event<'life0, 'async_trait>(
        &'life0 self,
        event_type: i32,
        ctx: &'life0 TaskContext,
        row: Row,
        args: Vec<&dyn std::any::Any>,
    ) -> Pin<Box<dyn Future<Output = UnitResult<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
}
