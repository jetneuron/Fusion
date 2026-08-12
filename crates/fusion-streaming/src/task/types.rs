use crate::network::channel::LocalTaskChannel;
use fusion_unit_sdk::graph::types::ComputingUnit;
use std::sync::atomic::AtomicI8;
use std::sync::Arc;

pub struct TaskCore {
    pub(crate) channel: Box<LocalTaskChannel>,
    pub(crate) unit: Option<ComputingUnit>,
    /// Remaining upstream sources — decremented on each incoming EOF.
    /// When it reaches ≤ 0, all upstreams are done and `on_eof` fires.
    upstream_remain: Arc<AtomicI8>,
}

impl TaskCore {
    pub(crate) fn new<T: Into<String>>(channel_id: T, upstream_remain: i8) -> Self {
        let mut channel = LocalTaskChannel::new();
        channel.set_channel_id(channel_id);
        TaskCore {
            channel: Box::new(channel),
            unit: None,
            upstream_remain: Arc::new(AtomicI8::new(upstream_remain)),
        }
    }

    pub(crate) fn set_unit(&mut self, unit: ComputingUnit) {
        let outgoing = unit.get_outgoing();
        self.channel.prepare_outputs(outgoing);
        let id = unit.get_id().clone();
        self.unit = Some(unit);
        self.channel.set_channel_id(id);
    }

    pub fn get_unit(&self) -> Option<ComputingUnit> {
        self.unit.clone()
    }

    pub fn get_unit_id(&self) -> String {
        let x = &self.unit;
        let cloned_unit = x.clone();
        cloned_unit.map_or_else(|| String::default(), |u| u.get_id().clone())
    }

    pub fn get_upstream_remain(&self) -> Arc<AtomicI8> {
        Arc::clone(&self.upstream_remain)
    }
}

impl Default for TaskCore {
    fn default() -> Self {
        TaskCore {
            channel: Box::new(LocalTaskChannel::new()),
            unit: None,
            upstream_remain: Arc::new(AtomicI8::new(0)),
        }
    }
}

pub trait UnitTask {
    fn new(unit: ComputingUnit) -> Self;

    fn get_core(&self) -> &TaskCore;
}
