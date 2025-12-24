use crate::network::channel::LocalTaskChannel;
use fusion_unit_sdk::graph::types::{ComputingUnit, Watermark};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TaskCore {
    pub(crate) channel: Box<LocalTaskChannel>,
    pub(crate) unit: Option<ComputingUnit>,
    watermark: Arc<RwLock<Watermark>>,
}

impl TaskCore {
    pub(crate) fn new<T: Into<String>>(channel_id: T, watermark: Watermark) -> Self {
        let mut channel = LocalTaskChannel::new();
        channel.set_channel_id(channel_id);
        TaskCore {
            channel: Box::new(channel),
            unit: None,
            watermark: Arc::new(RwLock::new(watermark)),
        }
    }

    pub(crate) fn set_unit(&mut self, unit: ComputingUnit) {
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

    pub fn get_watermark(&self) -> Arc<RwLock<Watermark>> {
        Arc::clone(&self.watermark)
    }
}

impl Default for TaskCore {
    fn default() -> Self {
        TaskCore {
            channel: Box::new(LocalTaskChannel::new()),
            unit: None,
            watermark: Arc::new(RwLock::new(Watermark::new(80, 60, 20, 0))),
        }
    }
}

pub trait UnitTask {
    fn new(unit: ComputingUnit) -> Self;

    fn get_core(&self) -> &TaskCore;

    // /// We connect this task to target. Create channel and prepare watermark, internal channel.
    // fn link<T>(&self, target: Arc<Mutex<T>>)
    // where
    //     T: UnitTask + MapUnit + Send + 'static,
    // {
    //     let core = self.get_core();
    //     let this_channel = &core.channel;
    //
    //     let binding = target.lock().unwrap();
    //     let target_core = binding.get_core();
    //     let target_channel = &target_core.channel;
    //
    //     let target_cloned = Arc::clone(&target);
    //     let target_receiver = this_channel.subscribe();
    //
    //     let x = &core.unit.clone();
    //     let option = x.clone().unwrap();
    //
    //     // capture the receiver and process by implementation.
    //     target_channel.capture_receiver(target_receiver, move |row, ctx| {
    //         let target_task = target_cloned.lock().unwrap();
    //
    //         let target_core = target_task.get_core();
    //         let target_channel = &target_core.channel;
    //         let feedback = target_channel.internal_sender();
    //
    //         let is_eof = &row.is_eof();
    //         if *is_eof {
    //             target_task.on_eof(&ctx);
    //             feedback.send(Row::eof()).unwrap();
    //             return;
    //         }
    //
    //         // report to sender which offset current is.
    //         let curr_offset = row.offset;
    //
    //         // core process aspect
    //         target_task.compute(row, &ctx);
    //
    //         feedback.send(Row::watermark(curr_offset)).unwrap();
    //     });
    //
    //     let from_target = target_channel.internal_subscribe();
    //     let watermark = core.get_watermark();
    //     this_channel.listening_feedback(from_target, watermark);
    // }
    //
    // fn collector(&self) -> BackpressureSender {
    //     let core = self.get_core();
    //     let ch = &core.channel;
    //     let watermark = core.get_watermark();
    //     BackpressureSender::new(ch.sender().clone(), watermark)
    // }
}
