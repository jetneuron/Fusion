use crate::graph::types::{ComputingUnit, TaskContext, Watermark};
use crate::proto::transfer::Row;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{RwLock, broadcast};

pub fn create_dev_context(unit: ComputingUnit) -> (TaskContext, Receiver<Row>) {
    let channel = broadcast::channel(100);
    let sender: Sender<Row> = channel.0;
    let receiver = channel.1;
    let context = TaskContext::new(
        unit,
        sender,
        Arc::new(RwLock::new(Watermark::new(20, 20, 20, 0))),
    );
    (context, receiver)
}
