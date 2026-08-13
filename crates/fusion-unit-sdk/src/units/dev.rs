use crate::graph::types::{ComputingUnit, TaskContext};
use crate::proto::transfer::Frame;
use tokio::sync::mpsc;

pub fn create_dev_context(unit: ComputingUnit) -> (TaskContext, mpsc::Receiver<Frame>) {
    // Use a large buffer so plugin unit tests that produce many frames
    // before draining don't deadlock (broadcast never blocked).
    let (tx, rx) = mpsc::channel(100_000);
    let states = unit.get_runtime_states().unwrap();
    let context = TaskContext::new(unit, vec![tx], states);
    (context, rx)
}
