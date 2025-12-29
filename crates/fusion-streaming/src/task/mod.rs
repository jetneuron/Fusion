use crate::task::types::UnitTask;

pub mod builtin;
#[cfg(feature = "http")]
pub mod http_unit;
pub mod types;
// pub struct Tasks {}
// impl Tasks {
//     pub fn run(task: Box<DebugInputUnitTask>) {
//         tokio::spawn(async move {
//             task.launch();
//         });
//     }
// }
