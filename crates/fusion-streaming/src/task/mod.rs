use crate::task::types::UnitTask;

pub mod builtin;
pub mod types;

#[cfg(feature = "filesystem")]
pub mod filesystem_unit;
#[cfg(feature = "http")]
pub mod http_unit;
// pub struct Tasks {}
// impl Tasks {
//     pub fn run(task: Box<DebugInputUnitTask>) {
//         tokio::spawn(async move {
//             task.launch();
//         });
//     }
// }
