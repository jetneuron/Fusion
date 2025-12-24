use fusion_streaming::runtime::physical::PhysicalTask;
use fusion_streaming::task::builtin::DebugInputUnitTask;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
pub async fn test_from_logical() {
    let input = DebugInputUnitTask::default();
    let physical: PhysicalTask = input.into();
    let input2 = DebugInputUnitTask::default();
    let physical2: PhysicalTask = input2.into();

    // physical.link(&Arc::new(Mutex::new(physical2)));

    sleep(Duration::from_secs(5)).await;
}
