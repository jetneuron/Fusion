use fusion_unit_sdk::graph::types::{ComputingUnit, Context, Watermark};
use fusion_unit_sdk::proto::transfer::Row;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{RwLock, broadcast};

pub trait TaskChannel {
    fn subscribe(&self) -> Receiver<Row>;
    fn internal_subscribe(&self) -> Receiver<Row>;

    fn capture_receiver<T>(&self, recv: Receiver<Row>, consumer: T)
    where
        T: Fn(Row, Context) + Send + Sync + 'static;

    fn listening_feedback(&self, recv: Receiver<Row>, watermark: Arc<RwLock<Watermark>>);

    fn write(&self, row: Row);

    fn sender(&self) -> &Sender<Row>;
    fn internal_sender(&self) -> &Sender<Row>;
}

pub struct LocalTaskChannel {
    pub(crate) channel_id: Option<String>,
    pub(crate) internal_channel: (Sender<Row>, Receiver<Row>),
    feedback_channel: (Sender<Row>, Receiver<Row>),
    buffer_size: usize,
}

impl TaskChannel for LocalTaskChannel {
    fn subscribe(&self) -> Receiver<Row> {
        self.internal_channel.0.subscribe()
    }

    fn internal_subscribe(&self) -> Receiver<Row> {
        self.feedback_channel.0.subscribe()
    }

    fn capture_receiver<T>(&self, mut receiver: Receiver<Row>, consumer: T)
    where
        T: Fn(Row, Context) + Send + Sync + 'static,
    {
        // let sender = self.internal_channel.0.clone();
        // let ctx = Context::new(ComputingUnit::default(), sender.clone());
        // let receiver_count = sender.receiver_count();
        // let option = self.channel_id.clone();
        // let channel_id = option.unwrap_or_default();
        //
        // tokio::spawn(async move {
        //     loop {
        //         match receiver.recv().await {
        //             Ok(row) => {
        //                 if receiver_count > 0 {
        //                     let cloned = row.clone();
        //                     consumer(cloned, ctx.clone());
        //                 } else {
        //                     println!("receiver = 0");
        //                 }
        //             }
        //             Err(broadcast::error::RecvError::Lagged(count)) => {
        //                 println!("消费滞后：{}", count);
        //             }
        //             Err(err) => {
        //                 println!("recv error: {}", err);
        //                 break;
        //             }
        //         }
        //     }
        // });
    }

    fn listening_feedback(&self, mut receiver: Receiver<Row>, watermark: Arc<RwLock<Watermark>>) {
        // let sender = self.feedback_channel.0.clone();
        // let receiver_count = sender.receiver_count();
        // let option = self.channel_id.clone();
        // let channel_id = option.unwrap_or_default();
        //
        // tokio::spawn(async move {
        //     loop {
        //         match receiver.recv().await {
        //             Ok(row) => {
        //                 if row.is_watermark() {
        //                     let mut wm = watermark.write().await;
        //                     if wm.level > 0 {
        //                         wm.level -= 1;
        //                     }
        //                 } else if row.is_eof() {
        //                     println!("feedback receive EOF");
        //                 }
        //             }
        //             Err(broadcast::error::RecvError::Lagged(count)) => {
        //                 println!("feedback消费滞后：{}", count);
        //             }
        //             Err(err) => {
        //                 println!("recv error: {}", err);
        //                 break;
        //             }
        //         }
        //     }
        // });
    }

    fn write(&self, row: Row) {
        let sender = &self.internal_channel.0;
        sender.send(row).unwrap();
    }

    fn sender(&self) -> &Sender<Row> {
        &self.internal_channel.0
    }

    fn internal_sender(&self) -> &Sender<Row> {
        &self.feedback_channel.0
    }
}

impl LocalTaskChannel {
    pub fn new() -> Self {
        let buffer_size: usize = 128;
        let feedback_channel_buffer_size = buffer_size;
        LocalTaskChannel {
            channel_id: None,
            internal_channel: broadcast::channel(buffer_size),
            feedback_channel: broadcast::channel(feedback_channel_buffer_size),
            buffer_size,
        }
    }

    pub fn set_channel_id<T>(&mut self, channel_id: T)
    where
        T: Into<String>,
    {
        self.channel_id = Some(channel_id.into());
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }
}

pub struct RemoteTaskChannel {}
