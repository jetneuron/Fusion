use fusion_unit_sdk::proto::transfer::Frame;
use tokio::sync::mpsc;

const CHANNEL_BUFFER: usize = 1024;

/// Per-node channel that holds pre-allocated mpsc senders and receivers.
/// Senders are created at `prepare_outputs()` time (when the outgoing edge
/// count is known). Each `link()` call pops one receiver for the forwarding
/// task on that edge.
pub struct LocalTaskChannel {
    pub(crate) channel_id: Option<String>,
    senders: Vec<mpsc::Sender<Frame>>,
    pending_receivers: std::sync::Mutex<Vec<mpsc::Receiver<Frame>>>,
}

impl LocalTaskChannel {
    pub fn new() -> Self {
        LocalTaskChannel {
            channel_id: None,
            senders: Vec::new(),
            pending_receivers: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn set_channel_id<T: Into<String>>(&mut self, channel_id: T) {
        self.channel_id = Some(channel_id.into());
    }

    /// Pre-allocate `outgoing` mpsc channels. Called during `set_unit`
    /// when the outgoing edge count is known.
    pub fn prepare_outputs(&mut self, outgoing: usize) {
        self.senders.reserve(outgoing);
        let mut receivers = self.pending_receivers.lock().unwrap();
        receivers.reserve(outgoing);
        for _ in 0..outgoing {
            let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
            self.senders.push(tx);
            receivers.push(rx);
        }
    }

    /// Return a clone of all mpsc senders (cheap — `mpsc::Sender` is an `Arc` internally).
    /// Safe to call multiple times (fan-in targets need one context per incoming edge).
    pub fn get_senders(&self) -> Vec<mpsc::Sender<Frame>> {
        self.senders.clone()
    }

    /// Pop one receiver for a forwarding task. Each edge calls this once during `link()`.
    pub fn take_receiver(&self) -> Option<mpsc::Receiver<Frame>> {
        self.pending_receivers.lock().unwrap().pop()
    }
}
