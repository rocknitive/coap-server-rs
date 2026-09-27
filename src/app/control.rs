use std::fmt::Debug;
use std::hash::Hash;

use coap_lite::Packet;
use tokio::sync::{mpsc, oneshot};

use crate::app::retransmission_manager::SendFailed;

pub(crate) struct ControlCommand<Endpoint> {
    pub(crate) packet: Packet,
    pub(crate) peer: Endpoint,
    pub(crate) result_tx: oneshot::Sender<Result<(), SendFailed>>,
}

#[derive(Clone)]
pub struct AppControl<Endpoint> {
    pub(crate) sender: mpsc::Sender<ControlCommand<Endpoint>>,
}

impl<Endpoint> AppControl<Endpoint> {
    pub(crate) fn new(sender: mpsc::Sender<ControlCommand<Endpoint>>) -> Self {
        Self { sender }
    }
}

impl<Endpoint> AppControl<Endpoint>
where
    Endpoint: Debug + Clone + Eq + Hash + Send + 'static,
{
    /// Send a Confirmable packet and wait for its ACK, RST, or retransmission exhaustion.
    pub async fn send_reliably(&self, packet: Packet, peer: Endpoint) -> Result<(), SendFailed> {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ControlCommand {
                packet,
                peer,
                result_tx,
            })
            .await
            .map_err(|_| SendFailed::InternalError("CoAP server control closed".into()))?;
        result_rx
            .await
            .map_err(|_| SendFailed::InternalError("CoAP transmission task closed".into()))?
    }
}
