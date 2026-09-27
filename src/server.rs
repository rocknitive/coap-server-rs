use std::fmt::Debug;
use std::hash::Hash;
use std::pin::Pin;

use coap_lite::Packet;
use futures::stream::Fuse;
use futures::{SinkExt, StreamExt};
use log::{error, trace, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

use crate::app::app_handler::AppHandler;
use crate::app::AppBuilder;
use crate::packet_handler::{IntoHandler, PacketHandler};
use crate::transport::{FramedBinding, FramedItem, FramedReadError, Transport, TransportError};

/// Primary server API to configure, bind, and ultimately run the CoAP server.
pub struct CoapServer<Handler, Endpoint> {
    binding: Fuse<Pin<Box<dyn FramedBinding<Endpoint>>>>,
    packet_relay_rx: Receiver<FramedItem<Endpoint>>,
    packet_relay_tx: Sender<FramedItem<Endpoint>>,
    handler: Option<Handler>,
}

impl<Handler, Endpoint: Debug + Send + Clone + 'static> CoapServer<Handler, Endpoint>
where
    Handler: PacketHandler<Endpoint> + Send + 'static,
{
    /// Bind the server to a specific source of incoming packets in a transport-agnostic way.  Most
    /// customers will wish to use [`crate::udp::UdpTransport`].
    pub async fn bind<T: Transport<Endpoint = Endpoint>>(
        transport: T,
    ) -> Result<Self, TransportError> {
        let binding = transport.bind().await?;
        let (packet_tx, packet_rx) = tokio::sync::mpsc::channel(32);
        Ok(Self {
            binding: binding.fuse(),
            packet_relay_rx: packet_rx,
            packet_relay_tx: packet_tx,
            handler: None,
        })
    }

    /// Run the server "forever".  Note that the function may return a fatal error if the server
    /// encounters unrecoverable issues, typically due to programmer error in this crate itself
    /// or transport errors not related to a specific peer.  The intention is that this crate
    /// should be highly reliable and run indefinitely for properly configured use cases.
    pub async fn serve(
        mut self,
        handler: impl IntoHandler<Handler, Endpoint>,
    ) -> Result<(), FatalServerError> {
        let mtu = self.binding.get_ref().mtu();
        self.handler = Some(handler.into_handler(mtu));

        loop {
            tokio::select! {
                event = self.binding.select_next_some() => {
                    self.handle_rx_event(event)?;
                }
                Some(item) = self.packet_relay_rx.recv() => {
                    self.handle_packet_relay(item).await;
                }
            }
        }
    }

    /// Serve an application with reliable-send control and graceful shutdown support.
    pub async fn serve_app(
        mut self,
        mut builder: AppBuilder<Endpoint>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), FatalServerError>
    where
        Endpoint: Ord + Eq + Hash,
    {
        let mtu = self.binding.get_ref().mtu();
        let mut control_rx = builder.take_control_receiver();
        let handler = AppHandler::from_builder(builder, mtu);

        loop {
            tokio::select! {
                event = self.binding.select_next_some() => {
                    self.handle_rx_event(event)?;
                }
                Some(item) = self.packet_relay_rx.recv() => {
                    self.handle_packet_relay(item).await;
                }
                Some(command) = control_rx.recv() => {
                    handler.handle_control(command, self.packet_relay_tx.clone()).await;
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_rx_event(
        &self,
        result: Result<FramedItem<Endpoint>, FramedReadError<Endpoint>>,
    ) -> Result<(), FatalServerError> {
        match result {
            Ok((packet, peer)) => {
                trace!("Incoming packet from {peer:?}: {packet:?}");
                self.do_handle_request(packet, peer)?
            }
            Err((transport_err, peer)) => {
                warn!("Error from {peer:?}: {transport_err}");
                if peer.is_none() {
                    return Err(transport_err.into());
                }
            }
        }

        Ok(())
    }

    fn do_handle_request(&self, packet: Packet, peer: Endpoint) -> Result<(), FatalServerError> {
        let handler = self
            .handler
            .as_ref()
            .ok_or_else(|| FatalServerError::InternalError("handler not set".to_string()))?;
        let reply_stream = Self::gen_and_send_responses(
            handler.clone(),
            self.packet_relay_tx.clone(),
            packet,
            peer,
        );
        tokio::spawn(reply_stream);
        Ok(())
    }

    async fn gen_and_send_responses(
        handler: Handler,
        packet_tx: Sender<FramedItem<Endpoint>>,
        packet: Packet,
        peer: Endpoint,
    ) {
        let mut stream = handler.handle(packet, peer.clone());
        while let Some(response) = stream.next().await {
            let cloned_peer = peer.clone();
            packet_tx
                .send((response, cloned_peer))
                .await
                .expect("packet_rx closed?");
        }
    }

    async fn handle_packet_relay(&mut self, item: FramedItem<Endpoint>) {
        let peer = item.1.clone();
        trace!("Outgoing packet to {:?}: {:?}", peer, item.0);
        if let Err(e) = self.binding.send(item).await {
            error!("Error sending to {peer:?}: {e:?}");
        }
    }
}

/// Fatal error preventing the server from starting or continuing.  Typically the result of
/// programmer error or misconfiguration.
#[derive(thiserror::Error, Debug)]
pub enum FatalServerError {
    /// Programmer error within this crate, file a bug!
    #[error("internal error: {0}")]
    InternalError(String),

    /// Transport error that is not related to any individual peer but would prevent any future
    /// packet exchanges on the transport.  Must abort the server.
    #[error("fatal transport error: {0}")]
    Transport(#[from] TransportError),
}
