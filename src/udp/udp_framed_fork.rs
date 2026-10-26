use tokio_util::codec::{Decoder, Encoder};

use tokio::{io::ReadBuf, net::UdpSocket};

use bytes::{BufMut, BytesMut};
use futures::{ready, Sink, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    borrow::Borrow,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use std::{io, mem::MaybeUninit};

/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
/// This file is a fork of:
/// https://github.com/tokio-rs/tokio/blob/tokio-util-0.7.0/tokio-util/src/udp/frame.rs
///
/// Specifically to address this issue:
/// https://github.com/tokio-rs/tokio/issues/4589
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

/// A unified [`Stream`] and [`Sink`] interface to an underlying `UdpSocket`, using
/// the `Encoder` and `Decoder` traits to encode and decode frames.
///
/// Raw UDP sockets work with datagrams, but higher-level code usually wants to
/// batch these into meaningful chunks, called "frames". This method layers
/// framing on top of this socket by using the `Encoder` and `Decoder` traits to
/// handle encoding and decoding of messages frames. Note that the incoming and
/// outgoing frame types may be distinct.
///
/// This function returns a *single* object that is both [`Stream`] and [`Sink`];
/// grouping this into a single object is often useful for layering things which
/// require both read and write access to the underlying object.
///
/// If you want to work more directly with the streams and sink, consider
/// calling [`split`] on the `UdpFramed` returned by this method, which will break
/// them into separate objects, allowing them to interact more easily.
///
/// [`Stream`]: futures_core::Stream
/// [`Sink`]: futures_sink::Sink
/// [`split`]: https://docs.rs/futures/0.3/futures/stream/trait.StreamExt.html#method.split
#[must_use = "sinks do nothing unless polled"]
#[derive(Debug)]
pub struct UdpFramed<C, T = UdpSocket> {
    socket: T,
    codec: C,
    rd: BytesMut,
    wr: BytesMut,
    out_addr: SocketAddr,
    flushed: bool,
    is_readable: bool,
    current_addr: Option<SocketAddr>,
}

const INITIAL_RD_CAPACITY: usize = 64 * 1024;
const INITIAL_WR_CAPACITY: usize = 8 * 1024;

impl<C, T> Unpin for UdpFramed<C, T> {}

impl<C, T> Stream for UdpFramed<C, T>
where
    T: Borrow<UdpSocket>,
    C: Decoder,
{
    type Item = Result<(C::Item, SocketAddr), (C::Error, Option<SocketAddr>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        pin.rd.reserve(INITIAL_RD_CAPACITY);

        loop {
            // Are there still bytes left in the read buffer to decode?
            if pin.is_readable {
                let current_addr = pin
                    .current_addr
                    .expect("will always be set before this line is called");

                if let Some(frame) = pin
                    .codec
                    .decode_eof(&mut pin.rd)
                    .map_err(|e| (e, Some(current_addr)))?
                {
                    return Poll::Ready(Some(Ok((frame, current_addr))));
                }

                // if this line has been reached then decode has returned `None`.
                pin.is_readable = false;
                pin.rd.clear();
            }

            // We're out of data. Try and fetch more data to decode
            let addr = unsafe {
                // Convert `&mut [MaybeUnit<u8>]` to `&mut [u8]` because we will be
                // writing to it via `poll_recv_from` and therefore initializing the memory.
                let buf = &mut *(pin.rd.chunk_mut() as *mut _ as *mut [MaybeUninit<u8>]);
                let mut read = ReadBuf::uninit(buf);
                let ptr = read.filled().as_ptr();
                let res = ready!(pin.socket.borrow().poll_recv_from(cx, &mut read));

                assert_eq!(ptr, read.filled().as_ptr());
                let addr = res.map_err(|e| (C::Error::from(e), None))?;
                pin.rd.advance_mut(read.filled().len());

                // A UDP source port of zero is reserved and must not be used as a
                // response destination. Drop the datagram before decoding it so
                // callers cannot accidentally send a response to that endpoint.
                if addr.port() == 0 {
                    pin.rd.clear();
                    continue;
                }

                addr
            };

            pin.current_addr = Some(addr);
            pin.is_readable = true;
        }
    }
}

impl<I, C, T> Sink<(I, SocketAddr)> for UdpFramed<C, T>
where
    T: Borrow<UdpSocket>,
    C: Encoder<I>,
{
    type Error = C::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.flushed {
            match self.poll_flush(cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: (I, SocketAddr)) -> Result<(), Self::Error> {
        let (frame, out_addr) = item;

        let pin = self.get_mut();

        pin.codec.encode(frame, &mut pin.wr)?;
        pin.out_addr = out_addr;
        pin.flushed = false;

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.flushed {
            return Poll::Ready(Ok(()));
        }

        let Self {
            ref socket,
            ref mut out_addr,
            ref mut wr,
            ..
        } = *self;

        let n = match ready!(socket.borrow().poll_send_to(cx, wr, *out_addr)) {
            Ok(n) => n,
            Err(error) => {
                // A failed datagram must not remain queued. Otherwise the next
                // call to poll_ready() retries this destination indefinitely and
                // prevents later responses from being sent.
                wr.clear();
                self.flushed = true;
                return Poll::Ready(Err(error.into()));
            }
        };

        let wrote_all = n == self.wr.len();
        self.wr.clear();
        self.flushed = true;

        let res = if wrote_all {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to write entire datagram to socket",
            )
            .into())
        };

        Poll::Ready(res)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }
}

impl<C, T> UdpFramed<C, T>
where
    T: Borrow<UdpSocket>,
{
    /// Create a new `UdpFramed` backed by the given socket and codec.
    ///
    /// See struct level documentation for more details.
    pub fn new(socket: T, codec: C) -> UdpFramed<C, T> {
        Self {
            socket,
            codec,
            out_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
            rd: BytesMut::with_capacity(INITIAL_RD_CAPACITY),
            wr: BytesMut::with_capacity(INITIAL_WR_CAPACITY),
            flushed: true,
            is_readable: false,
            current_addr: None,
        }
    }

    /// Returns a reference to the underlying I/O stream wrapped by `Framed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_ref(&self) -> &T {
        &self.socket
    }

    /// Returns a mutable reference to the underlying I/O stream wrapped by `Framed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.socket
    }

    /// Returns a reference to the underlying codec wrapped by
    /// `Framed`.
    ///
    /// Note that care should be taken to not tamper with the underlying codec
    /// as it may corrupt the stream of frames otherwise being worked with.
    pub fn codec(&self) -> &C {
        &self.codec
    }

    /// Returns a mutable reference to the underlying codec wrapped by
    /// `UdpFramed`.
    ///
    /// Note that care should be taken to not tamper with the underlying codec
    /// as it may corrupt the stream of frames otherwise being worked with.
    pub fn codec_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    /// Returns a reference to the read buffer.
    pub fn read_buffer(&self) -> &BytesMut {
        &self.rd
    }

    /// Returns a mutable reference to the read buffer.
    pub fn read_buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.rd
    }

    /// Consumes the `Framed`, returning its underlying I/O stream.
    pub fn into_inner(self) -> T {
        self.socket
    }
}

#[cfg(test)]
mod tests {
    use super::UdpFramed;
    use bytes::{Bytes, BytesMut};
    use futures::SinkExt;
    use std::io;
    use tokio::net::UdpSocket;
    use tokio::time::{timeout, Duration};
    use tokio_util::codec::{Decoder, Encoder};

    #[derive(Default)]
    struct TestCodec;

    impl Decoder for TestCodec {
        type Item = BytesMut;
        type Error = io::Error;

        fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if buffer.is_empty() {
                Ok(None)
            } else {
                Ok(Some(buffer.split()))
            }
        }
    }

    impl Encoder<Bytes> for TestCodec {
        type Error = io::Error;

        fn encode(&mut self, item: Bytes, buffer: &mut BytesMut) -> Result<(), Self::Error> {
            buffer.extend_from_slice(&item);
            Ok(())
        }
    }

    #[tokio::test]
    async fn successful_send_delivers_the_packet() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        let mut framed = UdpFramed::new(sender, TestCodec);

        framed
            .send((Bytes::from_static(b"packet"), receiver_addr))
            .await
            .unwrap();

        let mut buffer = [0; 64];
        let (length, _) = timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
            .await
            .expect("timed out waiting for packet")
            .unwrap();
        assert_eq!(&buffer[..length], b"packet");
    }

    #[tokio::test]
    async fn failed_send_is_dropped_before_the_next_send() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        let invalid_addr = "127.0.0.1:0".parse().unwrap();
        let mut framed = UdpFramed::new(sender, TestCodec);

        assert!(framed
            .send((Bytes::from_static(b"failed"), invalid_addr))
            .await
            .is_err());

        framed
            .send((Bytes::from_static(b"next"), receiver_addr))
            .await
            .unwrap();

        let mut buffer = [0; 64];
        let (length, _) = timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
            .await
            .expect("timed out waiting for packet")
            .unwrap();
        assert_eq!(&buffer[..length], b"next");
    }
}
