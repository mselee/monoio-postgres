use bytes::{Buf, BufMut, Bytes, BytesMut};
use local_sync::mpsc;
use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::marker::PhantomData;
use tracing::debug;

use crate::clients::{InnerClient, Responses};
use crate::entities::codec::FrontendMessage;
use crate::ops::simple_query;
use crate::Error;

/// The state machine of CopyBothReceiver
///
/// ```ignore
///       Setup
///         |
///         v
///      CopyBoth
///       /   \
///      v     v
///  CopyOut  CopyIn
///       \   /
///        v v
///      CopyNone
///         |
///         v
///    CopyComplete
///         |
///         v
///   CommandComplete
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyBothState {
    /// The state before having entered the CopyBoth mode.
    Setup,
    /// Initial state where CopyData messages can go in both directions
    CopyBoth,
    /// The server->client stream is closed and we're in CopyIn mode
    CopyIn,
    /// The client->server stream is closed and we're in CopyOut mode
    CopyOut,
    /// Both directions are closed, we waiting for CommandComplete messages
    CopyNone,
    /// We have received the first CommandComplete message for the copy
    CopyComplete,
    /// We have received the final CommandComplete message for the statement
    CommandComplete,
}

/// A CopyBothReceiver is responsible for handling the CopyBoth subprotocol. It ensures that no
/// matter what the users do with their CopyBothDuplex handle we're always going to send the
/// correct messages to the backend in order to restore the connection into a usable state.
///
/// ```ignore
///                                          |
///          <monoio_postgres owned>          |    <userland owned>
///                                          |
///  pg -> Connection -> CopyBothReceiver ---+---> CopyBothDuplex
///                                          |          ^   \
///                                          |         /     v
///                                          |      Sink    Stream
/// ```
pub struct CopyBothReceiver {
    /// Receiver of backend messages from the underlying [Connection](crate::Connection)
    responses: Responses,
    /// Receiver of frontend messages sent by the user using <CopyBothDuplex as Sink>
    sink_receiver: mpsc::bounded::Rx<FrontendMessage>,
    /// Sender of CopyData contents to be consumed by the user using <CopyBothDuplex as Stream>
    stream_sender: mpsc::bounded::Tx<Result<Message, Error>>,
    /// The current state of the subprotocol
    state: CopyBothState,
    /// Holds a buffered message until we are ready to send it to the user's stream
    buffered_message: Option<Result<Message, Error>>,
}

impl CopyBothReceiver {
    pub(crate) fn new(
        responses: Responses,
        sink_receiver: mpsc::bounded::Rx<FrontendMessage>,
        stream_sender: mpsc::bounded::Tx<Result<Message, Error>>,
    ) -> CopyBothReceiver {
        CopyBothReceiver {
            responses,
            sink_receiver,
            stream_sender,
            state: CopyBothState::Setup,
            buffered_message: None,
        }
    }

    /// Convenience method to set the subprotocol into an unexpected message state
    fn unexpected_message(&mut self) {
        self.sink_receiver.close();
        self.buffered_message = Some(Err(Error::unexpected_message()));
        self.state = CopyBothState::CommandComplete;
    }

    /// Processes messages from the backend, it will resolve once all backend messages have been
    /// processed
    async fn poll_backend(&mut self) -> Option<()> {
        use CopyBothState::*;

        loop {
            // Deliver the buffered message (if any) to the user to ensure we can potentially
            // buffer a new one in response to a server message
            if let Some(message) = self.buffered_message.take() {
                // If the receiver has hung up we'll just drop the message
                if let Err(_) = self.stream_sender.send(message).await {}
            }

            match self.responses.next().await {
                Some(Ok(Message::CopyBothResponse(body))) => match self.state {
                    Setup => {
                        self.buffered_message = Some(Ok(Message::CopyBothResponse(body)));
                        self.state = CopyBoth;
                    }
                    _ => self.unexpected_message(),
                },
                Some(Ok(Message::CopyData(body))) => match self.state {
                    CopyBoth | CopyOut => {
                        self.buffered_message = Some(Ok(Message::CopyData(body)));
                    }
                    _ => self.unexpected_message(),
                },
                // The server->client stream is done
                Some(Ok(Message::CopyDone)) => {
                    match self.state {
                        CopyBoth => self.state = CopyIn,
                        CopyOut => self.state = CopyNone,
                        _ => self.unexpected_message(),
                    };
                }
                Some(Ok(Message::CommandComplete(_))) => {
                    match self.state {
                        CopyNone => self.state = CopyComplete,
                        CopyComplete => {
                            self.stream_sender.close();
                            self.sink_receiver.close();
                            self.state = CommandComplete;
                        }
                        _ => self.unexpected_message(),
                    };
                }
                // The server indicated an error, terminate our side if we haven't already
                Some(Err(err)) => {
                    match self.state {
                        Setup | CopyBoth | CopyOut | CopyIn => {
                            self.sink_receiver.close();
                            self.buffered_message = Some(Err(err));
                            self.state = CommandComplete;
                        }
                        _ => self.unexpected_message(),
                    };
                }
                Some(Ok(Message::ReadyForQuery(_))) => match self.state {
                    CommandComplete => {
                        self.sink_receiver.close();
                        self.stream_sender.close();
                    }
                    _ => self.unexpected_message(),
                },
                Some(Ok(_)) => self.unexpected_message(),
                None => return Some(()),
            }
        }
    }

    pub(crate) fn hint(&self) -> usize {
        self.sink_receiver.hint()
    }
}

/// The [Connection](crate::Connection) will keep polling this stream until it is exhausted. This
/// is the mechanism that drives the CopyBoth subprotocol forward
impl Stream for CopyBothReceiver {
    type Item = FrontendMessage;

    async fn next(&mut self) -> Option<Self::Item> {
        use CopyBothState::*;

        loop {
            match self.poll_backend().await {
                Some(()) => {
                    return None;
                }
                None => match self.state {
                    Setup | CopyBoth | CopyIn => match self.sink_receiver.recv().await {
                        Some(msg) => {
                            return Some(msg);
                        }
                        None => match self.state {
                            // The user has cancelled their interest to this CopyBoth query but we're
                            // still in the Setup phase. From this point the receiver will either enter
                            // CopyBoth mode or will receive an Error response from PostgreSQL. When
                            // either of those happens the state machine will terminate the connection
                            // appropriately.
                            Setup => {
                                continue;
                            }
                            CopyBoth => {
                                self.state = CopyOut;
                                let mut buf = BytesMut::new();
                                frontend::copy_done(&mut buf);
                                return Some(FrontendMessage::Raw(buf.freeze()));
                            }
                            CopyIn => {
                                self.state = CopyNone;
                                let mut buf = BytesMut::new();
                                frontend::copy_done(&mut buf);
                                return Some(FrontendMessage::Raw(buf.freeze()));
                            }
                            _ => unreachable!(),
                        },
                    },
                    _ => {
                        continue;
                    }
                },
            }
        }
    }
}

/// A duplex stream for consuming streaming replication data.
///
/// Users should ensure that CopyBothDuplex is dropped before attempting to await on a new
/// query. This will ensure that the connection returns into normal processing mode.
///
/// ```no_run
/// use monoio_postgres::Client;
///
/// async fn foo(client: &Client) {
///   let duplex_stream = client.copy_both_simple::<&[u8]>("..").await;
///
///   // ⚠️ INCORRECT ⚠️
///   client.query("SELECT 1", &[]).await; // hangs forever
///
///   // duplex_stream drop-ed here
/// }
/// ```
///
/// ```no_run
/// use monoio_postgres::Client;
///
/// async fn foo(client: &Client) {
///   let duplex_stream = client.copy_both_simple::<&[u8]>("..").await;
///
///   // ✅ CORRECT ✅
///   drop(duplex_stream);
///
///   client.query("SELECT 1", &[]).await;
/// }
/// ```
pub struct CopyBothDuplex<T> {
    sink_sender: Option<mpsc::bounded::Tx<FrontendMessage>>,
    stream_receiver: mpsc::bounded::Rx<Result<Message, Error>>,
    buf: BytesMut,
    _p: PhantomData<T>,
}

impl<T> Stream for CopyBothDuplex<T> {
    type Item = Result<Bytes, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.stream_receiver.recv().await {
            Some(Ok(Message::CopyData(body))) => Some(Ok(body.into_bytes())),
            Some(Ok(_)) => Some(Err(Error::unexpected_message())),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

impl<T> Sink<T> for CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    type Error = Error;

    async fn send(&mut self, item: T) -> Result<(), Self::Error> {
        let data: Box<dyn Buf + Send> = if item.remaining() > 4096 {
            if self.buf.is_empty() {
                Box::new(item)
            } else {
                Box::new(self.buf.split().freeze().chain(item))
            }
        } else {
            self.buf.put(item);
            if self.buf.len() > 4096 {
                Box::new(self.buf.split().freeze())
            } else {
                return Ok(());
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        self.sink_sender
            .as_ref()
            .unwrap()
            .send(FrontendMessage::CopyData(data))
            .await
            .map_err(|_| Error::closed())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        if !self.buf.is_empty() {
            if self.sink_sender.as_ref().unwrap().is_closed() {
                return Err(Error::closed());
            }
            let data: Box<dyn Buf + Send> = Box::new(self.buf.split().freeze());
            let data = CopyData::new(data).map_err(Error::encode)?;
            self.sink_sender
                .as_ref()
                .unwrap()
                .send(FrontendMessage::CopyData(data))
                .await
                .map_err(|_| Error::closed())?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.flush().await?;
        let _ = self.sink_sender.take();
        Ok(())
    }
}

pub async fn copy_both_simple<T>(
    client: &InnerClient,
    query: &str,
) -> Result<CopyBothDuplex<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy both query {}", query);

    let buf = simple_query::encode(client, query)?;

    let mut handles = client.start_copy_both()?;

    handles
        .sink_sender
        .send(FrontendMessage::Raw(buf))
        .await
        .map_err(|_| Error::closed())?;

    match handles.stream_receiver.recv().await.transpose()? {
        Some(Message::CopyBothResponse(_)) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(CopyBothDuplex {
        stream_receiver: handles.stream_receiver,
        sink_sender: Some(handles.sink_sender),
        buf: BytesMut::new(),
        _p: PhantomData,
    })
}
