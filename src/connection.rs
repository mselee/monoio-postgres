use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::copy_both::CopyBothReceiver;
use crate::copy_in::CopyInReceiver;
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use local_sync::mpsc;
use log::{info, trace};
use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio_codec::Framed;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
    CopyBoth(CopyBothReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::bounded::Tx<BackendMessages>,
}

pub struct Response {
    sender: mpsc::bounded::Tx<BackendMessages>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::unbounded::Rx<Request>,
    pending_request: Option<RequestMessages>,
    pending_responses: VecDeque<BackendMessage>,
    responses: VecDeque<Response>,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncReadRent + AsyncWriteRent + Unpin,
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        receiver: mpsc::unbounded::Rx<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,
            pending_request: None,
            pending_responses,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    async fn poll_response(&mut self) -> Option<Result<BackendMessage, Error>> {
        if let Some(message) = self.pending_responses.pop_front() {
            trace!("retrying pending response");
            return Some(Ok(message));
        }

        self.stream.next().await.map(|r| r.map_err(Error::io))
    }

    async fn poll_read(&mut self) -> Result<Option<AsyncMessage>, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.poll_response().await {
                Some(message) => message,
                None => return Err(Error::closed()),
            };

            let (mut messages, request_complete) = match message? {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => (messages, request_complete),
            };

            let response = match self.responses.pop_front() {
                Some(response) => response,
                None => match messages.next().map_err(Error::parse)? {
                    Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                    _ => return Err(Error::unexpected_message()),
                },
            };

            match response.sender.send(messages).await {
                Ok(()) => {
                    if !request_complete {
                        self.responses.push_front(response);
                    }
                }
                Err(_) => {
                    // we need to keep paging through the rest of the messages even if the receiver's hung up
                    if !request_complete {
                        self.responses.push_front(response);
                    }
                }
            }
        }
    }

    async fn poll_request(&mut self) -> Option<RequestMessages> {
        if let Some(messages) = self.pending_request.take() {
            trace!("retrying pending request");
            return Some(messages);
        }

        if self.receiver.is_closed() {
            return None;
        }

        let request = self.receiver.recv().await?;
        trace!("polled new request");
        self.responses.push_back(Response {
            sender: request.sender,
        });
        Some(request.messages)
    }

    async fn poll_write(&mut self) -> Result<bool, Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(false);
            }

            let request = match self.poll_request().await {
                Some(request) => request,
                None if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    RequestMessages::Single(FrontendMessage::Raw(request.freeze()))
                }
                None => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len()
                    );
                    return Ok(true);
                }
            };

            match request {
                RequestMessages::Single(request) => {
                    self.stream.send(request).await.map_err(Error::io)?;
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    let message = match receiver.next().await {
                        Some(message) => message,
                        None => {
                            trace!("poll_write: finished copy_in request");
                            continue;
                        }
                    };
                    self.stream.send(message).await.map_err(Error::io)?;
                    self.pending_request = Some(RequestMessages::CopyIn(receiver));
                }
                RequestMessages::CopyBoth(mut receiver) => {
                    let message = match receiver.next().await {
                        Some(message) => message,
                        None => {
                            trace!("poll_write: finished copy_both request");
                            continue;
                        }
                    };
                    self.stream.send(message).await.map_err(Error::io)?;
                    self.pending_request = Some(RequestMessages::CopyBoth(receiver));
                }
            }
        }
    }

    async fn poll_flush(&mut self) -> Result<(), Error> {
        Sink::flush(&mut self.stream).await.map_err(Error::io)
    }

    async fn poll_shutdown(&mut self) -> Result<(), Error> {
        self.stream.close().await.map_err(Error::io)
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    ///
    /// Return values of `None` or `Some(Err(_))` are "terminal"; callers should not invoke this method again after
    /// receiving one of those values.
    pub async fn poll_message(&mut self) -> Option<Result<AsyncMessage, Error>> {
        let message = self.poll_read().await;
        if let Err(err) = message {
            return Some(Err(err));
        }
        let want_flush = self.poll_write().await;
        match want_flush {
            Ok(true) => {
                if let Err(err) = self.poll_flush().await {
                    return Some(Err(err));
                }
            }
            _ => {}
        }
        match message.unwrap() {
            Some(message) => Some(Ok(message)),
            None => match self.poll_shutdown().await {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            },
        }
    }
}

impl<S, T> Stream for Connection<S, T>
where
    S: AsyncReadRent + AsyncWriteRent + Unpin,
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    type Item = Result<AsyncMessage, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        let message = self.poll_message().await;
        if let Some(Ok(AsyncMessage::Notice(notice))) = message {
            info!("{}: {}", notice.severity(), notice.message());
            Some(Ok(AsyncMessage::Notice(notice)))
        } else {
            message
        }
    }
}
