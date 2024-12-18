use crate::entities::codec::{BackendMessage, FrontendMessage, PostgresCodec};
use crate::error::DbError;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use local_sync::{mpsc, oneshot};
use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use monoio::io::{
    AsyncWriteRent, CancelableAsyncReadRent, CancelableAsyncWriteRent, Canceller, OwnedReadHalf,
    OwnedWriteHalf, Splitable,
};
use monoio_codec::{FramedRead, FramedWrite};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::cell::{RefCell, UnsafeCell};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use tracing::{debug, info, trace};

use super::{Connection, Request, RequestMessages, Response, State};

struct Shared {
    state: State,
    responses: VecDeque<Response>,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `RawConnection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
pub struct RawConnection<S: CancelableAsyncReadRent + CancelableAsyncWriteRent> {
    shared: Rc<RefCell<Shared>>,
    incoming: FramedRead<OwnedReadHalf<S>, PostgresCodec>,
    pending_responses: VecDeque<BackendMessage>,
    shutdown: Option<oneshot::Sender<()>>,
    parameters: HashMap<String, String>,
}

struct Writer<S: CancelableAsyncReadRent + CancelableAsyncWriteRent> {
    outgoing: FramedWrite<OwnedWriteHalf<S>, PostgresCodec>,
    receiver: mpsc::unbounded::Rx<Request>,
    pending_request: Option<RequestMessages>,
    shared: Rc<RefCell<Shared>>,
}

impl<S> Writer<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent,
{
    async fn run(&mut self) -> Result<(), Error> {
        loop {
            let want_flush = self.write().await;
            match want_flush {
                Ok(true) => {
                    if let Err(err) = Sink::flush(&mut self.outgoing).await {
                        return Err(Error::io(err));
                    }
                }
                _ => {}
            }

            trace!("writer: check state");
            if self.shared.borrow().state == State::Closing {
                trace!("poll_write: done");
                return Ok(());
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        trace!("writer: shutting down...");
        self.outgoing.shutdown().await.map_err(Error::io)
    }

    async fn poll_request(&mut self) -> Option<RequestMessages> {
        if let Some(messages) = self.pending_request.take() {
            trace!("retrying pending request");
            return Some(messages);
        }

        if self.receiver.is_closed() {
            return None;
        }

        trace!("request: poll...");
        let request = self.receiver.recv().await?;
        trace!("polled new request");
        self.shared.borrow_mut().responses.push_back(Response {
            sender: request.sender,
        });
        Some(request.messages)
    }

    async fn write(&mut self) -> Result<bool, Error> {
        loop {
            trace!("writer: poll_request");
            let request = match self.poll_request().await {
                Some(request) => request,
                None => {
                    trace!("poll_write: at eof, terminating");
                    self.shared.borrow_mut().state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    RequestMessages::Single(FrontendMessage::Raw(request.freeze()))
                }
            };

            match request {
                RequestMessages::Single(message) => {
                    trace!("frontend: single");
                    self.outgoing.send(message).await.map_err(Error::io)?;
                    if self.receiver.hint() == 0 {
                        Sink::flush(&mut self.outgoing).await.map_err(Error::io)?;
                    }
                    trace!("frontend: sent");
                    if self.shared.borrow().state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.shared.borrow_mut().state = State::Closing;
                        return Ok(false);
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    trace!("request: copy-in");
                    let message = match receiver.next().await {
                        Some(message) => message,
                        None => {
                            trace!("poll_write: finished copy_in request");
                            if self.receiver.hint() == 0 {
                                Sink::flush(&mut self.outgoing).await.map_err(Error::io)?;
                            }
                            continue;
                        }
                    };
                    self.outgoing.send(message).await.map_err(Error::io)?;
                    if receiver.hint() == 0 {
                        Sink::flush(&mut self.outgoing).await.map_err(Error::io)?;
                    }
                    self.pending_request = Some(RequestMessages::CopyIn(receiver));
                }
                RequestMessages::CopyBoth(mut receiver) => {
                    trace!("request: copy-both");
                    let message = match receiver.next().await {
                        Some(message) => message,
                        None => {
                            trace!("poll_write: finished copy_both request");
                            if self.receiver.hint() == 0 {
                                Sink::flush(&mut self.outgoing).await.map_err(Error::io)?;
                            }
                            continue;
                        }
                    };
                    self.outgoing.send(message).await.map_err(Error::io)?;
                    if receiver.hint() == 0 {
                        Sink::flush(&mut self.outgoing).await.map_err(Error::io)?;
                    }
                    self.pending_request = Some(RequestMessages::CopyBoth(receiver));
                }
            }
        }
    }
}

impl<S: CancelableAsyncReadRent + CancelableAsyncWriteRent + Splitable + 'static> Connection<S>
    for RawConnection<S>
{
    fn new(
        stream: S,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        receiver: mpsc::unbounded::Rx<Request>,
    ) -> (oneshot::Sender<()>, Self) {
        let shared = Rc::new(UnsafeCell::new(stream));
        let (r, w) = (OwnedReadHalf(shared.clone()), OwnedWriteHalf(shared));
        let (shutdown_internal_tx, shutdown_internal_rx) = oneshot::channel();
        let (shutdown_external_tx, shutdown_external_rx) = oneshot::channel();
        let shared = Shared {
            state: State::Active,
            responses: VecDeque::new(),
        };

        let canceller = Canceller::new();
        let handle = canceller.handle();

        let shared = Rc::from(RefCell::from(shared));

        let mut writer = Writer {
            outgoing: FramedWrite::new(w, PostgresCodec, handle.clone()),
            receiver,
            pending_request: None,
            shared: shared.clone(),
        };

        monoio::spawn(async move {
            let writer_fut = writer.run();
            let shutdown_internal_fut = shutdown_internal_rx;
            let shutdown_external_fut = shutdown_external_rx;

            monoio::select! {
                biased;
                _ = writer_fut => {
                    trace!("shutdown: clean");
                },
                _ = shutdown_internal_fut => {
                    trace!("shutdown: internal");
                    canceller.cancel();
                },
                _ = shutdown_external_fut => {
                    trace!("shutdown: external");
                    canceller.cancel();
                },
            }

            let _ = writer.shutdown().await;
        });

        let conn = Self {
            pending_responses,
            shared,
            parameters,
            incoming: FramedRead::new(r, PostgresCodec, handle.clone()),
            shutdown: Some(shutdown_internal_tx),
        };
        (shutdown_external_tx, conn)
    }
}

impl<S> RawConnection<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent,
{
    fn shutdown(&mut self) -> Result<(), Error> {
        self.shutdown
            .take()
            .unwrap()
            .send(())
            .map_err(|_| Error::closed())?;
        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.shared.borrow().state == State::Closing
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    async fn response(&mut self) -> Option<Result<BackendMessage, Error>> {
        trace!("response: polling...");
        if let Some(message) = self.pending_responses.pop_front() {
            trace!("retrying pending response");
            return Some(Ok(message));
        }

        trace!("response: waiting on tcp...");
        self.incoming.next().await.map(|r| r.map_err(Error::io))
    }

    async fn read(&mut self) -> Result<Option<AsyncMessage>, Error> {
        trace!("poll_read: init");

        if self.shared.borrow().state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.response().await {
                Some(message) => message,
                None => return Err(Error::closed()),
            };

            trace!("read: new message");

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

            let mut shared = self.shared.borrow_mut();
            let response = match shared.responses.pop_front() {
                Some(response) => response,
                None => match messages.next().map_err(Error::parse)? {
                    Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                    _ => return Err(Error::unexpected_message()),
                },
            };

            // drop `shared` to avoid holding the reference across `await` boundaries
            drop(shared);

            trace!("request complete: {}", request_complete);
            match response.sender.send(messages).await {
                Ok(()) => {
                    if !request_complete {
                        self.shared.borrow_mut().responses.push_front(response);
                    }
                }
                Err(_) => {
                    // we need to keep paging through the rest of the messages even if the receiver's hung up
                    if !request_complete {
                        self.shared.borrow_mut().responses.push_front(response);
                    }
                }
            }
        }
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    ///
    /// Return values of `None` or `Some(Err(_))` are "terminal"; callers should not invoke this method again after
    /// receiving one of those values.
    #[inline]
    pub async fn next_message(&mut self) -> Option<Result<AsyncMessage, Error>> {
        match self.read().await {
            Ok(Some(message)) => Some(Ok(message)),
            Ok(None) => match self.shutdown() {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            },
            Err(err) => Some(Err(err)),
        }
    }
}

impl<S: CancelableAsyncReadRent + CancelableAsyncWriteRent + Splitable> Stream
    for RawConnection<S>
{
    type Item = Result<AsyncMessage, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        trace!("connection: idle");
        match self.next_message().await? {
            Ok(AsyncMessage::Notice(notice)) => {
                info!("{}: {}", notice.severity(), notice.message());
                Some(Ok(AsyncMessage::Notice(notice)))
            }
            Ok(message) => Some(Ok(message)),
            Err(err) => Some(Err(err)),
        }
    }
}
