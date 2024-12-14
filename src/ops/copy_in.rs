use bytes::{Buf, BufMut, Bytes, BytesMut};
use local_sync::mpsc;
use monoio::io::{sink::Sink, stream::Stream};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::marker::PhantomData;
use tracing::debug;

use crate::clients::{InnerClient, Responses};
use crate::connections::RequestMessages;
use crate::entities::codec::FrontendMessage;
use crate::ext::slice_iter;
use crate::ops::{query, simple_query};
use crate::{Error, Statement};

use super::query::extract_row_affected;

enum CopyInMessage {
    Message(FrontendMessage),
    Done,
}

pub struct CopyInReceiver {
    receiver: mpsc::bounded::Rx<CopyInMessage>,
    done: bool,
}

impl CopyInReceiver {
    fn new(receiver: mpsc::bounded::Rx<CopyInMessage>) -> CopyInReceiver {
        CopyInReceiver {
            receiver,
            done: false,
        }
    }

    pub(crate) fn hint(&self) -> usize {
        self.receiver.hint()
    }
}

impl Stream for CopyInReceiver {
    type Item = FrontendMessage;

    async fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.receiver.recv().await {
            Some(CopyInMessage::Message(message)) => Some(message),
            Some(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Some(FrontendMessage::Raw(buf.freeze()))
            }
            None => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Some(FrontendMessage::Raw(buf.freeze()))
            }
        }
    }
}

enum SinkState {
    Active,
    Closing,
    Reading,
}

/// A sink for `COPY ... FROM STDIN` query data.
///
/// The copy *must* be explicitly completed via the `Sink::close` or `finish` methods. If it is
/// not, the copy will be aborted.
pub struct CopyInSink<T> {
    sender: mpsc::bounded::Tx<CopyInMessage>,
    responses: Responses,
    buf: BytesMut,
    state: SinkState,
    _p: PhantomData<T>,
}

impl<T> CopyInSink<T>
where
    T: Buf + 'static + Send,
{
    /// Completes the copy, returning the number of rows inserted.
    ///
    /// The `Sink::close` method is equivalent to `finish`, except that it does not return the
    /// number of rows.
    pub async fn finish(&mut self) -> Result<u64, Error> {
        loop {
            match self.state {
                SinkState::Active => {
                    self.flush().await?;
                    self.sender
                        .send(CopyInMessage::Done)
                        .await
                        .map_err(|_| Error::closed())?;
                    self.state = SinkState::Closing;
                }
                SinkState::Closing => {
                    self.sender.close();
                    self.state = SinkState::Reading;
                }
                SinkState::Reading => {
                    match self.responses.next().await.ok_or_else(Error::closed)?? {
                        Message::CommandComplete(body) => {
                            let rows = extract_row_affected(&body)?;
                            return Ok(rows);
                        }
                        _ => return Err(Error::unexpected_message()),
                    }
                }
            }
        }
    }
}

impl<T> Sink<T> for CopyInSink<T>
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
        self.sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .await
            .map_err(|_| Error::closed())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        if !self.buf.is_empty() {
            let data: Box<dyn Buf + Send> = Box::new(self.buf.split().freeze());
            let data = CopyData::new(data).map_err(Error::encode)?;
            self.sender
                .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
                .await
                .map_err(|_| Error::closed())?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.finish().await?;
        Ok(())
    }
}

async fn start<T>(client: &InnerClient, buf: Bytes, simple: bool) -> Result<CopyInSink<T>, Error>
where
    T: Buf + 'static + Send,
{
    let (sender, receiver) = mpsc::bounded::channel(1);
    let receiver = CopyInReceiver::new(receiver);
    let mut responses = client.send(RequestMessages::CopyIn(receiver))?;

    sender
        .send(CopyInMessage::Message(FrontendMessage::Raw(buf)))
        .await
        .map_err(|_| Error::closed())?;

    if !simple {
        match responses.next().await.ok_or_else(Error::closed)?? {
            Message::BindComplete => {}
            _ => return Err(Error::unexpected_message()),
        }
    }

    match responses.next().await.ok_or_else(Error::closed)?? {
        Message::CopyInResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(CopyInSink {
        sender,
        responses,
        buf: BytesMut::new(),
        state: SinkState::Active,
        _p: PhantomData,
    })
}

pub async fn copy_in<T>(client: &InnerClient, statement: Statement) -> Result<CopyInSink<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy in statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    start(client, buf, false).await
}

pub async fn copy_in_simple<T>(client: &InnerClient, query: &str) -> Result<CopyInSink<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy in query {}", query);

    let buf = simple_query::encode(client, query)?;
    start(client, buf, true).await
}
