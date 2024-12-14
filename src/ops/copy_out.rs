use bytes::Bytes;
use monoio::io::stream::Stream;
use postgres_protocol::message::backend::Message;
use tracing::debug;

use crate::{
    clients::{InnerClient, Responses},
    connections::RequestMessages,
    entities::codec::FrontendMessage,
    ext::slice_iter,
    ops::{query, simple_query},
    Error, Statement,
};

pub async fn copy_out_simple(client: &InnerClient, query: &str) -> Result<CopyOutStream, Error> {
    debug!("executing copy out query {}", query);

    let buf = simple_query::encode(client, query)?;
    let responses = start(client, buf, true).await?;
    Ok(CopyOutStream { responses })
}

pub async fn copy_out(client: &InnerClient, statement: Statement) -> Result<CopyOutStream, Error> {
    debug!("executing copy out statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    let responses = start(client, buf, false).await?;
    Ok(CopyOutStream { responses })
}

async fn start(client: &InnerClient, buf: Bytes, simple: bool) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    if !simple {
        match responses.next().await.ok_or_else(Error::closed)?? {
            Message::BindComplete => {}
            _ => return Err(Error::unexpected_message()),
        }
    }

    match responses.next().await.ok_or_else(Error::closed)?? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

/// A stream of `COPY ... TO STDOUT` query data.
pub struct CopyOutStream {
    responses: Responses,
}

impl Stream for CopyOutStream {
    type Item = Result<Bytes, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.responses.next().await? {
            Ok(Message::CopyData(body)) => Some(Ok(body.into_bytes())),
            Ok(Message::CopyDone) => None,
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
