use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::query::extract_row_affected;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use log::debug;
use monoio::io::stream::Stream;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::Arc;

/// Information about a column of a single query row.
#[derive(Debug)]
pub struct SimpleColumn {
    name: String,
}

impl SimpleColumn {
    pub(crate) fn new(name: String) -> SimpleColumn {
        SimpleColumn { name }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub async fn simple_query(client: &InnerClient, query: &str) -> Result<SimpleQueryStream, Error> {
    debug!("executing simple query: {}", query);

    let buf = encode(client, query)?;
    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(SimpleQueryStream {
        responses,
        columns: None,
    })
}

pub async fn batch_execute(client: &InnerClient, query: &str) -> Result<(), Error> {
    debug!("executing statement batch: {}", query);

    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await.ok_or_else(Error::closed)?? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

pub(crate) fn encode(client: &InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}

/// A stream of simple query results.
pub struct SimpleQueryStream {
    responses: Responses,
    columns: Option<Arc<[SimpleColumn]>>,
}

impl Stream for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.responses.next().await? {
            Ok(Message::CommandComplete(body)) => match extract_row_affected(&body) {
                Ok(rows) => Some(Ok(SimpleQueryMessage::CommandComplete(rows))),
                Err(err) => Some(Err(err)),
            },
            Ok(Message::EmptyQueryResponse) => Some(Ok(SimpleQueryMessage::CommandComplete(0))),
            Ok(Message::RowDescription(body)) => {
                let columns: Arc<[SimpleColumn]> = match body
                    .fields()
                    .map(|f| Ok(SimpleColumn::new(f.name().to_string())))
                    .collect::<Vec<_>>()
                    .map_err(Error::parse)
                {
                    Ok(x) => x.into(),
                    Err(err) => return Some(Err(err)),
                };

                self.columns = Some(columns.clone());
                Some(Ok(SimpleQueryMessage::RowDescription(columns)))
            }
            Ok(Message::DataRow(body)) => {
                if let Some(columns) = &self.columns {
                    match SimpleQueryRow::new(columns.clone(), body) {
                        Ok(row) => Some(Ok(SimpleQueryMessage::Row(row))),
                        Err(err) => Some(Err(err)),
                    }
                } else {
                    Some(Err(Error::unexpected_message()))
                }
            }
            Ok(Message::ReadyForQuery(_)) => None,
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
