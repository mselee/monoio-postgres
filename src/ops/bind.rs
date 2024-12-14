use monoio::io::stream::Stream;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_types::BorrowToSql;
use std::cell::Cell;
use std::rc::Rc;

use crate::clients::InnerClient;
use crate::connections::RequestMessages;
use crate::entities::codec::FrontendMessage;
use crate::{Error, Portal, Statement};

use super::query;

thread_local! {
    static NEXT_ID: Cell<usize> = Cell::new(0);
}

pub async fn bind<P, I>(
    client: &Rc<InnerClient>,
    statement: Statement,
    params: I,
) -> Result<Portal, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let next_id = NEXT_ID.get() + 1;
    NEXT_ID.set(next_id);
    let name = format!("s{}", next_id);
    let buf = client.with_buf(|buf| {
        query::encode_bind(&statement, params, &name, buf)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    if let Some(Ok(Message::BindComplete)) = responses.next().await {
        Ok(Portal::new(client, name, statement))
    } else {
        Err(Error::unexpected_message())
    }
}
