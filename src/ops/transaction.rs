use crate::connections::RequestMessages;
use crate::entities::codec::FrontendMessage;
use crate::ext::{slice_iter, TryStreamExt};
use crate::types::{BorrowToSql, ToSql, Type};
use crate::{
    Client, CopyInSink, CopyOutStream, Error, Portal, Row, RowStream, SimpleQueryMessage,
    Statement, ToStatement,
};
use bytes::Buf;
use postgres_protocol::message::frontend;

use super::{bind, query};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a, C: Client> {
    client: &'a mut C,
    savepoint: Option<Savepoint>,
    done: bool,
}

/// A representation of a PostgreSQL database savepoint.
struct Savepoint {
    name: String,
    depth: u32,
}

impl<'a, C: Client> Drop for Transaction<'a, C> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("ROLLBACK TO {}", sp.name)
        } else {
            "ROLLBACK".to_string()
        };
        let buf = self.client.inner().with_buf(|buf| {
            frontend::query(&query, buf).unwrap();
            buf.split().freeze()
        });
        let _ = self
            .client
            .inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(buf)));
    }
}

impl<'a, C: Client> Transaction<'a, C> {
    pub(crate) fn new(client: &'a mut C) -> Self {
        Transaction {
            client,
            savepoint: None,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("RELEASE {}", sp.name)
        } else {
            "COMMIT".to_string()
        };
        self.client.batch_execute(&query).await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("ROLLBACK TO {}", sp.name)
        } else {
            "ROLLBACK".to_string()
        };
        self.client.batch_execute(&query).await
    }

    /// Like `Client::prepare`.
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query).await
    }

    /// Like `Client::prepare_typed`.
    pub async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.client.prepare_typed(query, parameter_types).await
    }

    /// Like `Client::query`.
    pub async fn query<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query(statement, params).await
    }

    /// Like `Client::query_one`.
    pub async fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query_one(statement, params).await
    }

    /// Like `Client::query_opt`.
    pub async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query_opt(statement, params).await
    }

    /// Like `Client::query_raw`.
    pub async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.query_raw(statement, params).await
    }

    /// Like `Client::query_typed`.
    pub async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.client.query_typed(statement, params).await
    }

    /// Like `Client::query_typed_raw`.
    pub async fn query_typed_raw<P, I>(&self, query: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        self.client.query_typed_raw(query, params).await
    }

    /// Like `Client::execute`.
    pub async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.execute(statement, params).await
    }

    /// Like `Client::execute_iter`.
    pub async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.execute_raw(statement, params).await
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created, and can only be used on the
    /// connection that created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub async fn bind<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.bind_raw(statement, slice_iter(params)).await
    }

    /// A maximally flexible version of [`bind`].
    ///
    /// [`bind`]: #method.bind
    pub async fn bind_raw<P, T, I>(&self, statement: &T, params: I) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self.client).await?;
        bind::bind(self.client.inner(), statement, params).await
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all rows will be returned.
    pub async fn query_portal(&self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.query_portal_raw(portal, max_rows)
            .await?
            .try_collect()
            .await
    }

    /// The maximally flexible version of [`query_portal`].
    ///
    /// [`query_portal`]: #method.query_portal
    pub async fn query_portal_raw(
        &self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<RowStream, Error> {
        query::query_portal(self.client.inner(), portal, max_rows).await
    }

    /// Like `Client::copy_in`.
    pub async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, Error>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static + Send,
    {
        self.client.copy_in(statement).await
    }

    /// Like `Client::copy_out`.
    pub async fn copy_out<T>(&self, statement: &T) -> Result<CopyOutStream, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.copy_out(statement).await
    }

    /// Like `Client::simple_query`.
    pub async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.client.simple_query(query).await
    }

    /// Like `Client::batch_execute`.
    pub async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query).await
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint.
    pub async fn transaction(&mut self) -> Result<Transaction<'_, C>, Error> {
        self._savepoint(None).await
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint with the specified name.
    pub async fn savepoint<I>(&mut self, name: I) -> Result<Transaction<'_, C>, Error>
    where
        I: Into<String>,
    {
        self._savepoint(Some(name.into())).await
    }

    async fn _savepoint(&mut self, name: Option<String>) -> Result<Transaction<'_, C>, Error> {
        let depth = self.savepoint.as_ref().map_or(0, |sp| sp.depth) + 1;
        let name = name.unwrap_or_else(|| format!("sp_{}", depth));
        let query = format!("SAVEPOINT {}", name);
        self.batch_execute(&query).await?;

        Ok(Transaction {
            client: self.client,
            savepoint: Some(Savepoint { name, depth }),
            done: false,
        })
    }

    /// Returns a reference to the underlying `Client`.
    pub fn client(&self) -> &C {
        self.client
    }
}
