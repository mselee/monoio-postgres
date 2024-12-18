pub(crate) mod raw;

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use crate::connections::{Request, RequestMessages};
use crate::ext::{slice_iter, TryStreamExt};
use crate::ops::copy_both::{self, CopyBothReceiver};
use crate::ops::{copy_in, copy_out, prepare, query, simple_query};
use crate::protocol::message::backend::Message;
use bytes::{Buf, BytesMut};
use fallible_iterator::FallibleIterator;
use local_sync::mpsc;
use monoio::io::stream::Stream;

use crate::entities::codec::{BackendMessages, FrontendMessage};
use crate::types::{BorrowToSql, Oid, ToSql, Type};
use crate::{
    Config, CopyBothDuplex, CopyInSink, CopyOutStream, Error, Row, RowStream, SimpleQueryMessage,
    SimpleQueryStream, Statement, ToStatement, Transaction, TransactionBuilder,
};

pub struct Responses {
    receiver: mpsc::bounded::Rx<BackendMessages>,
    cur: BackendMessages,
}

pub struct CopyBothHandles {
    pub(crate) stream_receiver: mpsc::bounded::Rx<Result<Message, Error>>,
    pub(crate) sink_sender: mpsc::bounded::Tx<FrontendMessage>,
}

impl Responses {
    async fn anext(&mut self) -> Result<Message, Error> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(message) => return Ok(message),
                None => {}
            }

            match self.receiver.recv().await {
                Some(messages) => self.cur = messages,
                None => return Err(Error::closed()),
            }
        }
    }
}

impl Stream for Responses {
    type Item = Result<Message, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.anext().await {
            Err(err) if err.is_closed() => None,
            msg => Some(msg),
        }
    }
}

/// A cache of type info and prepared statements for fetching type info
/// (corresponding to the queries in the [prepare](prepare) module).
#[derive(Default)]
struct CachedTypeInfo {
    /// A statement for basic information for a type from its
    /// OID. Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_QUERY) (or its
    /// fallback).
    typeinfo: Option<Statement>,
    /// A statement for getting information for a composite type from its OID.
    /// Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_COMPOSITE_QUERY).
    typeinfo_composite: Option<Statement>,
    /// A statement for getting information for a composite type from its OID.
    /// Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_COMPOSITE_QUERY) (or
    /// its fallback).
    typeinfo_enum: Option<Statement>,

    /// Cache of types already looked up.
    types: HashMap<Oid, Type>,
}

pub struct InnerClient {
    sender: mpsc::unbounded::Tx<Request>,
    cached_typeinfo: RefCell<CachedTypeInfo>,

    /// A buffer to use when writing out postgres commands.
    buffer: RefCell<BytesMut>,
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::bounded::channel(1);
        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
    }

    pub fn start_copy_both(&self) -> Result<CopyBothHandles, Error> {
        let (sender, receiver) = mpsc::bounded::channel(16);
        let (stream_sender, stream_receiver) = mpsc::bounded::channel(16);
        let (sink_sender, sink_receiver) = mpsc::bounded::channel(16);

        let responses = Responses {
            receiver,
            cur: BackendMessages::empty(),
        };
        let messages = RequestMessages::CopyBoth(CopyBothReceiver::new(
            responses,
            sink_receiver,
            stream_sender,
        ));

        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(CopyBothHandles {
            stream_receiver,
            sink_sender,
        })
    }

    pub fn typeinfo(&self) -> Option<Statement> {
        self.cached_typeinfo.borrow().typeinfo.clone()
    }

    pub fn set_typeinfo(&self, statement: &Statement) {
        self.cached_typeinfo.borrow_mut().typeinfo = Some(statement.clone());
    }

    pub fn typeinfo_composite(&self) -> Option<Statement> {
        self.cached_typeinfo.borrow().typeinfo_composite.clone()
    }

    pub fn set_typeinfo_composite(&self, statement: &Statement) {
        self.cached_typeinfo.borrow_mut().typeinfo_composite = Some(statement.clone());
    }

    pub fn typeinfo_enum(&self) -> Option<Statement> {
        self.cached_typeinfo.borrow().typeinfo_enum.clone()
    }

    pub fn set_typeinfo_enum(&self, statement: &Statement) {
        self.cached_typeinfo.borrow_mut().typeinfo_enum = Some(statement.clone());
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.cached_typeinfo.borrow().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: &Type) {
        self.cached_typeinfo
            .borrow_mut()
            .types
            .insert(oid, type_.clone());
    }

    pub fn clear_type_cache(&self) {
        self.cached_typeinfo.borrow_mut().types.clear();
    }

    /// Call the given function with a buffer to be used when writing out
    /// postgres commands.
    pub fn with_buf<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let mut buffer = self.buffer.borrow_mut();
        let r = f(&mut buffer);
        buffer.clear();
        r
    }
}

pub trait Client: Sized {
    type Transport;
    type Connection;

    fn inner(&self) -> &Rc<InnerClient>;

    async fn connect(
        config: Config,
        connector: fn(&Config) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Transport>>>>,
    ) -> Result<Self, Error>;

    async fn fork(&self) -> Result<Self, Error>;

    async fn disconnect(&mut self);

    fn kill(&mut self) -> Result<(), Error>;

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[]).await
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        prepare::prepare(&self.inner(), query, parameter_types).await
    }

    /// Executes a statement, returning a vector of the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    async fn query<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_raw(statement, slice_iter(params))
            .await?
            .try_collect()
            .await
    }

    /// Executes a statement which returns a single row, returning it.
    ///
    /// Returns an error if the query does not return exactly one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    async fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_opt(statement, params)
            .await
            .and_then(|res| res.ok_or_else(Error::row_count))
    }

    /// Executes a statements which returns zero or one rows, returning it.
    ///
    /// Returns an error if the query returns more than one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let mut stream = self.query_raw(statement, slice_iter(params)).await?;
        let mut first = None;

        // Originally this was two calls to `try_next().await?`,
        // once for the first element, and second to error if more than one.
        //
        // However, this new form with only one .await in a loop generates
        // slightly smaller codegen/stack usage for the resulting future.
        while let Some(row) = stream.try_next().await? {
            if first.is_some() {
                return Err(Error::row_count());
            }

            first = Some(row);
        }

        Ok(first)
    }

    /// The maximally flexible version of [`query`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// [`query`]: #method.query
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn async_main(client: &monoio_postgres::Client) -> Result<(), monoio_postgres::Error> {
    /// use futures_util::{pin_mut, TryStreamExt};
    ///
    /// let params: Vec<String> = vec![
    ///     "first param".into(),
    ///     "second param".into(),
    /// ];
    /// let mut it = client.query_raw(
    ///     "SELECT foo FROM bar WHERE biz = $1 AND baz = $2",
    ///     params,
    /// ).await?;
    ///
    /// pin_mut!(it);
    /// while let Some(row) = it.try_next().await? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self).await?;
        query::query(&self.inner(), statement, params).await
    }

    /// Like `query`, but requires the types of query parameters to be explicitly specified.
    ///
    /// Compared to `query`, this method allows performing queries without three round trips (for
    /// prepare, execute, and close) by requiring the caller to specify parameter values along with
    /// their Postgres type. Thus, this is suitable in environments where prepared statements aren't
    /// supported (such as Cloudflare Workers with Hyperdrive).
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the
    /// parameter of the list provided, 1-indexed.
    async fn query_typed(
        &self,
        query: &str,
        params: &[(&(dyn ToSql), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed_raw(query, params.iter().map(|(v, t)| (*v, t.clone())))
            .await?
            .try_collect()
            .await
    }

    /// The maximally flexible version of [`query_typed`].
    ///
    /// Compared to `query`, this method allows performing queries without three round trips (for
    /// prepare, execute, and close) by requiring the caller to specify parameter values along with
    /// their Postgres type. Thus, this is suitable in environments where prepared statements aren't
    /// supported (such as Cloudflare Workers with Hyperdrive).
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the
    /// parameter of the list provided, 1-indexed.
    ///
    /// [`query_typed`]: #method.query_typed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn async_main(client: &monoio_postgres::Client) -> Result<(), monoio_postgres::Error> {
    /// use futures_util::{pin_mut, TryStreamExt};
    /// use monoio_postgres::types::Type;
    ///
    /// let params: Vec<(String, Type)> = vec![
    ///     ("first param".into(), Type::TEXT),
    ///     ("second param".into(), Type::TEXT),
    /// ];
    /// let mut it = client.query_typed_raw(
    ///     "SELECT foo FROM bar WHERE biz = $1 AND baz = $2",
    ///     params,
    /// ).await?;
    ///
    /// pin_mut!(it);
    /// while let Some(row) = it.try_next().await? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn query_typed_raw<P, I>(&self, query: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        query::query_typed(&self.inner(), query, params).await
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute_raw(statement, slice_iter(params)).await
    }

    /// The maximally flexible version of [`execute`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// [`execute`]: #method.execute
    async fn execute_raw<T, P, I>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self).await?;
        query::execute(self.inner(), statement, params).await
    }

    /// Executes a `COPY FROM STDIN` statement, returning a sink used to write the copy data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any. The copy *must*
    /// be explicitly completed via the `Sink::close` or `finish` methods. If it is not, the copy will be aborted.
    async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, Error>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static + Send,
    {
        let statement = statement.__convert().into_statement(self).await?;
        copy_in::copy_in(self.inner(), statement).await
    }

    /// Executes a `COPY FROM STDIN` query, returning a sink used to write the copy data.
    async fn copy_in_simple<U>(&self, query: &str) -> Result<CopyInSink<U>, Error>
    where
        U: Buf + 'static + Send,
    {
        copy_in::copy_in_simple(self.inner(), query).await
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any.
    async fn copy_out<T>(&self, statement: &T) -> Result<CopyOutStream, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = statement.__convert().into_statement(self).await?;
        copy_out::copy_out(self.inner(), statement).await
    }

    /// Executes a `COPY TO STDOUT` query, returning a stream of the resulting data.
    async fn copy_out_simple(&self, query: &str) -> Result<CopyOutStream, Error> {
        copy_out::copy_out_simple(self.inner(), query).await
    }

    /// Executes a CopyBoth query, returning a combined Stream+Sink type to read and write copy
    /// data.
    async fn copy_both_simple<T>(&self, query: &str) -> Result<CopyBothDuplex<T>, Error>
    where
        T: Buf + 'static + Send,
    {
        copy_both::copy_both_simple(self.inner(), query).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a list of the
    /// rows, this method returns a list of an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_raw(query).await?.try_collect().await
    }

    async fn simple_query_raw(&self, query: &str) -> Result<SimpleQueryStream, Error> {
        simple_query::simple_query(self.inner(), query).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        simple_query::batch_execute(self.inner(), query).await
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    async fn transaction(&mut self) -> Result<Transaction<'_, Self>, Error> {
        self.build_transaction().start().await
    }

    /// Returns a builder for a transaction with custom settings.
    ///
    /// Unlike the `transaction` method, the builder can be used to control the transaction's isolation level and other
    /// attributes.
    fn build_transaction(&mut self) -> TransactionBuilder<'_, Self> {
        TransactionBuilder::new(self)
    }

    /// Returns the server's process ID for the connection.
    fn backend_pid(&self) -> i32;

    /// Clears the client's type information cache.
    ///
    /// When user-defined types are used in a query, the client loads their definitions from the database and caches
    /// them for the lifetime of the client. If those definitions are changed in the database, this method can be used
    /// to flush the local cache and allow the new, updated definitions to be loaded.
    fn clear_type_cache(&self) {
        self.inner().clear_type_cache();
    }

    /// Determines if the connection to the server has already closed.
    ///
    /// In that case, all future queries will fail.
    fn is_closed(&self) -> bool {
        self.inner().sender.is_closed()
    }
}
