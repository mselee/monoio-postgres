use crate::query::RowStream;
use crate::types::{BorrowToSql, ToSql, Type};
use crate::{Client, Error, Row, SimpleQueryMessage, Statement, ToStatement, Transaction};

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like [`Client::execute`].
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement;

    /// Like [`Client::execute_raw`].
    async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator;

    /// Like [`Client::query`].
    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like [`Client::query_one`].
    async fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync;

    /// Like [`Client::query_opt`].
    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like [`Client::query_raw`].
    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator;

    /// Like [`Client::query_typed`]
    async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql), Type)],
    ) -> Result<Vec<Row>, Error>;

    /// Like [`Client::query_typed_raw`]
    async fn query_typed_raw<P, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>;

    /// Like [`Client::prepare`].
    async fn prepare(&self, query: &str) -> Result<Statement, Error>;

    /// Like [`Client::prepare_typed`].
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error>;

    /// Like [`Client::transaction`].
    async fn transaction(&mut self) -> Result<Transaction<'_>, Error>;

    /// Like [`Client::batch_execute`].
    async fn batch_execute(&self, query: &str) -> Result<(), Error>;

    /// Like [`Client::simple_query`].
    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error>;

    /// Returns a reference to the underlying [`Client`].
    fn client(&self) -> &Client;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute(query, params).await
    }

    async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params).await
    }

    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query(query, params).await
    }

    async fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_one(statement, params).await
    }

    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_opt(statement, params).await
    }

    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params).await
    }

    async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params).await
    }

    async fn query_typed_raw<P, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        self.query_typed_raw(statement, params).await
    }

    async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query).await
    }

    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types).await
    }

    async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction().await
    }

    async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.batch_execute(query).await
    }

    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query).await
    }

    fn client(&self) -> &Client {
        self
    }
}

impl private::Sealed for Transaction<'_> {}

#[allow(clippy::needless_lifetimes)]
impl GenericClient for Transaction<'_> {
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute(query, params).await
    }

    async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params).await
    }

    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query(query, params).await
    }

    async fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_one(statement, params).await
    }

    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_opt(statement, params).await
    }

    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params).await
    }

    async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params).await
    }

    async fn query_typed_raw<P, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        self.query_typed_raw(statement, params).await
    }

    async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query).await
    }

    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types).await
    }

    #[allow(clippy::needless_lifetimes)]
    async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        self.transaction().await
    }

    async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.batch_execute(query).await
    }

    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query).await
    }

    fn client(&self) -> &Client {
        self.client()
    }
}
