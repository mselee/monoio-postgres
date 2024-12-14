//! An asynchronous, pipelined, PostgreSQL client.
//!
//! # Example
//!
//! ```no_run
//! use monoio_postgres::{NoTls, Error};
//!
//! # #[cfg(not(feature = "runtime"))] fn main() {}
//! #
//! #[monoio::main] // By default, monoio_postgres uses the monoio crate as its runtime.
//! async fn main() -> Result<(), Error> {
//!     // Connect to the database.
//!     let (client, connection) =
//!         monoio_postgres::connect("host=localhost user=postgres", NoTls).await?;
//!
//!     // The connection object performs the actual communication with the database,
//!     // so spawn it off to run on its own.
//!     monoio::spawn(async move {
//!         if let Err(e) = connection.await {
//!             eprintln!("connection error: {}", e);
//!         }
//!     });
//!
//!     // Now we can execute a simple statement that just returns its parameter.
//!     let rows = client
//!         .query("SELECT $1::TEXT", &[&"hello world"])
//!         .await?;
//!
//!     // And then check that we got back the same string we sent over.
//!     let value: &str = rows[0].get(0);
//!     assert_eq!(value, "hello world");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Behavior
//!
//! Calling a method like `Client::query` on its own does nothing. The associated request is not sent to the database
//! until the future returned by the method is first polled. Requests are executed in the order that they are first
//! polled, not in the order that their futures are created.
//!
//! # Pipelining
//!
//! The client supports *pipelined* requests. Pipelining can improve performance in use cases in which multiple,
//! independent queries need to be executed. In a traditional workflow, each query is sent to the server after the
//! previous query completes. In contrast, pipelining allows the client to send all of the queries to the server up
//! front, minimizing time spent by one side waiting for the other to finish sending data:
//!
//! ```not_rust
//!             Sequential                              Pipelined
//! | Client         | Server          |    | Client         | Server          |
//! |----------------|-----------------|    |----------------|-----------------|
//! | send query 1   |                 |    | send query 1   |                 |
//! |                | process query 1 |    | send query 2   | process query 1 |
//! | receive rows 1 |                 |    | send query 3   | process query 2 |
//! | send query 2   |                 |    | receive rows 1 | process query 3 |
//! |                | process query 2 |    | receive rows 2 |                 |
//! | receive rows 2 |                 |    | receive rows 3 |                 |
//! | send query 3   |                 |
//! |                | process query 3 |
//! | receive rows 3 |                 |
//! ```
//!
//! In both cases, the PostgreSQL server is executing the queries sequentially - pipelining just allows both sides of
//! the connection to work concurrently when possible.
//!
//! Pipelining happens automatically when futures are polled concurrently (for example, by using the futures `join`
//! combinator):
//!
//! ```rust
//! use futures_util::future;
//! use std::future::Future;
//! use monoio_postgres::{Client, Error, Statement};
//!
//! async fn pipelined_prepare(
//!     client: &Client,
//! ) -> Result<(Statement, Statement), Error>
//! {
//!     future::try_join(
//!         client.prepare("SELECT * FROM foo"),
//!         client.prepare("INSERT INTO bar (id, name) VALUES ($1, $2)")
//!     ).await
//! }
//! ```
//!
//! # Runtime
//!
//! The client works with arbitrary `AsyncRead + AsyncWrite` streams. Convenience APIs are provided to handle the
//! connection process, but these are gated by the `runtime` Cargo feature, which is enabled by default. If disabled,
//! all dependence on the monoio runtime is removed.
//!
//! # SSL/TLS support
//!
//! TLS support is implemented via external libraries. `Client::connect` and `Config::connect` take a TLS implementation
//! as an argument. The `NoTls` type in this crate can be used when TLS is not required. Otherwise, the
//! `postgres-openssl` and `postgres-native-tls` crates provide implementations backed by the `openssl` and `native-tls`
//! crates, respectively.
//!
//! # Features
//!
//! The following features can be enabled from `Cargo.toml`:
//!
//! | Feature | Description | Extra dependencies | Default |
//! | ------- | ----------- | ------------------ | ------- |
//! | `runtime` | Enable convenience API for the connection process based on the `monoio` crate. | [monoio](https://crates.io/crates/monoio) 1.0 with the features `net` and `time` | yes |
//! | `array-impls` | Enables `ToSql` and `FromSql` trait impls for arrays | - | no |
//! | `with-bit-vec-0_6` | Enable support for the `bit-vec` crate. | [bit-vec](https://crates.io/crates/bit-vec) 0.6 | no |
//! | `with-chrono-0_4` | Enable support for the `chrono` crate. | [chrono](https://crates.io/crates/chrono) 0.4 | no |
//! | `with-eui48-0_4` | Enable support for the 0.4 version of the `eui48` crate. This is deprecated and will be removed. | [eui48](https://crates.io/crates/eui48) 0.4 | no |
//! | `with-eui48-1` | Enable support for the 1.0 version of the `eui48` crate. | [eui48](https://crates.io/crates/eui48) 1.0 | no |
//! | `with-geo-types-0_6` | Enable support for the 0.6 version of the `geo-types` crate. | [geo-types](https://crates.io/crates/geo-types/0.6.0) 0.6 | no |
//! | `with-geo-types-0_7` | Enable support for the 0.7 version of the `geo-types` crate. | [geo-types](https://crates.io/crates/geo-types/0.7.0) 0.7 | no |
//! | `with-serde_json-1` | Enable support for the `serde_json` crate. | [serde_json](https://crates.io/crates/serde_json) 1.0 | no |
//! | `with-uuid-0_8` | Enable support for the `uuid` crate. | [uuid](https://crates.io/crates/uuid) 0.8 | no |
//! | `with-uuid-1` | Enable support for the `uuid` crate. | [uuid](https://crates.io/crates/uuid) 1.0 | no |
//! | `with-time-0_2` | Enable support for the 0.2 version of the `time` crate. | [time](https://crates.io/crates/time/0.2.0) 0.2 | no |
//! | `with-time-0_3` | Enable support for the 0.3 version of the `time` crate. | [time](https://crates.io/crates/time/0.3.0) 0.3 | no |
#![warn(rust_2021_compatibility, clippy::all, missing_docs)]
#![allow(async_fn_in_trait)]

// pub use crate::clients::socket::Socket;
pub use crate::clients::raw::RawClient;
pub use crate::clients::Client;
pub use crate::config::Config;
pub use crate::connections::{Connection, RawConnection};
pub use crate::entities::portal::Portal;
pub use crate::entities::row::{Row, SimpleQueryRow};
pub use crate::entities::statement::{Column, Statement};
pub use crate::entities::to_statement::ToStatement;
use crate::error::DbError;
pub use crate::error::Error;
pub use crate::ops::copy_both::CopyBothDuplex;
pub use crate::ops::copy_in::CopyInSink;
pub use crate::ops::copy_out::CopyOutStream;
pub use crate::ops::query::RowStream;
pub use crate::ops::simple_query::{SimpleColumn, SimpleQueryStream};
pub use crate::ops::transaction::Transaction;
pub use crate::ops::transaction_builder::{IsolationLevel, TransactionBuilder};

pub use crate::ops::binary_copy;

mod clients;
pub mod config;
mod connections;
mod entities;
pub mod error;
pub mod ext;
mod ops;
pub mod protocol;
pub mod replication;
pub mod types;

/// A convenience function which parses a connection string and connects to the database.
///
/// See the documentation for [`Config`] for details on the connection string format.
///
/// Requires the `runtime` Cargo feature (enabled by default).
///
/// [`Config`]: config/struct.Config.html

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

impl Notification {
    /// The process ID of the notifying backend process.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// The name of the channel that the notify has been raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The "payload" string passed from the notifying process.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
}

/// Message returned by the `SimpleQuery` stream.
#[derive(Debug)]
#[non_exhaustive]
pub enum SimpleQueryMessage {
    /// A row of data.
    Row(SimpleQueryRow),
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    CommandComplete(u64),
    /// Column values of the proceeding row values
    RowDescription(std::rc::Rc<[SimpleColumn]>),
}
