//! TLS support.

use monoio::io::{AsyncReadRent, AsyncWriteRent};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) mod private {
    pub struct ForcePrivateApi;
}

/// Channel binding information returned from a TLS handshake.
pub struct ChannelBinding {
    pub(crate) tls_server_end_point: Option<Vec<u8>>,
}

impl ChannelBinding {
    /// Creates a `ChannelBinding` containing no information.
    pub fn none() -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: None,
        }
    }

    /// Creates a `ChannelBinding` containing `tls-server-end-point` channel binding information.
    pub fn tls_server_end_point(tls_server_end_point: Vec<u8>) -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: Some(tls_server_end_point),
        }
    }
}

/// A constructor of `TlsConnect`ors.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[cfg(feature = "runtime")]
pub trait MakeTlsConnect<S> {
    /// The stream type created by the `TlsConnect` implementation.
    type Stream: TlsStream + Unpin;
    /// The `TlsConnect` implementation created by this type.
    type TlsConnect: TlsConnect<S, Stream = Self::Stream>;
    /// The error type returned by the `TlsConnect` implementation.
    type Error: Into<Box<dyn Error + Sync + Send>>;

    /// Creates a new `TlsConnect`or.
    ///
    /// The domain name is provided for certificate verification and SNI.
    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error>;
}

/// An asynchronous function wrapping a stream in a TLS session.
pub trait TlsConnect<S> {
    /// The stream returned by the future.
    type Stream: TlsStream + Unpin;
    /// The error returned by the future.
    type Error: Into<Box<dyn Error + Sync + Send>>;
    /// The future returned by the connector.
    type Future: Future<Output = Result<Self::Stream, Self::Error>>;

    /// Returns a future performing a TLS handshake over the stream.
    fn connect(self, stream: S) -> Self::Future;

    #[doc(hidden)]
    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        true
    }
}

/// A TLS-wrapped connection to a PostgreSQL database.
pub trait TlsStream: AsyncReadRent + AsyncWriteRent {
    /// Returns channel binding information for the session.
    fn channel_binding(&self) -> ChannelBinding;
}

/// A `MakeTlsConnect` and `TlsConnect` implementation which simply returns an error.
///
/// This can be used when `sslmode` is `none` or `prefer`.
#[derive(Debug, Copy, Clone)]
pub struct NoTls;

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for NoTls {
    type Stream = NoTlsStream;
    type TlsConnect = NoTls;
    type Error = NoTlsError;

    fn make_tls_connect(&mut self, _: &str) -> Result<NoTls, NoTlsError> {
        Ok(NoTls)
    }
}

impl<S> TlsConnect<S> for NoTls {
    type Stream = NoTlsStream;
    type Error = NoTlsError;
    type Future = NoTlsFuture;

    fn connect(self, _: S) -> NoTlsFuture {
        NoTlsFuture(())
    }

    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        false
    }
}

/// The future returned by `NoTls`.
pub struct NoTlsFuture(());

impl Future for NoTlsFuture {
    type Output = Result<NoTlsStream, NoTlsError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(NoTlsError(())))
    }
}

/// The TLS "stream" type produced by the `NoTls` connector.
///
/// Since `NoTls` doesn't support TLS, this type is uninhabited.
pub enum NoTlsStream {}

impl AsyncReadRent for NoTlsStream {
    fn read<T: monoio::buf::IoBufMut>(
        &mut self,
        buf: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        async { (Ok(0), buf) }
    }

    fn readv<T: monoio::buf::IoVecBufMut>(
        &mut self,
        buf: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        async { (Ok(0), buf) }
    }
}

impl AsyncWriteRent for NoTlsStream {
    fn write<T: monoio::buf::IoBuf>(
        &mut self,
        buf: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        async { (Ok(0), buf) }
    }

    fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        async { (Ok(0), buf_vec) }
    }

    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        async { Ok(()) }
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        async { Ok(()) }
    }
}

impl TlsStream for NoTlsStream {
    fn channel_binding(&self) -> ChannelBinding {
        match *self {}
    }
}

/// The error returned by `NoTls`.
#[derive(Debug)]
pub struct NoTlsError(());

impl fmt::Display for NoTlsError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("no TLS implementation configured")
    }
}

impl Error for NoTlsError {}
