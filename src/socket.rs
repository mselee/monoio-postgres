use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio::net::TcpStream;
#[cfg(unix)]
use monoio::net::UnixStream;
use monoio::BufResult;

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// The standard stream type used by the crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[derive(Debug)]
pub struct Socket(Inner);

impl Socket {
    pub(crate) fn new_tcp(stream: TcpStream) -> Socket {
        Socket(Inner::Tcp(stream))
    }

    #[cfg(unix)]
    pub(crate) fn new_unix(stream: UnixStream) -> Socket {
        Socket(Inner::Unix(stream))
    }
}

impl AsyncReadRent for Socket {
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        match &mut self.0 {
            Inner::Tcp(s) => s.read(buf).await,
            #[cfg(unix)]
            Inner::Unix(s) => s.read(buf).await,
        }
    }

    async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        match &mut self.0 {
            Inner::Tcp(s) => s.readv(buf).await,
            #[cfg(unix)]
            Inner::Unix(s) => s.readv(buf).await,
        }
    }
}

impl AsyncWriteRent for Socket {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        match &mut self.0 {
            Inner::Tcp(s) => s.write(buf).await,
            #[cfg(unix)]
            Inner::Unix(s) => s.write(buf).await,
        }
    }

    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        match &mut self.0 {
            Inner::Tcp(s) => s.writev(buf_vec).await,
            #[cfg(unix)]
            Inner::Unix(s) => s.writev(buf_vec).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.0 {
            Inner::Tcp(s) => s.flush().await,
            #[cfg(unix)]
            Inner::Unix(s) => s.flush().await,
        }
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        match &mut self.0 {
            Inner::Tcp(s) => s.shutdown().await,
            #[cfg(unix)]
            Inner::Unix(s) => s.shutdown().await,
        }
    }
}
