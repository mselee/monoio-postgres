use crate::tls::{ChannelBinding, TlsStream};
use monoio::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    io::{AsyncReadRent, AsyncWriteRent},
    BufResult,
};

pub enum MaybeTlsStream<S, T> {
    Raw(S),
    Tls(T),
}

impl<S, T> AsyncReadRent for MaybeTlsStream<S, T>
where
    S: AsyncReadRent + Unpin,
    T: AsyncReadRent + Unpin,
{
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        match self {
            MaybeTlsStream::Raw(s) => s.read(buf).await,
            MaybeTlsStream::Tls(s) => s.read(buf).await,
        }
    }

    async fn readv<B: IoVecBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        match self {
            MaybeTlsStream::Raw(s) => s.readv(buf).await,
            MaybeTlsStream::Tls(s) => s.readv(buf).await,
        }
    }
}

impl<S, T> AsyncWriteRent for MaybeTlsStream<S, T>
where
    S: AsyncWriteRent + Unpin,
    T: AsyncWriteRent + Unpin,
{
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        match self {
            MaybeTlsStream::Raw(s) => s.write(buf).await,
            MaybeTlsStream::Tls(s) => s.write(buf).await,
        }
    }

    async fn writev<B: IoVecBuf>(&mut self, buf_vec: B) -> BufResult<usize, B> {
        match self {
            MaybeTlsStream::Raw(s) => s.writev(buf_vec).await,
            MaybeTlsStream::Tls(s) => s.writev(buf_vec).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.flush().await,
            MaybeTlsStream::Tls(s) => s.flush().await,
        }
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.shutdown().await,
            MaybeTlsStream::Tls(s) => s.shutdown().await,
        }
    }
}

impl<S, T> TlsStream for MaybeTlsStream<S, T>
where
    S: AsyncReadRent + AsyncWriteRent + Unpin,
    T: TlsStream + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        match self {
            MaybeTlsStream::Raw(_) => ChannelBinding::none(),
            MaybeTlsStream::Tls(s) => s.channel_binding(),
        }
    }
}
