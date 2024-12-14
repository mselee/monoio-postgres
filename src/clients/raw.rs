use local_sync::mpsc;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use monoio::io::{CancelableAsyncReadRent, CancelableAsyncWriteRent, Canceller, Splitable};

use super::{Client, InnerClient};
use crate::connections::startup::StartupStream;
use crate::{Config, Connection, Error, RawConnection};

pub struct RawClient<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent + Splitable + 'static,
{
    inner: Rc<InnerClient>,
    config: Config,
    connector: fn(&Config) -> Pin<Box<dyn Future<Output = std::io::Result<S>>>>,
    process_id: i32,
    secret_key: i32,
}

impl<S> Client for RawClient<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent + Splitable + 'static,
{
    type Transport = S;
    type Connection = RawConnection<S>;

    #[inline]
    fn inner(&self) -> &Rc<InnerClient> {
        &self.inner
    }

    #[inline]
    fn backend_pid(&self) -> i32 {
        self.process_id
    }

    #[inline]
    async fn connect(
        config: Config,
        connector: fn(&Config) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Transport>>>>,
    ) -> Result<(Self, Self::Connection), Error> {
        let canceller = Canceller::new();
        let stream = connector(&config).await.map_err(Error::io)?;
        let mut stream = StartupStream::new(stream, canceller.handle());

        let user = config
            .user
            .as_deref()
            .map_or_else(|| Cow::Owned(whoami::username()), Cow::Borrowed);

        stream.startup(&config, &user).await?;
        stream.authenticate(&config, &user).await?;
        let (process_id, secret_key, parameters) = stream.read_info().await?;
        let (sender, receiver) = mpsc::unbounded::channel();
        let client = Self {
            inner: Rc::new(InnerClient {
                sender,
                cached_typeinfo: Default::default(),
                buffer: Default::default(),
            }),
            config,
            process_id,
            secret_key,
            connector,
        };
        let connection = RawConnection::new(
            stream.inner.into_inner(),
            stream.delayed,
            parameters,
            receiver,
        );
        Ok((client, connection))
    }

    #[inline]
    async fn reconnect(&self) -> Result<(Self, Self::Connection), Error> {
        Self::connect(self.config.clone(), self.connector).await
    }
}
