use std::collections::{HashMap, VecDeque};

use crate::config;
use crate::protocol::authentication;
use crate::protocol::authentication::sasl::{self, ScramSha256};
use crate::protocol::message::{
    backend::{AuthenticationSaslBody, Message},
    frontend,
};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use monoio::io::CancelHandle;
use monoio::io::{
    sink::{Sink, SinkExt},
    stream::Stream,
    CancelableAsyncReadRent, CancelableAsyncWriteRent,
};
use monoio_codec::Framed;

use crate::{
    config::ReplicationMode,
    entities::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec},
    ext::TryStreamExt,
    Config, Error,
};

pub struct StartupStream<S> {
    pub(crate) inner: Framed<S, PostgresCodec>,
    buf: BackendMessages,
    pub(crate) delayed: VecDeque<BackendMessage>,
}

impl<S> Sink<FrontendMessage> for StartupStream<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent,
{
    type Error = std::io::Error;

    #[inline]
    async fn send(&mut self, item: FrontendMessage) -> Result<(), Self::Error> {
        self.inner.send(item).await
    }

    #[inline]
    async fn flush(&mut self) -> Result<(), Self::Error> {
        Sink::flush(&mut self.inner).await
    }

    #[inline]
    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

impl<S> Stream for StartupStream<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent,
{
    type Item = std::io::Result<Message>;

    async fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Some(Ok(message)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }

            match self.inner.next().await {
                Some(Ok(BackendMessage::Normal { messages, .. })) => self.buf = messages,
                Some(Ok(BackendMessage::Async(message))) => return Some(Ok(message)),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

impl<S> StartupStream<S>
where
    S: CancelableAsyncReadRent + CancelableAsyncWriteRent,
{
    pub(crate) fn new(stream: S, cancel_handle: CancelHandle) -> Self {
        Self {
            inner: Framed::new(stream, PostgresCodec, cancel_handle),
            buf: BackendMessages::empty(),
            delayed: VecDeque::new(),
        }
    }

    pub(crate) async fn startup(&mut self, config: &Config, user: &str) -> Result<(), Error> {
        let mut params = vec![("client_encoding", "UTF8")];
        params.push(("user", user));
        if let Some(dbname) = &config.dbname {
            params.push(("database", &**dbname));
        }
        if let Some(options) = &config.options {
            params.push(("options", &**options));
        }
        if let Some(application_name) = &config.application_name {
            params.push(("application_name", &**application_name));
        }
        if let Some(replication_mode) = &config.replication_mode {
            match replication_mode {
                ReplicationMode::Physical => params.push(("replication", "true")),
                ReplicationMode::Logical => params.push(("replication", "database")),
            }
        }

        let mut buf = BytesMut::new();
        frontend::startup_message(params, &mut buf).map_err(Error::encode)?;

        self.send_and_flush(FrontendMessage::Raw(buf.freeze()))
            .await
            .map_err(Error::io)
    }

    pub(crate) async fn authenticate(&mut self, config: &Config, user: &str) -> Result<(), Error> {
        match self.try_next().await.map_err(Error::io)? {
            Some(Message::AuthenticationOk) => {
                can_skip_channel_binding(config)?;
                return Ok(());
            }
            Some(Message::AuthenticationCleartextPassword) => {
                can_skip_channel_binding(config)?;

                let pass = config
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::config("password missing".into()))?;

                self.authenticate_password(pass).await?;
            }
            Some(Message::AuthenticationMd5Password(body)) => {
                can_skip_channel_binding(config)?;

                let pass = config
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::config("password missing".into()))?;

                let output = authentication::md5_hash(user.as_bytes(), pass, body.salt());
                self.authenticate_password(output.as_bytes()).await?;
            }
            Some(Message::AuthenticationSasl(body)) => {
                self.authenticate_sasl(body, config).await?;
            }
            Some(Message::AuthenticationKerberosV5)
            | Some(Message::AuthenticationScmCredential)
            | Some(Message::AuthenticationGss)
            | Some(Message::AuthenticationSspi) => {
                return Err(Error::authentication(
                    "unsupported authentication method".into(),
                ))
            }
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }

        match self.try_next().await.map_err(Error::io)? {
            Some(Message::AuthenticationOk) => Ok(()),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    async fn authenticate_password(&mut self, password: &[u8]) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        frontend::password_message(password, &mut buf).map_err(Error::encode)?;

        self.inner
            .send(FrontendMessage::Raw(buf.freeze()))
            .await
            .map_err(Error::io)
    }

    async fn authenticate_sasl(
        &mut self,
        body: AuthenticationSaslBody,
        config: &Config,
    ) -> Result<(), Error> {
        let password = config
            .password
            .as_ref()
            .ok_or_else(|| Error::config("password missing".into()))?;

        let mut has_scram = false;
        let mut has_scram_plus = false;
        let mut mechanisms = body.mechanisms();
        while let Some(mechanism) = mechanisms.next().map_err(Error::parse)? {
            match mechanism {
                sasl::SCRAM_SHA_256 => has_scram = true,
                sasl::SCRAM_SHA_256_PLUS => has_scram_plus = true,
                _ => {}
            }
        }

        let channel_binding = None;
        // let channel_binding = self
        //     .inner
        //     .get_ref()
        //     .channel_binding()
        //     .tls_server_end_point
        //     .filter(|_| config.channel_binding != config::ChannelBinding::Disable)
        //     .map(sasl::ChannelBinding::tls_server_end_point);

        let (channel_binding, mechanism) = if has_scram_plus {
            match channel_binding {
                Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
                None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
            }
        } else if has_scram {
            match channel_binding {
                Some(_) => (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
                None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
            }
        } else {
            return Err(Error::authentication("unsupported SASL mechanism".into()));
        };

        if mechanism != sasl::SCRAM_SHA_256_PLUS {
            can_skip_channel_binding(config)?;
        }

        let mut scram = ScramSha256::new(password, channel_binding);

        let mut buf = BytesMut::new();
        frontend::sasl_initial_response(mechanism, scram.message(), &mut buf)
            .map_err(Error::encode)?;
        self.send_and_flush(FrontendMessage::Raw(buf.freeze()))
            .await
            .map_err(Error::io)?;

        let body = match self.try_next().await.map_err(Error::io)? {
            Some(Message::AuthenticationSaslContinue(body)) => body,
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        scram
            .update(body.data())
            .map_err(|e| Error::authentication(e.into()))?;

        let mut buf = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut buf).map_err(Error::encode)?;
        self.send_and_flush(FrontendMessage::Raw(buf.freeze()))
            .await
            .map_err(Error::io)?;

        let body = match self.try_next().await.map_err(Error::io)? {
            Some(Message::AuthenticationSaslFinal(body)) => body,
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        scram
            .finish(body.data())
            .map_err(|e| Error::authentication(e.into()))?;

        Ok(())
    }

    pub(crate) async fn read_info(&mut self) -> Result<(i32, i32, HashMap<String, String>), Error> {
        let mut process_id = 0;
        let mut secret_key = 0;
        let mut parameters = HashMap::new();

        loop {
            match self.try_next().await.map_err(Error::io)? {
                Some(Message::BackendKeyData(body)) => {
                    process_id = body.process_id();
                    secret_key = body.secret_key();
                }
                Some(Message::ParameterStatus(body)) => {
                    parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                }
                Some(msg @ Message::NoticeResponse(_)) => {
                    self.delayed.push_back(BackendMessage::Async(msg))
                }
                Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key, parameters)),
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(_) => return Err(Error::unexpected_message()),
                None => return Err(Error::closed()),
            }
        }
    }
}

fn can_skip_channel_binding(config: &Config) -> Result<(), Error> {
    match config.channel_binding {
        config::ChannelBinding::Disable | config::ChannelBinding::Prefer => Ok(()),
        config::ChannelBinding::Require => Err(Error::authentication(
            "server did not use channel binding".into(),
        )),
    }
}
