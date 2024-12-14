//! Utilities for working with the PostgreSQL replication copy both format.

use self::protocol::{LogicalReplicationMessage, ReplicationMessage};
use super::{CopyBothDuplex, Error};
use bytes::{BufMut, Bytes, BytesMut};
use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use postgres_types::PgLsn;

pub mod protocol;

const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
const HOT_STANDBY_FEEDBACK_TAG: u8 = b'h';

/// A type which deserializes the postgres replication protocol. This type can be used with
/// both physical and logical replication to get access to the byte content of each replication
/// message.
///
/// The replication *must* be explicitly completed via the `finish` method.
pub struct ReplicationStream {
    stream: CopyBothDuplex<Bytes>,
}

impl ReplicationStream {
    /// Creates a new ReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self { stream }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        buf.put_u8(STANDBY_STATUS_UPDATE_TAG);
        buf.put_u64(write_lsn.into());
        buf.put_u64(flush_lsn.into());
        buf.put_u64(apply_lsn.into());
        buf.put_i64(ts);
        buf.put_u8(reply);

        self.stream.send(buf.freeze()).await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        &mut self,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        buf.put_u8(HOT_STANDBY_FEEDBACK_TAG);
        buf.put_i64(timestamp);
        buf.put_u32(global_xmin);
        buf.put_u32(global_xmin_epoch);
        buf.put_u32(catalog_xmin);
        buf.put_u32(catalog_xmin_epoch);

        self.stream.send(buf.freeze()).await
    }
}

impl Stream for ReplicationStream {
    type Item = Result<ReplicationMessage<Bytes>, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.stream.next().await? {
            Ok(buf) => Some(ReplicationMessage::parse(&buf).map_err(Error::parse)),
            Err(err) => Some(Err(err)),
        }
    }
}

/// A type which deserializes the postgres logical replication protocol. This type gives access
/// to a high level representation of the changes in transaction commit order.
///
/// The replication *must* be explicitly completed via the `finish` method.
pub struct LogicalReplicationStream {
    stream: ReplicationStream,
}

impl LogicalReplicationStream {
    /// Creates a new LogicalReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self {
            stream: ReplicationStream::new(stream),
        }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        self.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, reply)
            .await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        &mut self,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), Error> {
        self.stream
            .hot_standby_feedback(
                timestamp,
                global_xmin,
                global_xmin_epoch,
                catalog_xmin,
                catalog_xmin_epoch,
            )
            .await
    }
}

impl Stream for LogicalReplicationStream {
    type Item = Result<ReplicationMessage<LogicalReplicationMessage>, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.stream.next().await? {
            Ok(ReplicationMessage::XLogData(body)) => {
                let body = body
                    .map_data(|buf| LogicalReplicationMessage::parse(&buf))
                    .map_err(Error::parse);
                match body {
                    Ok(body) => Some(Ok(ReplicationMessage::XLogData(body))),
                    Err(err) => Some(Err(err)),
                }
            }
            Ok(ReplicationMessage::PrimaryKeepAlive(body)) => {
                Some(Ok(ReplicationMessage::PrimaryKeepAlive(body)))
            }
            Err(err) => Some(Err(err)),
        }
    }
}
