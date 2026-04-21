use std::{fmt, sync::Arc};

use tokio::sync::broadcast;

use super::{ServeError, Track};

const DATAGRAM_CHANNEL_SIZE: usize = 1024;

pub struct Datagrams {
    pub track: Arc<Track>,
}

impl Datagrams {
    pub fn produce(self) -> (DatagramsWriter, DatagramsReader) {
        let (tx, rx) = broadcast::channel(DATAGRAM_CHANNEL_SIZE);

        let writer = DatagramsWriter::new(tx, self.track.clone());
        let reader = DatagramsReader::new(rx, self.track);

        (writer, reader)
    }
}

pub struct DatagramsWriter {
    tx: broadcast::Sender<Datagram>,
    pub track: Arc<Track>,
}

impl DatagramsWriter {
    fn new(tx: broadcast::Sender<Datagram>, track: Arc<Track>) -> Self {
        Self { tx, track }
    }

    pub fn write(&mut self, datagram: Datagram) -> Result<(), ServeError> {
        // Ignore send errors (no receivers) - datagrams are fire-and-forget
        let _ = self.tx.send(datagram);
        Ok(())
    }

    pub fn close(self, _err: ServeError) -> Result<(), ServeError> {
        // Channel closes when tx is dropped
        Ok(())
    }
}

pub struct DatagramsReader {
    rx: broadcast::Receiver<Datagram>,
    pub track: Arc<Track>,
    latest: Option<(u64, u64)>,
}

impl Clone for DatagramsReader {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.resubscribe(),
            track: self.track.clone(),
            latest: self.latest,
        }
    }
}

impl DatagramsReader {
    fn new(rx: broadcast::Receiver<Datagram>, track: Arc<Track>) -> Self {
        Self {
            rx,
            track,
            latest: None,
        }
    }

    pub async fn read(&mut self) -> Result<Option<Datagram>, ServeError> {
        loop {
            match self.rx.recv().await {
                Ok(datagram) => {
                    self.latest = Some((datagram.group_id, datagram.object_id));
                    return Ok(Some(datagram));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("[DATAGRAMS] reader lagged by {} datagrams", n);
                    // Continue reading - we'll get the next available datagram
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Ok(None); // Channel closed
                }
            }
        }
    }

    pub fn latest(&self) -> Option<(u64, u64)> {
        self.latest
    }

    pub fn is_closed(&self) -> bool {
        // Broadcast receiver doesn't have a direct is_closed check.
        // We return false (not closed) since we can't reliably detect sender drop
        // without actually trying to receive. The read() method will return None
        // when the channel is truly closed.
        false
    }
}

/// Static information about the datagram.
#[derive(Clone)]
pub struct Datagram {
    pub group_id: u64,
    pub object_id: u64,
    pub priority: u8,
    pub payload: bytes::Bytes,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,

    // Object status (e.g., EndOfGroup)
    pub status: Option<crate::data::ObjectStatus>,
}

impl fmt::Debug for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Datagram")
            .field("object_id", &self.object_id)
            .field("group_id", &self.group_id)
            .field("priority", &self.priority)
            .field("payload", &self.payload.len())
            .field("extension_headers", &self.extension_headers)
            .field("status", &self.status)
            .finish()
    }
}
