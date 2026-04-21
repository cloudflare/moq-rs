use std::ops;

use crate::coding::{ReasonPhrase, TrackNamespace};
use crate::serve::ServeError;
use crate::watch::State;
use crate::{data, message, serve};

use super::Subscriber;

#[derive(Debug, Clone)]
pub struct PublishReceivedInfo {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    pub track_alias: u64,
    /// Forward parameter from PUBLISH (0x10): true = forward immediately, false = paused
    pub forward: bool,
}

impl PublishReceivedInfo {
    pub fn new_from_publish(msg: &message::Publish) -> Self {
        // Forward parameter (0x10): default to true if not present
        // Value of 0 means paused, 1 (or non-zero) means forward
        let forward = msg
            .params
            .get_intvalue(0x10) // ParameterType::Forward
            .map(|v| v != 0)
            .unwrap_or(true);

        Self {
            id: msg.id,
            track_namespace: msg.track_namespace.clone(),
            track_name: msg.track_name.clone(),
            track_alias: msg.track_alias,
            forward,
        }
    }
}

struct PublishReceivedState {
    ok: bool,
    closed: Result<(), ServeError>,
    writer: Option<serve::TrackWriter>,
}

impl Default for PublishReceivedState {
    fn default() -> Self {
        Self {
            ok: false,
            closed: Ok(()),
            writer: None,
        }
    }
}

#[must_use = "sends PUBLISH_ERROR on drop if not accepted"]
pub struct PublishReceived {
    subscriber: Subscriber,
    pub info: PublishReceivedInfo,
    state: State<PublishReceivedState>,
    ok: bool,
}

impl PublishReceived {
    pub(super) fn new(
        subscriber: Subscriber,
        msg: &message::Publish,
    ) -> (Self, PublishReceivedRecv) {
        let info = PublishReceivedInfo::new_from_publish(msg);

        let (send, recv) = State::default().split();

        let send = Self {
            subscriber,
            info,
            state: send,
            ok: false,
        };

        let recv = PublishReceivedRecv {
            state: recv,
            writer_mode: None,
        };

        (send, recv)
    }

    pub fn accept(
        mut self,
        track: serve::TrackWriter,
        publish_msg: message::PublishOk,
    ) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.ok {
            return Err(ServeError::Duplicate);
        }
        state.closed.clone()?;

        self.subscriber.send_message(publish_msg);

        if let Some(mut state) = state.into_mut() {
            state.ok = true;
            state.writer = Some(track);
        }

        self.ok = true;

        std::mem::forget(self);

        Ok(())
    }

    pub fn reject(mut self, error_code: u64, reason: &str) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        self.subscriber.send_message(message::RequestError {
            id: self.info.id,
            error_code,
            retry_interval: 0,
            reason_phrase: ReasonPhrase(reason.to_string()),
        });

        if let Some(mut state) = state.into_mut() {
            state.closed = Err(ServeError::Closed(error_code));
        }

        std::mem::forget(self);

        Ok(())
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }
}

impl ops::Deref for PublishReceived {
    type Target = PublishReceivedInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for PublishReceived {
    fn drop(&mut self) {
        if self.ok {
            return;
        }

        let state = self.state.lock();
        let err = state
            .closed
            .as_ref()
            .err()
            .cloned()
            .unwrap_or(ServeError::NotFound);
        drop(state);

        self.subscriber.send_message(message::RequestError {
            id: self.info.id,
            error_code: err.code(),
            retry_interval: 0,
            reason_phrase: ReasonPhrase(err.to_string()),
        });
    }
}

pub(super) struct PublishReceivedRecv {
    state: State<PublishReceivedState>,
    writer_mode: Option<serve::TrackWriterMode>,
}

impl PublishReceivedRecv {
    pub fn track_alias(&self) -> Option<u64> {
        None
    }

    pub fn recv_done(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        if let Some(mut state) = state.into_mut() {
            state.closed = Err(ServeError::Done);
        }

        Ok(())
    }

    fn take_writer(&mut self) -> Result<serve::TrackWriterMode, ServeError> {
        if let Some(writer) = self.writer_mode.take() {
            return Ok(writer);
        }

        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        let writer = state.writer.take().ok_or(ServeError::Done)?;
        Ok(writer.into())
    }

    fn put_writer(&mut self, writer: serve::TrackWriterMode) {
        self.writer_mode = Some(writer);
    }

    pub fn subgroup(
        &mut self,
        header: data::SubgroupHeader,
    ) -> Result<serve::SubgroupWriter, ServeError> {
        let writer = self.take_writer()?;

        let mut subgroups = match writer {
            serve::TrackWriterMode::Track(track) => track.subgroups()?,
            serve::TrackWriterMode::Subgroups(subgroups) => subgroups,
            _ => return Err(ServeError::Mode),
        };

        let result = subgroups.create(serve::Subgroup {
            group_id: header.group_id,
            subgroup_id: header.subgroup_id.unwrap_or(0),
            priority: header.publisher_priority.unwrap_or(127),
            header_type: Some(header.header_type),
        });

        // Always put writer back, even on error, to avoid losing it
        self.put_writer(subgroups.into());

        result
    }

    pub fn datagram(&mut self, datagram: data::Datagram) -> Result<(), ServeError> {
        let writer = self.take_writer()?;

        // Determine status from datagram type or explicit status field
        let status = if datagram.datagram_type.is_end_of_group() {
            Some(crate::data::ObjectStatus::EndOfGroup)
        } else {
            datagram.status
        };

        match writer {
            serve::TrackWriterMode::Track(track) => {
                let mut datagrams = track.datagrams()?;
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    priority: datagram.publisher_priority.unwrap_or(127),
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                    status,
                })?;
                self.put_writer(serve::TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            serve::TrackWriterMode::Datagrams(mut datagrams) => {
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    priority: datagram.publisher_priority.unwrap_or(127),
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                    status,
                })?;
                self.put_writer(serve::TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            other => {
                self.put_writer(other);
                Err(ServeError::Mode)
            }
        }
    }
}
