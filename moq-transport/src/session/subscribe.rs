use std::ops;

use crate::{
    coding::{KeyValuePairs, TrackNamespace},
    data, message,
    serve::{self, ServeError, TrackWriter, TrackWriterMode},
};

use crate::watch::State;

use super::Subscriber;

// TODO rename to SubscriptionInfo when used for Publishes as well?
#[derive(Debug, Clone)]
pub struct SubscribeInfo {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    /// Optional parameters
    pub params: KeyValuePairs,

    // Set to true if this is a track_status request only
    pub track_status: bool,
}

impl SubscribeInfo {
    pub fn new_from_subscribe(msg: &message::Subscribe) -> Self {
        Self {
            id: msg.id,
            track_namespace: msg.track_namespace.clone(),
            track_name: msg.track_name.clone(),
            params: msg.params.clone(),
            track_status: false,
        }
    }
}

struct SubscribeState {
    ok: bool,
    track_alias: Option<u64>,
    closed: Result<(), ServeError>,
}

impl Default for SubscribeState {
    fn default() -> Self {
        Self {
            ok: Default::default(),
            track_alias: None,
            closed: Ok(()),
        }
    }
}

// Held by the application
#[must_use = "unsubscribe on drop"]
pub struct Subscribe {
    state: State<SubscribeState>,
    subscriber: Subscriber,

    pub info: SubscribeInfo,
}

impl Subscribe {
    pub(super) fn new(
        mut subscriber: Subscriber,
        request_id: u64,
        track: TrackWriter,
    ) -> (Subscribe, SubscribeRecv) {
        let subscribe_message = message::Subscribe {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            params: Default::default(),
        };
        let info = SubscribeInfo::new_from_subscribe(&subscribe_message);

        subscriber.send_message(subscribe_message);

        let (send, recv) = State::default().split();

        let send = Subscribe {
            state: send,
            subscriber,
            info,
        };

        let recv = SubscribeRecv {
            state: recv,
            writer: Some(track.into()),
        };

        (send, recv)
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

impl Drop for Subscribe {
    fn drop(&mut self) {
        self.subscriber
            .send_message(message::Unsubscribe { id: self.info.id });
    }
}

impl ops::Deref for Subscribe {
    type Target = SubscribeInfo;

    fn deref(&self) -> &SubscribeInfo {
        &self.info
    }
}

pub(super) struct SubscribeRecv {
    state: State<SubscribeState>,
    writer: Option<TrackWriterMode>,
}

impl SubscribeRecv {
    pub fn ok(&mut self, alias: u64) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.ok {
            return Err(ServeError::Duplicate);
        }

        if let Some(mut state) = state.into_mut() {
            state.ok = true;
            state.track_alias = Some(alias);
        }

        Ok(())
    }

    pub fn track_alias(&self) -> Option<u64> {
        let state = self.state.lock();
        state.track_alias
    }

    pub fn error(mut self, err: ServeError) -> Result<(), ServeError> {
        if let Some(writer) = self.writer.take() {
            writer.close(err.clone())?;
        }

        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }

    pub fn subgroup(
        &mut self,
        header: data::SubgroupHeader,
    ) -> Result<serve::SubgroupWriter, ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        let mut subgroups = match writer {
            // TODO SLG - understand why both of these are needed, clock demo won't run if I comment out TrackWriteMode::Track
            TrackWriterMode::Track(track) => track.subgroups()?,
            TrackWriterMode::Subgroups(subgroups) => subgroups,
            _ => return Err(ServeError::Mode),
        };

        let writer = subgroups.create(serve::Subgroup {
            group_id: header.group_id,
            // When subgroup_id is not present in the header type, it implicitly means subgroup 0
            subgroup_id: header.subgroup_id.unwrap_or(0),
            // When priority is not present (NoPriority header types), default to 0
            priority: header.publisher_priority.unwrap_or(0),
        })?;

        self.writer = Some(subgroups.into());

        Ok(writer)
    }

    pub fn datagram(&mut self, datagram: data::Datagram) -> Result<(), ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        match writer {
            TrackWriterMode::Track(track) => {
                // convert Track -> Datagrams writer, write, then put Datagrams back
                let mut datagrams = track.datagrams()?;
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    // When priority is not present (NoPriority datagram types), default to 0
                    priority: datagram.publisher_priority.unwrap_or(0),
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                })?;
                self.writer = Some(TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            TrackWriterMode::Datagrams(mut datagrams) => {
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    // When priority is not present (NoPriority datagram types), default to 0
                    priority: datagram.publisher_priority.unwrap_or(0),
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                })?;
                self.writer = Some(TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            other => {
                // preserve whatever unexpected mode was present, then report error
                self.writer = Some(other);
                Err(ServeError::Mode)
            }
        }
    }
}
