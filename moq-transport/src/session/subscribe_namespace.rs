use std::ops;

use crate::coding::TrackNamespace;
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::Subscriber;

#[derive(Debug, Clone)]
pub struct SubscribeNsInfo {
    pub request_id: u64,
    pub namespace_prefix: TrackNamespace,
}

struct SubscribeNsState {
    ok: bool,
    closed: Result<(), ServeError>,
}

impl Default for SubscribeNsState {
    fn default() -> Self {
        Self {
            ok: false,
            closed: Ok(()),
        }
    }
}

/// Represents an outbound SUBSCRIBE_NAMESPACE request (subscriber side).
/// When dropped, sends UNSUBSCRIBE_NAMESPACE to the peer.
#[must_use = "sends UNSUBSCRIBE_NAMESPACE on drop"]
pub struct SubscribeNs {
    subscriber: Subscriber,
    state: State<SubscribeNsState>,

    pub info: SubscribeNsInfo,
}

impl SubscribeNs {
    pub(super) fn new(
        mut subscriber: Subscriber,
        request_id: u64,
        namespace_prefix: TrackNamespace,
    ) -> (SubscribeNs, SubscribeNsRecv) {
        let info = SubscribeNsInfo {
            request_id,
            namespace_prefix: namespace_prefix.clone(),
        };

        subscriber.send_message(message::SubscribeNamespace {
            id: request_id,
            track_namespace_prefix: namespace_prefix,
            forward: 1,
            params: Default::default(),
        });

        let (send, recv) = State::default().split();

        let send = Self {
            subscriber,
            info,
            state: send,
        };
        let recv = SubscribeNsRecv { state: recv };

        (send, recv)
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    pub async fn ok(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                if state.ok {
                    return Ok(());
                }
                state.closed.clone()?;

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }
}

impl Drop for SubscribeNs {
    fn drop(&mut self) {
        // In draft-16, SUBSCRIBE_NAMESPACE uses its own bidirectional stream.
        // Closing the stream implicitly unsubscribes.
    }
}

impl ops::Deref for SubscribeNs {
    type Target = SubscribeNsInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

pub(super) struct SubscribeNsRecv {
    state: State<SubscribeNsState>,
}

impl SubscribeNsRecv {
    pub fn recv_ok(&mut self) -> Result<(), ServeError> {
        if let Some(mut state) = self.state.lock_mut() {
            if state.ok {
                return Err(ServeError::Duplicate);
            }

            state.ok = true;
        }

        Ok(())
    }

    pub fn recv_error(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }
}
