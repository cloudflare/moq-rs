use std::ops;

use crate::coding::TrackNamespace;
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::Publisher;

#[derive(Debug, Clone)]
pub struct PublishNamespaceInfo {
    pub request_id: u64,
    pub namespace: TrackNamespace,
}

/// Internal state for PublishNamespace.
///
/// PublishNamespace is a namespace registry that advertises to subscribers
/// that a publisher has tracks available in a namespace. It does NOT route
/// subscriptions - that happens via PUBLISH/SUBSCRIBE messages directly.
struct PublishNamespaceState {
    ok: bool,
    closed: Result<(), ServeError>,
}

impl Default for PublishNamespaceState {
    fn default() -> Self {
        Self {
            ok: false,
            closed: Ok(()),
        }
    }
}

/// Represents an outbound PUBLISH_NAMESPACE request (publisher side).
/// When dropped, sends PUBLISH_NAMESPACE_DONE to the peer.
#[must_use = "sends PUBLISH_NAMESPACE_DONE on drop"]
pub struct PublishNamespace {
    publisher: Publisher,
    state: State<PublishNamespaceState>,

    pub info: PublishNamespaceInfo,
}

impl PublishNamespace {
    pub(super) fn new(
        mut publisher: Publisher,
        request_id: u64,
        namespace: TrackNamespace,
    ) -> (PublishNamespace, PublishNamespaceRecv) {
        let info = PublishNamespaceInfo {
            request_id,
            namespace: namespace.clone(),
        };

        publisher.send_message(message::PublishNamespace {
            id: request_id,
            track_namespace: namespace.clone(),
            params: Default::default(),
        });

        let (send, recv) = State::default().split();

        let send = Self {
            publisher,
            info,
            state: send,
        };
        let recv = PublishNamespaceRecv {
            state: recv,
            request_id,
        };

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

impl Drop for PublishNamespace {
    fn drop(&mut self) {
        if self.state.lock().closed.is_err() {
            return;
        }

        self.publisher.send_message(message::PublishNamespaceDone {
            track_namespace: self.namespace.clone(),
        });
    }
}

impl ops::Deref for PublishNamespace {
    type Target = PublishNamespaceInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

pub(super) struct PublishNamespaceRecv {
    state: State<PublishNamespaceState>,
    pub request_id: u64,
}

impl PublishNamespaceRecv {
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
