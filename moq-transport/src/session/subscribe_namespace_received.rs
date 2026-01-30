use std::ops;

use crate::coding::{ReasonPhrase, TrackNamespace};
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::Publisher;

#[derive(Debug, Clone)]
pub struct SubscribeNamespaceReceivedInfo {
    pub request_id: u64,
    pub namespace_prefix: TrackNamespace,
}

struct SubscribeNamespaceReceivedState {
    closed: Result<(), ServeError>,
}

impl Default for SubscribeNamespaceReceivedState {
    fn default() -> Self {
        Self { closed: Ok(()) }
    }
}

#[must_use = "sends SUBSCRIBE_NAMESPACE_ERROR on drop if not accepted"]
pub struct SubscribeNamespaceReceived {
    publisher: Publisher,
    state: State<SubscribeNamespaceReceivedState>,
    pub info: SubscribeNamespaceReceivedInfo,
    ok: bool,
}

impl SubscribeNamespaceReceived {
    pub(super) fn new(
        publisher: Publisher,
        request_id: u64,
        namespace_prefix: TrackNamespace,
    ) -> (Self, SubscribeNamespaceReceivedRecv) {
        let info = SubscribeNamespaceReceivedInfo {
            request_id,
            namespace_prefix: namespace_prefix.clone(),
        };

        let (send, recv) = State::default().split();

        let send = Self {
            publisher,
            info,
            state: send,
            ok: false,
        };

        let recv = SubscribeNamespaceReceivedRecv {
            state: recv,
            namespace_prefix,
        };

        (send, recv)
    }

    pub fn ok(&mut self) -> Result<(), ServeError> {
        if self.ok {
            return Err(ServeError::Duplicate);
        }

        self.publisher.send_message(message::SubscribeNamespaceOk {
            id: self.info.request_id,
        });

        self.ok = true;

        Ok(())
    }

    pub fn reject(mut self, error_code: u64, reason: &str) -> Result<(), ServeError> {
        self.publisher
            .send_message(message::SubscribeNamespaceError {
                id: self.info.request_id,
                error_code,
                reason_phrase: ReasonPhrase(reason.to_string()),
            });

        self.ok = true;

        Ok(())
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
}

impl ops::Deref for SubscribeNamespaceReceived {
    type Target = SubscribeNamespaceReceivedInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for SubscribeNamespaceReceived {
    fn drop(&mut self) {
        if self.ok {
            return;
        }

        self.publisher
            .send_message(message::SubscribeNamespaceError {
                id: self.info.request_id,
                error_code: ServeError::NotFound.code(),
                reason_phrase: ReasonPhrase("SUBSCRIBE_NAMESPACE not handled".to_string()),
            });
    }
}

pub(super) struct SubscribeNamespaceReceivedRecv {
    state: State<SubscribeNamespaceReceivedState>,
    namespace_prefix: TrackNamespace,
}

impl SubscribeNamespaceReceivedRecv {
    pub fn namespace_prefix(&self) -> &TrackNamespace {
        &self.namespace_prefix
    }

    pub fn recv_unsubscribe(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        if let Some(mut state) = state.into_mut() {
            state.closed = Err(ServeError::Cancel);
        }

        Ok(())
    }
}
