// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::ops;

use crate::coding::{ReasonPhrase, TrackNamespace};
use crate::message::RequestErrorCode;
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::{PublishNamespaceInfo, Subscriber};

// There is currently no subscriber feedback beyond an OK or error, so the
// shared state carries no data.
#[derive(Default)]
struct PublishedNamespaceState {}

/// Represents an inbound PUBLISH_NAMESPACE received by a subscriber.
///
/// On drop, sends REQUEST_OK if accepted, or REQUEST_ERROR if rejected.
pub struct PublishedNamespace {
    session: Subscriber,
    state: State<PublishedNamespaceState>,

    pub info: PublishNamespaceInfo,

    ok: bool,
    error: Option<ServeError>,
}

impl PublishedNamespace {
    pub(super) fn new(
        session: Subscriber,
        request_id: u64,
        namespace: TrackNamespace,
    ) -> (PublishedNamespace, PublishedNamespaceRecv) {
        let info = PublishNamespaceInfo {
            request_id,
            namespace,
        };

        let (send, recv) = State::default().split();
        let send = Self {
            session,
            info,
            ok: false,
            error: None,
            state: send,
        };
        let recv = PublishedNamespaceRecv {
            _state: recv,
            request_id,
        };

        (send, recv)
    }

    /// Accept the PUBLISH_NAMESPACE by sending REQUEST_OK (draft-16 §9.7).
    pub fn ok(&mut self) -> Result<(), ServeError> {
        if self.ok {
            return Err(ServeError::Duplicate);
        }

        // Draft-16 §6.2: acceptance is signalled with REQUEST_OK, not the
        // legacy PUBLISH_NAMESPACE_OK.
        self.session.send_message(message::RequestOk {
            id: self.info.request_id,
            params: Default::default(),
        });

        self.ok = true;

        Ok(())
    }

    /// Wait until the peer closes the namespace publish (PUBLISH_NAMESPACE_DONE).
    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            self.state
                .lock()
                .modified()
                .ok_or(ServeError::Cancel)?
                .await;
        }
    }

    /// Reject the PUBLISH_NAMESPACE; the error is sent on drop.
    pub fn close(mut self, err: ServeError) -> Result<(), ServeError> {
        self.error = Some(err);
        Ok(())
    }
}

impl ops::Deref for PublishedNamespace {
    type Target = PublishNamespaceInfo;

    fn deref(&self) -> &PublishNamespaceInfo {
        &self.info
    }
}

impl Drop for PublishedNamespace {
    fn drop(&mut self) {
        let err = self.error.clone().unwrap_or(ServeError::Done);

        if self.ok {
            // Accepted: send PUBLISH_NAMESPACE_CANCEL to revoke acceptance
            // (draft-16 §9.24).  Carries Request ID, not the namespace.
            self.session.send_message(message::PublishNamespaceCancel {
                id: self.info.request_id,
                error_code: err.code(),
                reason_phrase: ReasonPhrase(err.to_string()),
            });
        } else {
            // Never accepted: send REQUEST_ERROR (draft-16 §9.8).
            self.session.send_message(message::RequestError {
                id: self.info.request_id,
                error_code: RequestErrorCode::Uninterested as u64,
                retry_interval: 0,
                reason: ReasonPhrase(err.to_string()),
            });
        }
    }
}

pub(super) struct PublishedNamespaceRecv {
    _state: State<PublishedNamespaceState>,
    /// Request ID of the corresponding PUBLISH_NAMESPACE, used for O(1) lookup
    /// when PUBLISH_NAMESPACE_DONE or PUBLISH_NAMESPACE_CANCEL arrives.
    pub request_id: u64,
}

impl PublishedNamespaceRecv {
    pub fn recv_done(self) -> Result<(), ServeError> {
        // Dropping the state signals the PublishedNamespace that the peer is done.
        Ok(())
    }
}
