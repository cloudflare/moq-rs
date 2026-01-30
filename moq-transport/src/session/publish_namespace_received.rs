use std::ops;

use crate::coding::{ReasonPhrase, TrackNamespace};
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::{PublishNamespaceInfo, Subscriber};

#[derive(Default)]
struct PublishNamespaceReceivedState {}

/// Represents an inbound PUBLISH_NAMESPACE that was received (subscriber side).
/// When dropped, sends PUBLISH_NAMESPACE_CANCEL (if ok'd) or PUBLISH_NAMESPACE_ERROR.
pub struct PublishNamespaceReceived {
    session: Subscriber,
    state: State<PublishNamespaceReceivedState>,

    pub info: PublishNamespaceInfo,

    ok: bool,
    error: Option<ServeError>,
}

impl PublishNamespaceReceived {
    pub(super) fn new(
        session: Subscriber,
        request_id: u64,
        namespace: TrackNamespace,
    ) -> (PublishNamespaceReceived, PublishNamespaceReceivedRecv) {
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
        let recv = PublishNamespaceReceivedRecv { _state: recv };

        (send, recv)
    }

    pub fn ok(&mut self) -> Result<(), ServeError> {
        if self.ok {
            return Err(ServeError::Duplicate);
        }

        self.session.send_message(message::PublishNamespaceOk {
            id: self.info.request_id,
        });

        self.ok = true;

        Ok(())
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            self.state
                .lock()
                .modified()
                .ok_or(ServeError::Cancel)?
                .await;
        }
    }

    pub fn close(mut self, err: ServeError) -> Result<(), ServeError> {
        self.error = Some(err);
        Ok(())
    }
}

impl ops::Deref for PublishNamespaceReceived {
    type Target = PublishNamespaceInfo;

    fn deref(&self) -> &PublishNamespaceInfo {
        &self.info
    }
}

impl Drop for PublishNamespaceReceived {
    fn drop(&mut self) {
        let err = self.error.clone().unwrap_or(ServeError::Done);

        if self.ok {
            self.session.send_message(message::PublishNamespaceCancel {
                track_namespace: self.namespace.clone(),
                error_code: err.code(),
                reason_phrase: ReasonPhrase(err.to_string()),
            });
        } else {
            self.session.send_message(message::PublishNamespaceError {
                id: self.info.request_id,
                error_code: err.code(),
                reason_phrase: ReasonPhrase(err.to_string()),
            });
        }
    }
}

pub(super) struct PublishNamespaceReceivedRecv {
    _state: State<PublishNamespaceReceivedState>,
}

impl PublishNamespaceReceivedRecv {
    pub fn recv_done(self) -> Result<(), ServeError> {
        Ok(())
    }
}


