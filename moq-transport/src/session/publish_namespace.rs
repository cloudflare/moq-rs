// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{collections::VecDeque, ops};

use crate::coding::TrackNamespace;
use crate::watch::State;
use crate::{
    message,
    serve::{ServeError, TracksReader},
};
use futures::{stream::FuturesUnordered, StreamExt};

use super::{Publisher, SessionError, Subscribed, TrackStatusRequested, CANCELLED_STREAM_CODE};

/// Information about an outbound PUBLISH_NAMESPACE request.
#[derive(Debug, Clone)]
pub struct PublishNamespaceInfo {
    pub request_id: u64,
    pub namespace: TrackNamespace,
}

struct PublishNamespaceState {
    subscribers: VecDeque<Subscribed>,
    track_statuses_requested: VecDeque<TrackStatusRequested>,
    ok: bool,
    closed: Result<(), ServeError>,
}

impl Default for PublishNamespaceState {
    fn default() -> Self {
        Self {
            subscribers: Default::default(),
            track_statuses_requested: Default::default(),
            ok: false,
            closed: Ok(()),
        }
    }
}

impl Drop for PublishNamespaceState {
    fn drop(&mut self) {
        for subscriber in self.subscribers.drain(..) {
            subscriber
                .close(ServeError::not_found_ctx(
                    "publish_namespace dropped before subscription handled",
                ))
                .ok();
        }
    }
}

/// Represents an outbound PUBLISH_NAMESPACE sent by a publisher.
///
/// Dropping the handle cancels the request stream unless the peer already closed it.
#[must_use = "cancels PUBLISH_NAMESPACE on drop"]
pub struct PublishNamespace {
    state: State<PublishNamespaceState>,
    cancel: Option<tokio::sync::oneshot::Sender<u32>>,

    pub info: PublishNamespaceInfo,
}

impl PublishNamespace {
    /// Create a PublishNamespace without sending on the control stream.
    /// The caller sends via a bidi request stream (draft-18).
    pub(super) fn new(
        request_id: u64,
        namespace: TrackNamespace,
    ) -> (
        PublishNamespace,
        PublishNamespaceRecv,
        tokio::sync::oneshot::Receiver<u32>,
    ) {
        let info = PublishNamespaceInfo {
            request_id,
            namespace: namespace.clone(),
        };
        Self::from_parts(info, request_id)
    }

    /// Return the wire message to send on the request stream.
    pub(super) fn wire_message(&self) -> message::PublishNamespace {
        message::PublishNamespace {
            id: self.info.request_id,
            track_namespace: self.info.namespace.clone(),
            params: Default::default(),
        }
    }

    fn from_parts(
        info: PublishNamespaceInfo,
        request_id: u64,
    ) -> (
        PublishNamespace,
        PublishNamespaceRecv,
        tokio::sync::oneshot::Receiver<u32>,
    ) {
        let (send, recv) = State::default().split();
        let (cancel, recv_cancel) = tokio::sync::oneshot::channel();

        let send = Self {
            info,
            state: send,
            cancel: Some(cancel),
        };
        let recv = PublishNamespaceRecv {
            state: recv,
            request_id,
        };

        (send, recv, recv_cancel)
    }

    /// Serve subscriptions and track status requests until withdrawal or an error.
    ///
    /// The tracks must belong to this request's namespace. This does not wait
    /// for acceptance first; callers that need that ordering can await `ok()`.
    /// `Session::run` must be driven concurrently.
    pub async fn serve(self, tracks: TracksReader) -> Result<(), SessionError> {
        if tracks.namespace != self.info.namespace {
            return Err(ServeError::Internal(
                "PUBLISH_NAMESPACE tracks have a different namespace".to_string(),
            )
            .into());
        }
        let mut subscribe_tasks = FuturesUnordered::new();
        let mut status_tasks = FuturesUnordered::new();
        let mut subscribe_done = false;
        let mut status_done = false;

        loop {
            tokio::select! {
                res = self.subscribed(), if !subscribe_done => {
                    match res? {
                        Some(subscribed) => {
                            let tracks = tracks.clone();
                            subscribe_tasks.push(async move {
                                let info = subscribed.info.clone();
                                if let Err(err) = Publisher::serve_subscribe(subscribed, tracks).await {
                                    tracing::warn!(
                                        subscribe_info = ?info,
                                        error = %err,
                                        "failed serving subscribe"
                                    );
                                }
                            });
                        }
                        None => subscribe_done = true,
                    }
                },
                res = self.track_status_requested(), if !status_done => {
                    match res? {
                        Some(status) => {
                            let tracks = tracks.clone();
                            status_tasks.push(async move {
                                let request_msg = status.request_msg.clone();
                                if let Err(err) = Publisher::serve_track_status(status, tracks).await {
                                    tracing::warn!(
                                        request = ?request_msg,
                                        error = %err,
                                        "failed serving track status request"
                                    );
                                }
                            });
                        }
                        None => status_done = true,
                    }
                },
                Some(res) = subscribe_tasks.next() => res,
                Some(res) = status_tasks.next() => res,
                else => return Ok(()),
            }
        }
    }

    /// Wait until the namespace publish is closed (error or peer disconnect).
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

    /// Wait until a subscriber arrives for this namespace.
    pub async fn subscribed(&self) -> Result<Option<Subscribed>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if !state.subscribers.is_empty() {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.subscribers.pop_front()));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(None),
                }
            }
            .await;
        }
    }

    /// Wait until a TRACK_STATUS request arrives for this namespace.
    pub async fn track_status_requested(&self) -> Result<Option<TrackStatusRequested>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if !state.track_statuses_requested.is_empty() {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.track_statuses_requested.pop_front()));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(None),
                }
            }
            .await;
        }
    }

    /// Wait until the peer has sent REQUEST_OK for this namespace.
    /// Returns an error on rejection or closure before acceptance.
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
                    None => return Err(ServeError::Cancel),
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

        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(CANCELLED_STREAM_CODE);
        }
    }
}

impl ops::Deref for PublishNamespace {
    type Target = PublishNamespaceInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Peer-facing handle for tracking a PUBLISH_NAMESPACE request.
pub(super) struct PublishNamespaceRecv {
    state: State<PublishNamespaceState>,
    /// Request ID of the outbound PUBLISH_NAMESPACE.
    // Namespace lookup alone is insufficient: both request_id and namespace
    // are needed, so Publisher holds a second index by request_id.
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

    pub fn recv_error(&mut self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }

    pub fn recv_subscribe(&mut self, subscriber: Subscribed) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        state.subscribers.push_back(subscriber);

        Ok(())
    }

    pub fn recv_track_status_requested(
        &mut self,
        track_status_requested: TrackStatusRequested,
    ) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        state
            .track_statuses_requested
            .push_back(track_status_requested);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::coding::TrackNamespace;

    use super::PublishNamespace;

    #[tokio::test]
    async fn ok_fails_if_response_state_ends_without_acceptance() {
        let (publish, recv, _cancel) =
            PublishNamespace::new(6, TrackNamespace::from_utf8_path("test"));
        drop(recv);
        assert!(matches!(
            publish.ok().await,
            Err(crate::serve::ServeError::Cancel)
        ));
    }

    #[tokio::test]
    async fn drop_cancels_request_stream() {
        let (publish, _recv, cancel) =
            PublishNamespace::new(6, TrackNamespace::from_utf8_path("test"));

        drop(publish);

        assert_eq!(cancel.await.unwrap(), super::CANCELLED_STREAM_CODE);
    }
}
