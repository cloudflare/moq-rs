// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{hash_map, HashMap},
    sync::{Arc, Mutex},
};

use futures::{stream::FuturesUnordered, StreamExt};

use crate::{
    coding::TrackNamespace,
    message::{self, Message},
    mlog,
    serve::{ServeError, TracksReader},
};

use crate::watch::Queue;

use super::{
    PublishNamespace, PublishNamespaceRecv, RequestId, RequestIdAllocation, Session, SessionError,
    Subscribed, SubscribedRecv, TrackStatusRequested,
};

// TODO remove Clone.
#[derive(Clone)]
pub struct Publisher {
    webtransport: web_transport::Session,

    /// Active outbound PUBLISH_NAMESPACE requests, keyed by namespace.
    publish_namespaces: Arc<Mutex<HashMap<TrackNamespace, PublishNamespaceRecv>>>,

    /// When a Subscribe is received and we have a matching publish_namespace entry, the
    /// subscription is routed to that PublishNamespaceRecv.  Otherwise it goes here.
    subscribeds: Arc<Mutex<HashMap<u64, SubscribedRecv>>>,

    /// Subscriptions for namespaces that have no matching PUBLISH_NAMESPACE.
    unknown_subscribed: Queue<Subscribed>,

    /// TRACK_STATUS requests for namespaces that have no matching PUBLISH_NAMESPACE.
    unknown_track_status_requested: Queue<TrackStatusRequested>,

    /// Queue for outbound control messages; processed by the session run_send task.
    outgoing: Queue<Message>,

    /// Shared with Subscriber so all requests within a session use unique IDs.
    /// When we need a new Request Id for sending a request, we can get it from here.
    /// The manager is shared with the Subscriber, so the session uses unique request ids
    /// for all requests generated.  If we initiated the QUIC connection then request
    /// IDs start at 0 and increment by 2 (even numbers).  If we accepted an inbound
    /// QUIC connection then request IDs start at 1 and increment by 2 (odd numbers).
    request_id: RequestId,

    /// Optional mlog writer for logging transport events
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Publisher {
    pub(crate) fn new(
        outgoing: Queue<Message>,
        webtransport: web_transport::Session,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        request_id: RequestId,
    ) -> Self {
        Self {
            webtransport,
            publish_namespaces: Default::default(),
            subscribeds: Default::default(),
            unknown_subscribed: Default::default(),
            unknown_track_status_requested: Default::default(),
            outgoing,
            request_id,
            mlog,
        }
    }

    pub async fn accept(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::accept(session, None, transport).await?;
        Ok((session, publisher.unwrap()))
    }

    pub async fn connect(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::connect(session, None, transport).await?;
        Ok((session, publisher))
    }

    /// Send a PUBLISH_NAMESPACE for a namespace and serve tracks using the provided
    /// [serve::TracksReader].  Blocks until the namespace is unannounced or an error occurs.
    pub async fn publish_namespace(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        let publish_ns = match self
            .publish_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?
            .entry(tracks.namespace.clone())
        {
            // Duplicate PUBLISH_NAMESPACE for the same namespace is a protocol error.
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),

            hash_map::Entry::Vacant(entry) => {
                // Allocate a request ID, enforcing the peer-advertised maximum.
                let request_id = match self.request_id.allocate()? {
                    RequestIdAllocation::Allocated(id) => id,
                    blocked @ RequestIdAllocation::Blocked { .. } => {
                        if let Some(msg) = blocked.requests_blocked() {
                            let _ = self.outgoing.push(msg.into());
                        }
                        return Err(SessionError::TooManyRequests);
                    }
                };
                let (send, recv) =
                    PublishNamespace::new(self.clone(), request_id, tracks.namespace.clone());
                entry.insert(recv);
                send
            }
        };

        let mut subscribe_tasks = FuturesUnordered::new();
        let mut status_tasks = FuturesUnordered::new();
        let mut subscribe_done = false;
        let mut status_done = false;

        loop {
            tokio::select! {
                res = publish_ns.subscribed(), if !subscribe_done => {
                    match res? {
                        Some(subscribed) => {
                            let tracks = tracks.clone();
                            subscribe_tasks.push(async move {
                                let info = subscribed.info.clone();
                                if let Err(err) = Self::serve_subscribe(subscribed, tracks).await {
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
                res = publish_ns.track_status_requested(), if !status_done => {
                    match res? {
                        Some(status) => {
                            let tracks = tracks.clone();
                            status_tasks.push(async move {
                                let request_msg = status.request_msg.clone();
                                if let Err(err) = Self::serve_track_status(status, tracks).await {
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

    pub async fn serve_subscribe(
        subscribed: Subscribed,
        mut tracks: TracksReader,
    ) -> Result<(), SessionError> {
        if let Some(track) = tracks.subscribe(
            subscribed.info.track_namespace.clone(),
            &subscribed.info.track_name,
        ) {
            subscribed.serve(track).await?;
        } else {
            let namespace = subscribed.info.track_namespace.clone();
            let name = subscribed.info.track_name.clone();
            subscribed.close(ServeError::not_found_ctx(format!(
                "track '{}/{}' not found in tracks",
                namespace, name
            )))?;
        }

        Ok(())
    }

    pub async fn serve_track_status(
        track_status_request: TrackStatusRequested,
        mut tracks: TracksReader,
    ) -> Result<(), SessionError> {
        let track = tracks
            .subscribe(
                track_status_request.request_msg.track_namespace.clone(),
                &track_status_request.request_msg.track_name,
            )
            .ok_or_else(|| {
                ServeError::not_found_ctx(format!(
                    "track '{}/{}' not found for track_status",
                    track_status_request.request_msg.track_namespace,
                    track_status_request.request_msg.track_name
                ))
            })?;

        track_status_request.respond_ok(&track)?;

        Ok(())
    }

    /// Returns the next subscription that did not match any active PUBLISH_NAMESPACE.
    pub async fn subscribed(&mut self) -> Option<Subscribed> {
        self.unknown_subscribed.pop().await
    }

    /// Returns the next TRACK_STATUS request that did not match any active PUBLISH_NAMESPACE.
    pub async fn track_status_requested(&mut self) -> Option<TrackStatusRequested> {
        self.unknown_track_status_requested.pop().await
    }

    pub(crate) fn recv_message(&mut self, msg: message::Subscriber) -> Result<(), SessionError> {
        let res = match msg {
            message::Subscriber::Subscribe(msg) => self.recv_subscribe(msg),
            // REQUEST_UPDATE: draft-16 replacement for SubscribeUpdate (TODO itzmanish).
            message::Subscriber::RequestUpdate(_msg) => {
                Err(SessionError::unimplemented("REQUEST_UPDATE"))
            }
            // Draft-16: REQUEST_OK from subscriber is the acceptance of PUBLISH_NAMESPACE.
            message::Subscriber::RequestOk(msg) => self.recv_publish_namespace_ok(msg),
            // Draft-16: REQUEST_ERROR from subscriber is the rejection of PUBLISH_NAMESPACE.
            message::Subscriber::RequestError(msg) => self.recv_publish_namespace_error(msg),
            // Legacy stub retained for dispatch (pre-draft-16 peers only).
            message::Subscriber::SubscribeUpdate(msg) => self.recv_subscribe_update(msg),
            message::Subscriber::Unsubscribe(msg) => self.recv_unsubscribe(msg),
            message::Subscriber::Fetch(_msg) => Err(SessionError::unimplemented("FETCH")),
            message::Subscriber::FetchCancel(_msg) => {
                Err(SessionError::unimplemented("FETCH_CANCEL"))
            }
            message::Subscriber::TrackStatus(msg) => self.recv_track_status(msg),
            message::Subscriber::SubscribeNamespace(_msg) => {
                Err(SessionError::unimplemented("SUBSCRIBE_NAMESPACE"))
            }
            message::Subscriber::UnsubscribeNamespace(_msg) => {
                Err(SessionError::unimplemented("UNSUBSCRIBE_NAMESPACE"))
            }
            message::Subscriber::PublishNamespaceCancel(msg) => {
                self.recv_publish_namespace_cancel(msg)
            }
            // Legacy stub types — stub IDs (0x100+) are never decoded from the wire;
            // these arms exist only so the exhaustive match compiles.
            message::Subscriber::PublishNamespaceOk(_) => Ok(()),
            message::Subscriber::PublishNamespaceError(_) => Ok(()),
            message::Subscriber::PublishOk(_msg) => Err(SessionError::unimplemented("PUBLISH_OK")),
            message::Subscriber::PublishError(_msg) => {
                Err(SessionError::unimplemented("PUBLISH_ERROR"))
            }
        };

        if let Err(err) = res {
            tracing::warn!(error = %err, "failed to process subscriber message");
        }

        Ok(())
    }

    /// Handle REQUEST_OK from subscriber — acceptance of our PUBLISH_NAMESPACE (draft-16 §9.7).
    fn recv_publish_namespace_ok(
        &mut self,
        msg: message::RequestOk,
    ) -> Result<(), SessionError> {
        // The publish_namespaces map is keyed by namespace; we must search by request_id.
        // TODO(itzmanish): maintain a second index keyed by request_id to make this O(1).
        let mut namespaces = self
            .publish_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?;
        if let Some(entry) = namespaces.iter_mut().find(|(_k, v)| v.request_id == msg.id) {
            entry.1.recv_ok()?;
        }

        Ok(())
    }

    /// Handle REQUEST_ERROR from subscriber — rejection of our PUBLISH_NAMESPACE (draft-16 §9.8).
    fn recv_publish_namespace_error(
        &mut self,
        msg: message::RequestError,
    ) -> Result<(), SessionError> {
        if let Some(recv) = self.drop_publish_namespace(msg.id) {
            recv.recv_error(ServeError::Closed(msg.error_code))?;
        }
        Ok(())
    }



    fn recv_publish_namespace_cancel(
        &mut self,
        msg: message::PublishNamespaceCancel,
    ) -> Result<(), SessionError> {
        // Draft-16 §9.24: PUBLISH_NAMESPACE_CANCEL now carries Request ID.
        if let Some(recv) = self.drop_publish_namespace(msg.id) {
            recv.recv_error(ServeError::Cancel)?;
        }
        Ok(())
    }

    fn recv_subscribe(&mut self, msg: message::Subscribe) -> Result<(), SessionError> {
        let namespace = msg.track_namespace.clone();

        let subscribed = {
            let mut subscribeds = self
                .subscribeds
                .lock()
                .map_err(|_| SessionError::Internal)?;

            let entry = match subscribeds.entry(msg.id) {
                hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
                hash_map::Entry::Vacant(entry) => entry,
            };

            let (send, recv) = Subscribed::new(self.clone(), msg, self.mlog.clone());
            entry.insert(recv);

            send
        };

        // Route to an active PUBLISH_NAMESPACE if present.
        if let Some(ns) = self
            .publish_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?
            .get_mut(&namespace)
        {
            return ns.recv_subscribe(subscribed).map_err(Into::into);
        }

        // Otherwise, surface it to the application via the unknown queue.
        if let Err(err) = self.unknown_subscribed.push(subscribed) {
            err.close(ServeError::not_found_ctx(format!(
                "unknown_subscribed queue full for namespace {:?}",
                namespace
            )))?;
        }

        Ok(())
    }

    fn recv_subscribe_update(
        &mut self,
        _msg: message::SubscribeUpdate,
    ) -> Result<(), SessionError> {
        // TODO: Implement REQUEST_UPDATE for subscriptions.
        Err(SessionError::unimplemented("SUBSCRIBE_UPDATE"))
    }

    fn recv_track_status(&mut self, msg: message::TrackStatus) -> Result<(), SessionError> {
        let namespace = msg.track_namespace.clone();

        let track_status_requested = TrackStatusRequested::new(self.clone(), msg);

        if let Some(ns) = self
            .publish_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?
            .get_mut(&namespace)
        {
            return ns
                .recv_track_status_requested(track_status_requested)
                .map_err(Into::into);
        }

        if let Err(mut err) = self
            .unknown_track_status_requested
            .push(track_status_requested)
        {
            err.respond_error(0, "Internal error")?;
        }

        Ok(())
    }

    fn recv_unsubscribe(&mut self, msg: message::Unsubscribe) -> Result<(), SessionError> {
        if let Some(subscribed) = self
            .subscribeds
            .lock()
            .map_err(|_| SessionError::Internal)?
            .get_mut(&msg.id)
        {
            subscribed.recv_unsubscribe()?;
        }

        Ok(())
    }

    /// Pre-send hook: clean up internal state when terminal publisher messages are enqueued.
    fn act_on_message_to_send<T: Into<message::Publisher>>(
        &mut self,
        msg: T,
    ) -> message::Publisher {
        let msg = msg.into();
        match &msg {
            message::Publisher::PublishDone(m) => self.drop_subscribe(m.id),
            message::Publisher::SubscribeError(m) => self.drop_subscribe(m.id),
            // Draft-16: PUBLISH_NAMESPACE_DONE carries Request ID, not namespace.
            // Dropping the recv state signals that the namespace is done.
            message::Publisher::PublishNamespaceDone(m) => {
                let _ = self.drop_publish_namespace(m.id);
            }
            _ => {}
        }
        msg
    }

    /// Enqueue a control message for sending (fire-and-forget).
    pub(super) fn send_message<T: Into<message::Publisher> + Into<Message>>(&mut self, msg: T) {
        let msg = self.act_on_message_to_send(msg);
        self.outgoing.push(msg.into()).ok();
    }

    /// Enqueue a control message and wait until it has been dequeued for sending.
    pub(super) async fn send_message_and_wait<T: Into<message::Publisher> + Into<Message>>(
        &mut self,
        msg: T,
    ) {
        let msg = self.act_on_message_to_send(msg);
        self.outgoing
            .push_and_wait_until_popped(msg.into())
            .await
            .ok();
    }

    fn drop_subscribe(&mut self, id: u64) {
        if let Ok(mut s) = self.subscribeds.lock() {
            s.remove(&id);
        }
    }

    fn drop_publish_namespace(&mut self, id: u64) -> Option<PublishNamespaceRecv> {
        if let Ok(mut ns) = self.publish_namespaces.lock() {
            let key = ns
                .iter()
                .find(|(_k, v)| v.request_id == id)
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                return ns.remove(&key);
            }
        }
        None
    }

    pub(super) async fn open_uni(&mut self) -> Result<web_transport::SendStream, SessionError> {
        Ok(self.webtransport.open_uni().await?)
    }

    pub(super) async fn send_datagram(&mut self, data: bytes::Bytes) -> Result<(), SessionError> {
        Ok(self.webtransport.send_datagram(data).await?)
    }
}
