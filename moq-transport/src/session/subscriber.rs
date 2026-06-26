// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{hash_map, HashMap},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::Notify;

use crate::{
    coding::{Decode, TrackName, TrackNamespace},
    data,
    message::{self, Message},
    mlog,
    serve::{self, ServeError},
};

use crate::watch::Queue;

use super::{
    PublishedNamespace, PublishedNamespaceRecv, Reader, RequestId, Session, SessionError,
    Subscribe, SubscribeRecv, Writer,
};

// Default timeout for waiting for subscribe aliases to become available via SUBSCRIBE_OK (1 second)
const DEFAULT_ALIAS_WAIT_TIME_MS: u64 = 1000;

// TODO remove Clone.
#[derive(Clone)]
pub struct Subscriber {
    /// Active inbound PUBLISH_NAMESPACE messages, keyed by namespace.
    published_namespaces: Arc<Mutex<HashMap<TrackNamespace, PublishedNamespaceRecv>>>,

    /// Queue of inbound PUBLISH_NAMESPACE events waiting to be consumed by the application.
    published_namespace_queue: Queue<PublishedNamespace>,

    /// The currently active outbound subscribes, keyed by request id.
    subscribes: Arc<Mutex<HashMap<u64, SubscribeRecv>>>,

    /// Map of track alias to subscription id for quick lookup when receiving streams/datagrams.
    subscribe_alias_map: Arc<Mutex<HashMap<u64, u64>>>,

    /// Notify when subscribe alias map is updated
    subscribe_alias_notify: Arc<Notify>,

    /// The queue we will write any outbound control messages we want to send, the session run_send task
    /// will process the queue and send the message on the control stream.
    outgoing: Queue<Message>,

    /// WebTransport session, used to open bidi streams for requests (draft-18).
    webtransport: web_transport::Session,

    /// Shared with Publisher so all requests within a session use unique IDs.
    /// When we need a new Request Id for sending a request, we can get it from here.
    /// The manager is shared with the Publisher, so the session uses unique request ids
    /// for all requests generated.  If we initiated the QUIC connection then request
    /// IDs start at 0 and increment by 2 (even numbers).  If we accepted an inbound
    /// QUIC connection then request IDs start at 1 and increment by 2 (odd numbers).
    request_id: RequestId,

    /// Optional mlog writer for logging transport events
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,

    /// Channel for sending bidi reader futures to `Session::run`, which polls
    /// them cooperatively under structured concurrency (no task is spawned).
    bidi_task_tx: super::BidiTaskSender,
}

impl Subscriber {
    pub(super) fn new(
        outgoing: Queue<Message>,
        webtransport: web_transport::Session,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        request_id: RequestId,
        bidi_task_tx: super::BidiTaskSender,
    ) -> Self {
        Self {
            published_namespaces: Default::default(),
            published_namespace_queue: Default::default(),
            subscribes: Default::default(),
            subscribe_alias_map: Default::default(),
            outgoing,
            webtransport,
            request_id,
            mlog,
            subscribe_alias_notify: Arc::new(Notify::new()),
            bidi_task_tx,
        }
    }

    /// Create an inbound/server QUIC connection, by accepting a bi-directional QUIC stream for control messages.
    pub async fn accept(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) = Session::accept(session, None, transport).await?;
        Ok((session, subscriber.unwrap()))
    }

    /// Create an outbound/client QUIC connection, by opening a bi-directional QUIC stream for control messages.
    pub async fn connect(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) = Session::connect(session, None, transport).await?;
        Ok((session, subscriber))
    }

    /// Wait for the next inbound PUBLISH_NAMESPACE from the peer, if any.
    pub async fn published_namespace(&mut self) -> Option<PublishedNamespace> {
        self.published_namespace_queue.pop().await
    }

    fn add_mlog_event<F>(&self, make_event: F)
    where
        F: FnOnce(f64) -> mlog::Event,
    {
        if let Some(ref mlog) = self.mlog {
            if let Ok(mut mlog) = mlog.lock() {
                let event = make_event(mlog.elapsed_ms());
                let _ = mlog.add_event(event);
            }
        }
    }

    fn log_request_ok_parsed(&self, request_kind: &str, msg: &message::RequestOk) {
        self.add_mlog_event(|time| mlog::events::request_ok_parsed(time, 0, request_kind, msg));
    }

    fn log_request_error_parsed(&self, request_kind: &str, msg: &message::RequestError) {
        self.add_mlog_event(|time| mlog::events::request_error_parsed(time, 0, request_kind, msg));
    }

    fn log_request_error_created(&self, request_kind: &str, msg: &message::RequestError) {
        self.add_mlog_event(|time| mlog::events::request_error_created(time, 0, request_kind, msg));
    }

    pub(super) fn send_request_ok(&mut self, request_kind: &str, msg: message::RequestOk) {
        self.add_mlog_event(|time| mlog::events::request_ok_created(time, 0, request_kind, &msg));
        self.send_message(msg);
    }

    pub(super) fn send_request_error(&mut self, request_kind: &str, msg: message::RequestError) {
        self.log_request_error_created(request_kind, &msg);
        self.send_message(msg);
    }

    /// Allocate the next outbound request ID.
    fn get_next_request_id(&mut self) -> Result<u64, SessionError> {
        self.request_id.allocate()
    }

    /// Open a bidirectional request stream (draft-18 §10), send a request
    /// message, and return the request stream's Writer (send side) together
    /// with a Reader for the response on the same stream.
    ///
    /// The Writer is returned (not dropped here) so the caller can keep the
    /// send side open and explicitly finish it on error paths, mirroring
    /// publisher.rs which holds its request writer in scope.
    async fn open_request_stream(
        &self,
        msg: &message::Message,
    ) -> Result<(Writer, Reader), SessionError> {
        let (send_stream, recv_stream) = self.webtransport.open_bi().await?;
        let mut writer = Writer::new(send_stream);
        writer.encode(msg).await?;
        Ok((writer, Reader::new(recv_stream)))
    }

    /// Send a TRACK_STATUS request for a track.
    pub fn track_status(
        &mut self,
        track_namespace: &TrackNamespace,
        track_name: impl Into<TrackName>,
    ) {
        let id = match self.get_next_request_id() {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(error = %e, "could not send TRACK_STATUS: request ID limit reached");
                return;
            }
        };
        self.send_message(message::TrackStatus {
            id,
            track_namespace: track_namespace.clone(),
            track_name: track_name.into(),
            params: Default::default(),
        });
        // TODO(itzmanish): make async and wait for response?
    }

    /// Subscribe to a track by creating a new subscribe request to the publisher.  Block until subscription is closed.
    pub async fn subscribe(&mut self, track: serve::TrackWriter) -> Result<(), ServeError> {
        let subscribe = self.subscribe_open(track).await?;
        subscribe.closed().await
    }

    /// Subscribe to a track and wait until the publisher acknowledges it.
    ///
    /// Draft-18: sends SUBSCRIBE on a new bidi request stream and reads
    /// the response (REQUEST_OK / REQUEST_ERROR) from the same stream.
    ///
    /// The caller must drive `Session::run` concurrently: the bidi response
    /// reader is polled by `run`, so the returned acknowledgement only
    /// resolves while `run` is being polled.
    pub async fn subscribe_open(
        &mut self,
        track: serve::TrackWriter,
    ) -> Result<Subscribe, ServeError> {
        let request_id = self
            .get_next_request_id()
            .map_err(|e| ServeError::internal_ctx(format!("request ID limit: {}", e)))?;
        let (send, recv, subscribe) = Subscribe::new(self.clone(), request_id, track);

        // Open a bidi stream and send the SUBSCRIBE message BEFORE
        // registering in the subscribes map — avoids a leaked entry if
        // open_request_stream fails. The wire message is the one built by
        // Subscribe::new, so it is not reconstructed here. The request writer
        // (send side) is held here so error paths can finish it explicitly.
        let subscribe_msg: Message = subscribe.into();
        let (mut request_writer, mut response_reader) = self
            .open_request_stream(&subscribe_msg)
            .await
            .map_err(|e| {
                ServeError::internal_ctx(format!("failed to open request stream: {}", e))
            })?;

        // Register the response state. If the lock is poisoned after the stream
        // is open, cleanly finish the send side (FIN) before bailing instead of
        // silently dropping it — mirrors the publisher.rs error-path handling.
        match self.subscribes.lock() {
            Ok(mut subscribes) => {
                subscribes.insert(request_id, recv);
            }
            Err(_) => {
                tracing::warn!(
                    request_id,
                    "subscribes lock poisoned after bidi stream open; finishing stream"
                );
                request_writer.finish();
                return Err(ServeError::internal_ctx("subscribe lock poisoned"));
            }
        }

        // Hand a reader future for bidi stream responses (draft-18) to
        // Session::run, which polls it cooperatively (structured concurrency).
        // No task is spawned; the future is dropped/cancelled on session exit.
        let mut subscriber_clone = self.clone();
        let _ = self.bidi_task_tx.send(Box::pin(async move {
            loop {
                match Session::decode_bidi_response(&mut response_reader, request_id).await {
                    Ok(msg) => {
                        if let Ok(pub_msg) = TryInto::<message::Publisher>::try_into(msg) {
                            if let Err(e) = subscriber_clone.recv_message(pub_msg) {
                                tracing::warn!(error = %e, "error handling bidi response");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, request_id, "bidi response reader ended");
                        break;
                    }
                }
            }
        }));

        send.ok().await?;
        Ok(send)
    }

    /// Send a message to the publisher via the control stream.
    pub(super) fn send_message<M: Into<message::Subscriber>>(&mut self, msg: M) {
        let msg = msg.into();

        // Remove our entry on terminal state.
        // Draft-16: PUBLISH_NAMESPACE_CANCEL carries Request ID, so look up
        // the namespace by iterating the map.
        if let message::Subscriber::PublishNamespaceCancel(msg) = &msg {
            let _ = self.drop_publish_namespace(msg.id);
        }

        // TODO report dropped messages?
        let _ = self.outgoing.push(msg.into());
    }

    /// Receive a message from the publisher via the control stream.
    pub(super) fn recv_message(&mut self, msg: message::Publisher) -> Result<(), SessionError> {
        match &msg {
            message::Publisher::PublishNamespace(msg) => self.recv_publish_namespace(msg)?,
            message::Publisher::PublishNamespaceDone(msg) => {
                self.recv_publish_namespace_done(msg)?;
            }
            // PUBLISH (publisher-initiated subscription) not yet implemented.
            // Send REQUEST_ERROR NOT_SUPPORTED so the publisher knows we cannot accept it.
            message::Publisher::Publish(msg) => {
                self.send_not_supported(msg.id, "publish");
            }
            message::Publisher::PublishDone(msg) => self.recv_publish_done(msg)?,
            message::Publisher::SubscribeOk(msg) => self.recv_subscribe_ok(msg)?,
            // Draft-16 shared responses (REQUEST_OK / REQUEST_ERROR).
            message::Publisher::RequestOk(msg) => self.recv_request_ok(msg)?,
            message::Publisher::RequestError(msg) => self.recv_request_error(msg)?,
            // FETCH_OK is part of draft-16, but FETCH is not implemented here yet.
            message::Publisher::FetchOk(msg) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    request_id = msg.id,
                    "received FETCH_OK for unsupported FETCH — ignoring"
                );
            }
        }

        Ok(())
    }

    /// Send REQUEST_ERROR NOT_SUPPORTED for an incoming request we do not implement.
    ///
    /// Draft-16 §4: limited endpoints SHOULD respond with NOT_SUPPORTED rather
    /// than ignoring unsupported request types.
    fn send_not_supported(&mut self, request_id: u64, request_kind: &str) {
        tracing::debug!(
            target: "moq_transport::control",
            request_id,
            "sending REQUEST_ERROR NOT_SUPPORTED for unimplemented request"
        );
        self.send_request_error(
            request_kind,
            message::RequestError {
                id: request_id,
                error_code: crate::message::RequestErrorCode::NotSupported as u64,
                retry_interval: 0,
                reason: crate::coding::ReasonPhrase("not supported".to_string()),
            },
        );
    }

    /// Handle reception of an inbound PUBLISH_NAMESPACE from the publisher.
    fn recv_publish_namespace(
        &mut self,
        msg: &message::PublishNamespace,
    ) -> Result<(), SessionError> {
        let mut published_namespaces = self
            .published_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?;

        // Duplicate PUBLISH_NAMESPACE for the same namespace within a session is invalid.
        let entry = match published_namespaces.entry(msg.track_namespace.clone()) {
            hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (published_ns, recv) =
            PublishedNamespace::new(self.clone(), msg.id, msg.track_namespace.clone());
        if let Err(published_ns) = self.published_namespace_queue.push(published_ns) {
            published_ns.close(ServeError::Cancel)?;
            return Ok(());
        }
        entry.insert(recv);

        Ok(())
    }

    /// Handle reception of PUBLISH_NAMESPACE_DONE from the publisher.
    fn recv_publish_namespace_done(
        &mut self,
        msg: &message::PublishNamespaceDone,
    ) -> Result<(), SessionError> {
        // Draft-16 §9.22: PUBLISH_NAMESPACE_DONE carries Request ID, not namespace.
        if let Some(recv) = self.drop_publish_namespace(msg.id) {
            recv.recv_done()?;
        }
        Ok(())
    }

    /// Handle the reception of a SubscribeOk message from the publisher.
    fn recv_subscribe_ok(&mut self, msg: &message::SubscribeOk) -> Result<(), SessionError> {
        if let Some(subscribe) = self
            .subscribes
            .lock()
            .map_err(|_| SessionError::Internal)?
            .get_mut(&msg.id)
        {
            // Map track alias to subscription id for quick lookup when receiving streams/datagrams
            self.subscribe_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?
                .insert(msg.track_alias, msg.id);

            // Notify waiting tasks that the alias map has been updated
            self.subscribe_alias_notify.notify_waiters();

            // Notify the subscribe of the successful subscription
            subscribe.ok(msg.track_alias)?;
        }

        Ok(())
    }

    /// Remove a subscribe from our map of active subscribes, and the alias map if present.
    pub(super) fn remove_subscribe(&mut self, id: u64) -> Option<SubscribeRecv> {
        let subscribe = self.subscribes.lock().ok().and_then(|mut s| s.remove(&id));
        if let Some(ref sub) = subscribe {
            if let Some(track_alias) = sub.track_alias() {
                if let Ok(mut alias_map) = self.subscribe_alias_map.lock() {
                    alias_map.remove(&track_alias);
                }
            }
        }
        subscribe
    }

    /// Handle the reception of a PublishDone message from the publisher.
    fn recv_publish_done(&mut self, msg: &message::PublishDone) -> Result<(), SessionError> {
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.status_code))?;
        }

        Ok(())
    }

    /// Handle REQUEST_OK from the publisher.
    ///
    /// REQUEST_OK is the shared positive response for REQUEST_UPDATE, TRACK_STATUS,
    /// SUBSCRIBE_NAMESPACE, and PUBLISH_NAMESPACE.  SUBSCRIBE uses its own dedicated
    /// SUBSCRIBE_OK message (§9.10) and does not come through this handler.
    /// Full routing for the other request types is wired up (TODO itzmanish).
    fn recv_request_ok(&mut self, msg: &message::RequestOk) -> Result<(), SessionError> {
        self.log_request_ok_parsed("unknown", msg);
        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            "received REQUEST_OK"
        );
        // TODO(itzmanish): route to the correct pending request type by ID.
        Ok(())
    }

    /// Handle REQUEST_ERROR from the publisher.
    ///
    /// Routes to the matching active subscribe (via request ID) if one
    /// exists, otherwise logs and ignores.  Full per-flow routing is
    /// wired up (TODO itzmanish).
    fn recv_request_error(&mut self, msg: &message::RequestError) -> Result<(), SessionError> {
        // Route to a matching subscribe if present.
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            self.log_request_error_parsed("subscribe", msg);
            let err = Self::request_error_to_serve_error(msg);
            subscribe.error(err)?;
        } else {
            self.log_request_error_parsed("unknown", msg);
        }

        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            error_code = msg.error_code,
            retry_interval = msg.retry_interval,
            reason = %msg.reason.0,
            "received REQUEST_ERROR"
        );
        Ok(())
    }

    /// Map a REQUEST_ERROR to a semantic ServeError so callers see
    /// meaningful variants (e.g. NotFound) instead of opaque error codes.
    fn request_error_to_serve_error(msg: &message::RequestError) -> ServeError {
        use message::RequestErrorCode;
        match msg.error_code {
            c if c == RequestErrorCode::DoesNotExist as u64 => {
                ServeError::not_found_ctx(msg.reason.0.clone())
            }
            c if c == RequestErrorCode::InternalError as u64 => {
                ServeError::internal_ctx(msg.reason.0.clone())
            }
            c if c == RequestErrorCode::DuplicateSubscription as u64 => ServeError::Duplicate,
            c if c == RequestErrorCode::NotSupported as u64 => {
                ServeError::NotImplemented(msg.reason.0.clone())
            }
            code => ServeError::Closed(code),
        }
    }

    fn drop_publish_namespace(&mut self, id: u64) -> Option<PublishedNamespaceRecv> {
        if let Ok(mut ns) = self.published_namespaces.lock() {
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

    /// Get a subscribe id by track alias, waiting up to the specified timeout if not present.
    /// If timeout_ms is None, only check if already present and return None if not.
    async fn get_subscribe_id_by_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Result<Option<u64>, SessionError> {
        // If no timeout specified, don't wait
        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => {
                // Just check once
                return match self.subscribe_alias_map.lock() {
                    Ok(aliases) => Ok(aliases.get(&track_alias).cloned()),
                    Err(_) => {
                        tracing::error!(
                            target: "moq_transport::control",
                            track_alias,
                            "subscribe alias map lock poisoned"
                        );
                        Err(SessionError::Internal)
                    }
                };
            }
        };

        // Wait for it to appear, checking after each notification
        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                // Register for notification before checking map
                let notified = self.subscribe_alias_notify.notified();

                // Check Map for alias
                let id = match self.subscribe_alias_map.lock() {
                    Ok(aliases) => aliases.get(&track_alias).cloned(),
                    Err(_) => {
                        tracing::error!(
                            target: "moq_transport::control",
                            track_alias,
                            "subscribe alias map lock poisoned"
                        );
                        return Err(SessionError::Internal);
                    }
                };

                if let Some(id) = id {
                    return Ok(Some(id));
                }

                // Alias not present yet, wait for notification
                notified.await;
            }
        })
        .await
        .unwrap_or(Ok(None))
    }

    /// Handle reception of a new stream from the QUIC session.
    pub(super) async fn recv_stream(
        mut self,
        stream: web_transport::RecvStream,
    ) -> Result<(), SessionError> {
        tracing::trace!("[SUBSCRIBER] recv_stream: new stream received, decoding header");
        let mut reader = Reader::new(stream);

        // Decode the stream header
        let stream_header: data::StreamHeader = reader.decode().await?;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream: decoded stream header type={:?}",
            stream_header.header_type
        );

        // No fetch support yet
        if !stream_header.header_type.is_subgroup() {
            return Err(SessionError::unimplemented("non-SUBGROUP stream types"));
        }

        // Log subgroup header parsed/received
        if let Some(ref subgroup_header) = stream_header.subgroup_header {
            if let Some(ref mlog) = self.mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let event = mlog::subgroup_header_parsed(time, stream_id, subgroup_header);
                    let _ = mlog_guard.add_event(event);
                }
            }
        }

        let track_alias = stream_header.subgroup_header.as_ref().unwrap().track_alias;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream: stream for subscription track_alias={}",
            track_alias
        );

        let mlog = self.mlog.clone();
        let res = self.recv_stream_inner(reader, stream_header, mlog).await;
        if let Err(SessionError::Serve(err)) = &res {
            tracing::warn!(
                "[SUBSCRIBER] recv_stream: stream processing error for track_alias={}: {:?}",
                track_alias,
                err
            );
            // The writer is closed, so we should terminate.
            // TODO it would be nice to do this immediately when the Writer is closed.
            if let Some(subscribe_id) = self.get_subscribe_id_by_alias(track_alias, None).await? {
                if let Some(subscribe) = self.remove_subscribe(subscribe_id) {
                    subscribe.error(err.clone())?;
                }
            }
        }

        res
    }

    /// Continue handling the reception of a new stream from the QUIC session.
    async fn recv_stream_inner(
        &mut self,
        reader: Reader,
        stream_header: data::StreamHeader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let track_alias = stream_header.subgroup_header.as_ref().unwrap().track_alias;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream_inner: processing stream for track_alias={}",
            track_alias
        );

        let Some(subscribe_id) = self
            .get_subscribe_id_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?
        else {
            return Err(SessionError::Serve(ServeError::not_found_ctx(format!(
                "subscription track_alias={} not found",
                track_alias
            ))));
        };

        tracing::trace!("[SUBSCRIBER] recv_stream_inner: receiving subgroup data");
        self.recv_subgroup(
            stream_header.header_type,
            stream_header.subgroup_header.unwrap(),
            subscribe_id,
            reader,
            mlog,
        )
        .await?;

        tracing::trace!(
            "[SUBSCRIBER] recv_stream_inner: completed processing stream for track_alias={}",
            track_alias
        );
        Ok(())
    }

    /// If new stream is a Subgroup stream, handle reception of subgroup objects and payloads.
    async fn recv_subgroup(
        &mut self,
        stream_header_type: data::StreamHeaderType,
        mut subgroup_header: data::SubgroupHeader,
        subscribe_id: u64,
        mut reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        tracing::trace!(
            "[SUBSCRIBER] recv_subgroup: starting - group_id={}, subgroup_id={:?}, priority={}",
            subgroup_header.group_id,
            subgroup_header.subgroup_id,
            subgroup_header.publisher_priority
        );

        let mut object_count = 0;
        let mut previous_object_id: Option<u64> = None;
        let mut subgroup_writer: Option<serve::SubgroupWriter> = None;
        while !reader.done().await? {
            tracing::trace!(
                "[SUBSCRIBER] recv_subgroup: reading object #{} (has_ext_headers={})",
                object_count + 1,
                stream_header_type.has_extension_headers()
            );

            // Need to be able to decode the subgroup object conditionally based on the stream header type
            // read the object payload length into remaining_bytes
            let (mut remaining_bytes, object_id_delta, status, decoded_object) =
                match stream_header_type.has_extension_headers() {
                    true => {
                        let object = reader.decode::<data::SubgroupObjectExt>().await?;
                        tracing::trace!(
                        "[SUBSCRIBER] recv_subgroup: object #{} with extension headers - object_id_delta={}, payload_length={}, status={:?}, extension_headers={:?}",
                        object_count + 1,
                        object.object_id_delta,
                        object.payload_length,
                        object.status,
                        object.extension_headers
                    );

                        // Check for known draft-14 extension types

                        // Check for Immutable Extensions (type 0xB = 11)
                        if object.extension_headers.has(0xB) {
                            tracing::trace!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains IMMUTABLE EXTENSIONS (type 0xB) - will be forwarded",
                                object_count + 1
                            );
                            if let Some(immutable_ext) = object.extension_headers.get(0xB) {
                                tracing::trace!(
                                    "[SUBSCRIBER] recv_subgroup: immutable extension details: {:?}",
                                    immutable_ext
                                );
                            }
                        }

                        // Check for Prior Group ID Gap (type 0x3C = 60)
                        if object.extension_headers.has(0x3C) {
                            tracing::trace!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains PRIOR GROUP ID GAP (type 0x3C)",
                                object_count + 1
                            );
                            if let Some(gap_ext) = object.extension_headers.get(0x3C) {
                                tracing::trace!(
                                    "[SUBSCRIBER] recv_subgroup: prior group id gap details: {:?}",
                                    gap_ext
                                );
                            }
                        }

                        let obj_copy = object.clone();
                        (
                            object.payload_length,
                            object.object_id_delta,
                            object.status,
                            Some(obj_copy),
                        )
                    }
                    false => {
                        let object = reader.decode::<data::SubgroupObject>().await?;
                        tracing::trace!(
                        "[SUBSCRIBER] recv_subgroup: object #{} - object_id_delta={}, payload_length={}, status={:?}",
                        object_count + 1,
                        object.object_id_delta,
                        object.payload_length,
                        object.status
                    );
                        (
                            object.payload_length,
                            object.object_id_delta,
                            object.status,
                            None,
                        )
                    }
                };

            let current_object_id = match previous_object_id {
                Some(previous) => previous
                    .checked_add(object_id_delta)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        SessionError::ProtocolViolation("subgroup object id overflow".to_string())
                    })?,
                None => object_id_delta,
            };
            previous_object_id = Some(current_object_id);

            // Extract extension headers if present
            let extension_headers = decoded_object
                .as_ref()
                .map(|obj| obj.extension_headers.clone());

            if status.is_some_and(|status| status != data::ObjectStatus::NormalObject)
                && extension_headers
                    .as_ref()
                    .is_some_and(|headers| !headers.is_empty())
            {
                return Err(SessionError::ProtocolViolation(
                    "non-normal object status with extension headers".to_string(),
                ));
            }

            if subgroup_writer.is_none() {
                if stream_header_type.uses_first_object_id_as_subgroup_id() {
                    subgroup_header.subgroup_id = Some(current_object_id);
                }

                let mut subscribes = self.subscribes.lock().map_err(|_| SessionError::Internal)?;
                let subscribe = subscribes.get_mut(&subscribe_id).ok_or_else(|| {
                    ServeError::not_found_ctx(format!(
                        "subscribe_id={} not found for track_alias={}",
                        subscribe_id, subgroup_header.track_alias
                    ))
                })?;

                subgroup_writer = Some(subscribe.subgroup(subgroup_header.clone())?);
            }

            // Log subgroup object parsed/received
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let event = if let Some(obj_ext) = decoded_object {
                        mlog::subgroup_object_ext_parsed(
                            time,
                            stream_id,
                            subgroup_header.group_id,
                            subgroup_header.subgroup_id.unwrap_or(0),
                            current_object_id,
                            &obj_ext,
                        )
                    } else {
                        // For non-extension objects, create a temporary SubgroupObject for logging
                        let temp_obj = data::SubgroupObject {
                            object_id_delta,
                            payload_length: remaining_bytes,
                            status,
                        };
                        mlog::subgroup_object_parsed(
                            time,
                            stream_id,
                            subgroup_header.group_id,
                            subgroup_header.subgroup_id.unwrap_or(0),
                            current_object_id,
                            &temp_obj,
                        )
                    };
                    let _ = mlog_guard.add_event(event);
                }
            }

            // Pass extension headers through to the serve layer
            // TODO SLG - object_id_delta and object status are still being ignored

            let subgroup_writer = subgroup_writer.as_mut().ok_or(SessionError::Internal)?;
            let mut object_writer = subgroup_writer.create(remaining_bytes, extension_headers)?;
            tracing::trace!(
                "[SUBSCRIBER] recv_subgroup: reading payload for object #{} ({} bytes)",
                object_count + 1,
                remaining_bytes
            );

            let mut chunks_read = 0;
            while remaining_bytes > 0 {
                let data = reader
                    .read_chunk(remaining_bytes)
                    .await?
                    .ok_or_else(|| {
                        tracing::error!(
                            "[SUBSCRIBER] recv_subgroup: ERROR - stream ended with {} bytes remaining for object #{}",
                            remaining_bytes,
                            object_count + 1
                        );
                        SessionError::WrongSize
                    })?;
                tracing::trace!(
                    "[SUBSCRIBER] recv_subgroup: received payload chunk #{} for object #{} ({} bytes, {} remaining)",
                    chunks_read + 1,
                    object_count + 1,
                    data.len(),
                    remaining_bytes - data.len()
                );
                remaining_bytes -= data.len();
                object_writer.write(data)?;
                chunks_read += 1;
            }

            tracing::trace!(
                "[SUBSCRIBER] recv_subgroup: completed object #{} ({} chunks)",
                object_count + 1,
                chunks_read
            );
            object_count += 1;
        }

        tracing::trace!(
            "[SUBSCRIBER] recv_subgroup: completed subgroup (group_id={}, subgroup_id={}, {} objects received)",
            subgroup_header.group_id,
            subgroup_header.subgroup_id.unwrap_or(0),
            object_count
        );

        Ok(())
    }

    /// Handle reception of a datagram from the QUIC session.
    pub async fn recv_datagram(&mut self, datagram: bytes::Bytes) -> Result<(), SessionError> {
        let mut cursor = io::Cursor::new(datagram);
        let datagram = data::Datagram::decode(&mut cursor)?;

        if let Some(ref mlog) = self.mlog {
            if let Ok(mut mlog_guard) = mlog.lock() {
                let time = mlog_guard.elapsed_ms();
                let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                let _ =
                    mlog_guard.add_event(mlog::object_datagram_parsed(time, stream_id, &datagram));
            }
        }

        // Check for extension headers in the datagram
        if let Some(ref ext_headers) = datagram.extension_headers {
            tracing::trace!(
                "[SUBSCRIBER] recv_datagram: datagram contains extension headers: {:?}",
                ext_headers
            );

            // Check for known draft-14 extension types

            // Check for Immutable Extensions (type 0xB = 11)
            if ext_headers.has(0xB) {
                tracing::trace!(
                    "[SUBSCRIBER] recv_datagram: datagram contains IMMUTABLE EXTENSIONS (type 0xB)"
                );
                if let Some(immutable_ext) = ext_headers.get(0xB) {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram: immutable extension details: {:?}",
                        immutable_ext
                    );
                }
            }

            // Check for Prior Group ID Gap (type 0x3C = 60)
            if ext_headers.has(0x3C) {
                tracing::trace!(
                    "[SUBSCRIBER] recv_datagram: datagram contains PRIOR GROUP ID GAP (type 0x3C)"
                );
                if let Some(gap_ext) = ext_headers.get(0x3C) {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram: prior group id gap details: {:?}",
                        gap_ext
                    );
                }
            }
        }

        // Look up the subscribe id for this track alias
        if let Some(subscribe_id) = self
            .get_subscribe_id_by_alias(datagram.track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?
        {
            // Look up the subscribe by id
            if let Some(subscribe) = self
                .subscribes
                .lock()
                .ok()
                .as_mut()
                .and_then(|s| s.get_mut(&subscribe_id))
            {
                tracing::trace!(
                    "[SUBSCRIBER] recv_datagram: track_alias={}, group_id={}, object_id={}, publisher_priority={}, status={}, payload_length={}",
                    datagram.track_alias,
                    datagram.group_id,
                    datagram.object_id.unwrap_or(0),
                    datagram.publisher_priority,
                    datagram.status.as_ref().map_or("None".to_string(), |s| format!("{:?}", s)),
                    datagram.payload.as_ref().map_or(0, |p| p.len()));
                subscribe.datagram(datagram)?;
            }
        } else {
            tracing::warn!(
                "[SUBSCRIBER] recv_datagram: discarded due to unknown track_alias: track_alias={}, group_id={}, object_id={}, publisher_priority={}, status={}, payload_length={}",
                datagram.track_alias,
                datagram.group_id,
                datagram.object_id.unwrap_or(0),
                datagram.publisher_priority,
                datagram.status.as_ref().map_or("None".to_string(), |s| format!("{:?}", s)),
                datagram.payload.as_ref().map_or(0, |p| p.len()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Establish a real `web_transport::Session` over an in-process QUIC
    /// loopback so we can construct a `Subscriber` and exercise the actual
    /// cleanup paths (`Subscribe::Drop` and `Subscriber::remove_subscribe`).
    ///
    /// The session is never dialed by these tests — the cleanup logic only
    /// touches the in-memory maps and the outgoing queue — but `Subscriber`
    /// holds a concrete `web_transport::Session`, so a live connection is the
    /// only honest way to build one. The accepted server side is parked for
    /// the lifetime of the test to keep the client session established.
    async fn loopback_session() -> web_transport::Session {
        use web_transport::quinn::{ClientBuilder, ServerBuilder};

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed certificate");
        let cert_der = cert.cert.der().clone();
        let key = cert
            .key_pair
            .serialize_der()
            .try_into()
            .expect("serialize private key");

        let mut server = ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().expect("server addr"))
            .with_certificate(vec![cert_der], key)
            .expect("build loopback server");
        let addr = server.local_addr().expect("server local addr");

        tokio::spawn(async move {
            if let Some(request) = server.accept().await {
                // Hold the accepted server session open for the lifetime of the
                // test so the client side stays established; the spawned task is
                // cancelled at runtime shutdown when the test returns.
                if let Ok(_session) = request.ok().await {
                    std::future::pending::<()>().await;
                }
            }
        });

        let client = ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()
            .expect("build loopback client");
        let url = url::Url::parse(&format!("https://127.0.0.1:{}/", addr.port()))
            .expect("parse loopback url");

        client
            .connect(url)
            .await
            .expect("connect loopback session")
            .into()
    }

    fn test_subscriber(session: web_transport::Session) -> Subscriber {
        let outgoing = Queue::default().split().0;
        let (bidi_task_tx, _bidi_task_rx) = tokio::sync::mpsc::unbounded_channel();
        Subscriber::new(outgoing, session, None, RequestId::new(0, 1), bidi_task_tx)
    }

    fn test_track(name: &str) -> serve::TrackWriter {
        let (writer, _reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("test/ns"), name).produce();
        writer
    }

    /// `Subscribe::drop` must remove the request's entry from the subscribes map
    /// (the real Drop impl calls `Subscriber::remove_subscribe`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_subscribe_removes_recv_state() {
        let subscriber = test_subscriber(loopback_session().await);
        let request_id = 4;

        let (subscribe, recv, _msg) =
            Subscribe::new(subscriber.clone(), request_id, test_track("video"));
        // Mimic subscribe_open: register the recv state under the request id.
        subscriber
            .subscribes
            .lock()
            .unwrap()
            .insert(request_id, recv);
        assert!(
            subscriber
                .subscribes
                .lock()
                .unwrap()
                .contains_key(&request_id),
            "precondition: subscribe should be registered"
        );

        drop(subscribe);

        assert!(
            !subscriber
                .subscribes
                .lock()
                .unwrap()
                .contains_key(&request_id),
            "Subscribe::drop should remove the subscribes-map entry"
        );
    }

    /// `remove_subscribe` must clear both `subscribes` and `subscribe_alias_map`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_subscribe_clears_alias_map() {
        let mut subscriber = test_subscriber(loopback_session().await);
        let request_id = 6;
        let track_alias = 42;

        let (subscribe, mut recv, _msg) =
            Subscribe::new(subscriber.clone(), request_id, test_track("audio"));
        // Record the track alias while the Subscribe (the state's send half) is
        // still alive, so the recv state accepts the mutation. Then drop the
        // handle — its Drop runs against the still-empty map (a no-op) — and
        // drive remove_subscribe directly via the registered recv state.
        recv.ok(track_alias).unwrap();
        drop(subscribe);
        subscriber
            .subscribes
            .lock()
            .unwrap()
            .insert(request_id, recv);
        subscriber
            .subscribe_alias_map
            .lock()
            .unwrap()
            .insert(track_alias, request_id);

        let removed = subscriber.remove_subscribe(request_id);

        assert!(
            removed.is_some(),
            "remove_subscribe should return the removed recv state"
        );
        assert!(
            !subscriber
                .subscribes
                .lock()
                .unwrap()
                .contains_key(&request_id),
            "remove_subscribe should clear the subscribes map"
        );
        assert!(
            !subscriber
                .subscribe_alias_map
                .lock()
                .unwrap()
                .contains_key(&track_alias),
            "remove_subscribe should clear the subscribe_alias_map"
        );
    }
}
