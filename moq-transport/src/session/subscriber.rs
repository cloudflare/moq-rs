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
    coding::{Decode, TrackNamespace},
    data,
    message::{self, FilterType, GroupOrder, Message},
    mlog,
    serve::{self, ServeError},
};

use crate::watch::Queue;

use super::{
    PublishedNamespace, PublishedNamespaceRecv, Reader, RequestId, RequestIdAllocation, Session,
    SessionError, Subscribe, SubscribeRecv,
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

    /// Shared with Publisher so all requests within a session use unique IDs.
    /// When we need a new Request Id for sending a request, we can get it from here.
    /// The manager is shared with the Publisher, so the session uses unique request ids
    /// for all requests generated.  If we initiated the QUIC connection then request
    /// IDs start at 0 and increment by 2 (even numbers).  If we accepted an inbound
    /// QUIC connection then request IDs start at 1 and increment by 2 (odd numbers).
    request_id: RequestId,

    /// Optional mlog writer for logging transport events
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Subscriber {
    pub(super) fn new(
        outgoing: Queue<Message>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        request_id: RequestId,
    ) -> Self {
        Self {
            published_namespaces: Default::default(),
            published_namespace_queue: Default::default(),
            subscribes: Default::default(),
            subscribe_alias_map: Default::default(),
            outgoing,
            request_id,
            mlog,
            subscribe_alias_notify: Arc::new(Notify::new()),
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

    /// Allocate the next outbound request ID, enforcing the peer-advertised maximum.
    ///
    /// Returns `Err(TooManyRequests)` if no budget remains and also sends
    /// REQUESTS_BLOCKED if not already sent for this limit.
    fn get_next_request_id(&mut self) -> Result<u64, SessionError> {
        match self.request_id.allocate()? {
            RequestIdAllocation::Allocated(id) => Ok(id),
            blocked @ RequestIdAllocation::Blocked { .. } => {
                if let Some(msg) = blocked.requests_blocked() {
                    let _ = self.outgoing.push(msg.into());
                }
                Err(SessionError::TooManyRequests)
            }
        }
    }

    pub fn track_status(&mut self, track_namespace: &TrackNamespace, track_name: &str) {
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
            track_name: track_name.to_string(),
            subscriber_priority: 127, // default to mid value, see: https://github.com/moq-wg/moq-transport/issues/504
            group_order: GroupOrder::Publisher, // defer to publisher send order
            forward: true,            // default to forwarding objects
            filter_type: FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
            params: Default::default(),
        });
        // TODO(itzmanish): make async and wait for response?
    }

    /// Subscribe to a track by creating a new subscribe request to the publisher.  Block until subscription is closed.
    pub async fn subscribe(&mut self, track: serve::TrackWriter) -> Result<(), ServeError> {
        let request_id = self
            .get_next_request_id()
            .map_err(|e| ServeError::internal_ctx(format!("request ID limit: {}", e)))?;
        let (send, recv) = Subscribe::new(self.clone(), request_id, track);
        self.subscribes
            .lock()
            .map_err(|_| ServeError::internal_ctx("subscribe lock poisoned"))?
            .insert(request_id, recv);

        send.closed().await
    }

    /// Send a message to the publisher via the control stream.
    pub(super) fn send_message<M: Into<message::Subscriber>>(&mut self, msg: M) {
        let msg = msg.into();

        // Remove our entry on terminal state.
        // Draft-16: PUBLISH_NAMESPACE_CANCEL carries Request ID, so look up
        // the namespace by iterating the map.
        match &msg {
            // Dropping the returned recv signals the PublishedNamespace that
            // the cancel has been sent.
            message::Subscriber::PublishNamespaceCancel(msg) => {
                let _ = self.drop_publish_namespace(msg.id);
            }
            // PublishNamespaceError is the legacy stub (internal only; not sent on wire).
            // REQUEST_ERROR is now used for rejections.
            message::Subscriber::PublishNamespaceError(_msg) => {}
            _ => {}
        }

        // TODO report dropped messages?
        let _ = self.outgoing.push(msg.into());
    }

    /// Receive a message from the publisher via the control stream.
    pub(super) fn recv_message(&mut self, msg: message::Publisher) -> Result<(), SessionError> {
        let res = match &msg {
            message::Publisher::PublishNamespace(msg) => self.recv_publish_namespace(msg),
            message::Publisher::PublishNamespaceDone(msg) => self.recv_publish_namespace_done(msg),
            message::Publisher::Publish(_msg) => Err(SessionError::unimplemented("PUBLISH")),
            message::Publisher::PublishDone(msg) => self.recv_publish_done(msg),
            message::Publisher::SubscribeOk(msg) => self.recv_subscribe_ok(msg),
            // Draft-16 shared responses (REQUEST_OK / REQUEST_ERROR).
            message::Publisher::RequestOk(msg) => self.recv_request_ok(msg),
            message::Publisher::RequestError(msg) => self.recv_request_error(msg),
            // Legacy stubs retained for dispatch (TODO itzmanish: replace with REQUEST_OK/ERROR routing).
            message::Publisher::SubscribeError(msg) => self.recv_subscribe_error(msg),
            message::Publisher::TrackStatusOk(msg) => self.recv_track_status_ok(msg),
            message::Publisher::TrackStatusError(_msg) => {
                Err(SessionError::unimplemented("TRACK_STATUS_ERROR"))
            }
            message::Publisher::FetchOk(_msg) => Err(SessionError::unimplemented("FETCH_OK")),
            message::Publisher::FetchError(_msg) => Err(SessionError::unimplemented("FETCH_ERROR")),
            message::Publisher::SubscribeNamespaceOk(_msg) => {
                Err(SessionError::unimplemented("SUBSCRIBE_NAMESPACE_OK"))
            }
            message::Publisher::SubscribeNamespaceError(_msg) => {
                Err(SessionError::unimplemented("SUBSCRIBE_NAMESPACE_ERROR"))
            }
        };

        if let Err(SessionError::Serve(err)) = res {
            tracing::debug!("failed to process message: {:?} {}", msg, err);
            return Ok(());
        }

        res
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
    fn remove_subscribe(&mut self, id: u64) -> Option<SubscribeRecv> {
        let subscribe = self
            .subscribes
            .lock()
            .ok()
            .and_then(|mut s| s.remove(&id));
        if let Some(ref sub) = subscribe {
            if let Some(track_alias) = sub.track_alias() {
                if let Ok(mut alias_map) = self.subscribe_alias_map.lock() {
                    alias_map.remove(&track_alias);
                }
            }
        }
        subscribe
    }

    /// Handle the reception of a SubscribeError message from the publisher.
    fn recv_subscribe_error(&mut self, msg: &message::SubscribeError) -> Result<(), SessionError> {
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.error_code))?;
        }

        Ok(())
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
    /// REQUEST_OK is a shared response for REQUEST_UPDATE, TRACK_STATUS,
    /// SUBSCRIBE_NAMESPACE and PUBLISH_NAMESPACE.  For now we log it and
    /// route by best-effort to an active subscribe (for REQUEST_UPDATE
    /// responses) or ignore it (for other types).  Full routing is wired
    /// up per-flow (TODO itzmanish).
    fn recv_request_ok(&mut self, msg: &message::RequestOk) -> Result<(), SessionError> {
        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            "received REQUEST_OK"
        );
        // TODO(itzmanish): route to the correct pending request by ID.
        Ok(())
    }

    /// Handle REQUEST_ERROR from the publisher.
    ///
    /// Routes to the matching active subscribe (via request ID) if one
    /// exists, otherwise logs and ignores.  Full per-flow routing is
    /// wired up (TODO itzmanish).
    fn recv_request_error(&mut self, msg: &message::RequestError) -> Result<(), SessionError> {
        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            error_code = msg.error_code,
            retry_interval = msg.retry_interval,
            reason = %msg.reason.0,
            "received REQUEST_ERROR"
        );
        // Route to a matching subscribe if present.
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.error_code))?;
        }
        Ok(())
    }

    /// Handle the reception of a TrackStatusOk message from the publisher (legacy).
    fn recv_track_status_ok(&mut self, _msg: &message::TrackStatusOk) -> Result<(), SessionError> {
        // TODO: Expose this somehow?
        // TODO: Also add a way to send a Track Status Request in the first place
        Ok(())
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
    ) -> Option<u64> {
        // If no timeout specified, don't wait
        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => {
                // Just check once
                return self
                    .subscribe_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned();
            }
        };

        // Wait for it to appear, checking after each notification
        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                // Register for notification before checking map
                let notified = self.subscribe_alias_notify.notified();

                // Check Map for alias
                if let Some(id) = self
                    .subscribe_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned()
                {
                    return id;
                }

                // Alias not present yet, wait for notification
                notified.await;
            }
        })
        .await
        .ok()
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
        tracing::debug!(
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
            if let Some(subscribe_id) = self.get_subscribe_id_by_alias(track_alias, None).await {
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

        // This is super silly, but I couldn't figure out a way to avoid the mutex guard across awaits.
        enum Writer {
            //Fetch(serve::FetchWriter),
            Subgroup(serve::SubgroupWriter),
        }

        let writer = {
            // Look up the subscribe id for this track alias
            if let Some(subscribe_id) = self
                .get_subscribe_id_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
                .await
            {
                // Look up the subscribe by id
                let mut subscribes = self
                    .subscribes
                    .lock()
                    .map_err(|_| SessionError::Internal)?;
                let subscribe = subscribes.get_mut(&subscribe_id).ok_or_else(|| {
                    ServeError::not_found_ctx(format!(
                        "subscribe_id={} not found for track_alias={}",
                        subscribe_id, track_alias
                    ))
                })?;

                // Create the appropriate writer based on the stream header type
                if stream_header.header_type.is_subgroup() {
                    tracing::trace!("[SUBSCRIBER] recv_stream_inner: creating subgroup writer");
                    Writer::Subgroup(subscribe.subgroup(stream_header.subgroup_header.unwrap())?)
                } else {
                    return Err(SessionError::Serve(ServeError::internal_ctx(format!(
                        "unsupported stream header type={}",
                        stream_header.header_type
                    ))));
                }
            } else {
                return Err(SessionError::Serve(ServeError::not_found_ctx(format!(
                    "subscription track_alias={} not found",
                    track_alias
                ))));
            }
        };

        // Handle the stream based on the writer type
        match writer {
            //Writer::Fetch(fetch) => Self::recv_fetch(fetch, reader).await?,
            Writer::Subgroup(subgroup_writer) => {
                tracing::trace!("[SUBSCRIBER] recv_stream_inner: receiving subgroup data");
                Self::recv_subgroup(stream_header.header_type, subgroup_writer, reader, mlog)
                    .await?
            }
        };

        tracing::debug!(
            "[SUBSCRIBER] recv_stream_inner: completed processing stream for track_alias={}",
            track_alias
        );
        Ok(())
    }

    /// If new stream is a Subgroup stream, handle reception of subgroup objects and payloads.
    async fn recv_subgroup(
        stream_header_type: data::StreamHeaderType,
        mut subgroup_writer: serve::SubgroupWriter,
        mut reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        tracing::debug!(
            "[SUBSCRIBER] recv_subgroup: starting - group_id={}, subgroup_id={}, priority={}",
            subgroup_writer.info.group_id,
            subgroup_writer.info.subgroup_id,
            subgroup_writer.info.priority
        );

        let mut object_count = 0;
        let mut current_object_id = 0u64;
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
                        tracing::debug!(
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
                            tracing::info!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains IMMUTABLE EXTENSIONS (type 0xB) - will be forwarded",
                                object_count + 1
                            );
                            if let Some(immutable_ext) = object.extension_headers.get(0xB) {
                                tracing::debug!(
                                    "[SUBSCRIBER] recv_subgroup: immutable extension details: {:?}",
                                    immutable_ext
                                );
                            }
                        }

                        // Check for Prior Group ID Gap (type 0x3C = 60)
                        if object.extension_headers.has(0x3C) {
                            tracing::info!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains PRIOR GROUP ID GAP (type 0x3C)",
                                object_count + 1
                            );
                            if let Some(gap_ext) = object.extension_headers.get(0x3C) {
                                tracing::debug!(
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
                        tracing::debug!(
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

            // Calculate absolute object_id from delta
            current_object_id += object_id_delta;

            // Extract extension headers if present
            let extension_headers = decoded_object
                .as_ref()
                .map(|obj| obj.extension_headers.clone());

            // Log subgroup object parsed/received
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let event = if let Some(obj_ext) = decoded_object {
                        mlog::subgroup_object_ext_parsed(
                            time,
                            stream_id,
                            subgroup_writer.info.group_id,
                            subgroup_writer.info.subgroup_id,
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
                            subgroup_writer.info.group_id,
                            subgroup_writer.info.subgroup_id,
                            current_object_id,
                            &temp_obj,
                        )
                    };
                    let _ = mlog_guard.add_event(event);
                }
            }

            // Pass extension headers through to the serve layer
            // TODO SLG - object_id_delta and object status are still being ignored

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

        tracing::info!(
            "[SUBSCRIBER] recv_subgroup: completed subgroup (group_id={}, subgroup_id={}, {} objects received)",
            subgroup_writer.info.group_id,
            subgroup_writer.info.subgroup_id,
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
            tracing::debug!(
                "[SUBSCRIBER] recv_datagram: datagram contains extension headers: {:?}",
                ext_headers
            );

            // Check for known draft-14 extension types

            // Check for Immutable Extensions (type 0xB = 11)
            if ext_headers.has(0xB) {
                tracing::info!(
                    "[SUBSCRIBER] recv_datagram: datagram contains IMMUTABLE EXTENSIONS (type 0xB)"
                );
                if let Some(immutable_ext) = ext_headers.get(0xB) {
                    tracing::debug!(
                        "[SUBSCRIBER] recv_datagram: immutable extension details: {:?}",
                        immutable_ext
                    );
                }
            }

            // Check for Prior Group ID Gap (type 0x3C = 60)
            if ext_headers.has(0x3C) {
                tracing::info!(
                    "[SUBSCRIBER] recv_datagram: datagram contains PRIOR GROUP ID GAP (type 0x3C)"
                );
                if let Some(gap_ext) = ext_headers.get(0x3C) {
                    tracing::debug!(
                        "[SUBSCRIBER] recv_datagram: prior group id gap details: {:?}",
                        gap_ext
                    );
                }
            }
        }

        // Look up the subscribe id for this track alias
        if let Some(subscribe_id) = self
            .get_subscribe_id_by_alias(datagram.track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await
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
