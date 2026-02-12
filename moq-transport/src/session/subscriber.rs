use std::{
    collections::{hash_map, HashMap},
    io,
    sync::{atomic, Arc, Mutex},
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
    PublishNamespaceReceived, PublishNamespaceReceivedRecv, PublishReceived, PublishReceivedRecv,
    Reader, Session, SessionError, Subscribe, SubscribeNs, SubscribeNsRecv, SubscribeRecv,
};

// Default timeout for waiting for subscribe aliases to become available via SUBSCRIBE_OK (1 second)
const DEFAULT_ALIAS_WAIT_TIME_MS: u64 = 1000;

#[derive(Clone)]
pub struct Subscriber {
    publish_namespaces_received: Arc<Mutex<HashMap<TrackNamespace, PublishNamespaceReceivedRecv>>>,

    publish_namespace_received_queue: Queue<PublishNamespaceReceived>,

    subscribe_namespaces: Arc<Mutex<HashMap<u64, SubscribeNsRecv>>>,

    subscribes: Arc<Mutex<HashMap<u64, SubscribeRecv>>>,

    subscribe_alias_map: Arc<Mutex<HashMap<u64, u64>>>,

    subscribe_alias_notify: Arc<Notify>,

    publishes_received: Arc<Mutex<HashMap<u64, PublishReceivedRecv>>>,

    publish_received_queue: Queue<PublishReceived>,

    publish_alias_map: Arc<Mutex<HashMap<u64, u64>>>,

    publish_alias_notify: Arc<Notify>,

    outgoing: Queue<Message>,

    next_requestid: Arc<atomic::AtomicU64>,

    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Subscriber {
    pub(super) fn new(
        outgoing: Queue<Message>,
        next_requestid: Arc<atomic::AtomicU64>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Self {
        Self {
            publish_namespaces_received: Default::default(),
            publish_namespace_received_queue: Default::default(),
            subscribe_namespaces: Default::default(),
            subscribes: Default::default(),
            subscribe_alias_map: Default::default(),
            subscribe_alias_notify: Arc::new(Notify::new()),
            publishes_received: Default::default(),
            publish_received_queue: Default::default(),
            publish_alias_map: Default::default(),
            publish_alias_notify: Arc::new(Notify::new()),
            outgoing,
            next_requestid,
            mlog,
        }
    }

    /// Create an inbound/server QUIC connection, by accepting a bi-directional QUIC stream for control messages.
    pub async fn accept(session: web_transport::Session) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) = Session::accept(session, None).await?;
        Ok((session, subscriber.unwrap()))
    }

    /// Create an outbound/client QUIC connection, by opening a bi-directional QUIC stream for control messages.
    pub async fn connect(session: web_transport::Session) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) = Session::connect(session, None).await?;
        Ok((session, subscriber))
    }

    pub async fn publish_ns_recvd(&mut self) -> Option<PublishNamespaceReceived> {
        self.publish_namespace_received_queue.pop().await
    }

    pub async fn publish_received(&mut self) -> Option<PublishReceived> {
        self.publish_received_queue.pop().await
    }

    /// Get the current next request id to use and increment the value for by 2 for the next request
    fn get_next_request_id(&self) -> u64 {
        self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed)
    }

    pub fn track_status(&mut self, track_namespace: &TrackNamespace, track_name: &str) {
        self.send_message(message::TrackStatus {
            id: self.get_next_request_id(),
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
        // TODO make async and wait for response?
    }

    /// Subscribe to a track by creating a new subscribe request to the publisher.  Block until subscription is closed.
    pub async fn subscribe(&mut self, track: serve::TrackWriter) -> Result<(), ServeError> {
        let request_id = self.get_next_request_id();
        let (send, recv) = Subscribe::new(self.clone(), request_id, track);
        self.subscribes.lock().unwrap().insert(request_id, recv);

        send.closed().await
    }

    pub fn subscribe_ns(
        &mut self,
        namespace_prefix: TrackNamespace,
    ) -> Result<SubscribeNs, ServeError> {
        let request_id = self.get_next_request_id();

        let mut subscribe_namespaces = self.subscribe_namespaces.lock().unwrap();
        let entry = match subscribe_namespaces.entry(request_id) {
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (send, recv) = SubscribeNs::new(self.clone(), request_id, namespace_prefix);
        entry.insert(recv);

        Ok(send)
    }

    /// Send a message to the publisher via the control stream.
    pub(super) fn send_message<M: Into<message::Subscriber>>(&mut self, msg: M) {
        let msg = msg.into();

        // Remove our entry on terminal state.
        if let message::Subscriber::PublishNamespaceCancel(msg) = &msg {
            self.drop_publish_namespace(&msg.track_namespace)
        }

        // TODO report dropped messages?
        let _ = self.outgoing.push(msg.into());
    }

    pub(super) fn recv_message(&mut self, msg: message::Publisher) -> Result<(), SessionError> {
        let res = match &msg {
            message::Publisher::PublishNamespace(msg) => self.recv_publish_namespace(msg),
            message::Publisher::PublishNamespaceDone(msg) => self.recv_publish_ns_done(msg),
            message::Publisher::Publish(msg) => self.recv_publish(msg),
            message::Publisher::PublishDone(msg) => self.recv_publish_done(msg),
            message::Publisher::SubscribeOk(msg) => self.recv_subscribe_ok(msg),
            message::Publisher::RequestError(msg) => self.recv_request_error(msg),
            message::Publisher::TrackStatusOk(msg) => self.recv_track_status_ok(msg),
            message::Publisher::FetchOk(_msg) => Err(SessionError::unimplemented("FETCH_OK")),
            message::Publisher::RequestOk(msg) => self.recv_request_ok(msg),
        };

        if let Err(SessionError::Serve(err)) = res {
            log::debug!("failed to process message: {:?} {}", msg, err);
            return Ok(());
        }

        res
    }

    fn recv_publish_namespace(
        &mut self,
        msg: &message::PublishNamespace,
    ) -> Result<(), SessionError> {
        let mut entries = self.publish_namespaces_received.lock().unwrap();

        let entry = match entries.entry(msg.track_namespace.clone()) {
            hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (publish_ns_received, recv) =
            PublishNamespaceReceived::new(self.clone(), msg.id, msg.track_namespace.clone());
        if let Err(publish_ns_received) = self
            .publish_namespace_received_queue
            .push(publish_ns_received)
        {
            publish_ns_received.close(ServeError::Cancel)?;
            return Ok(());
        }
        entry.insert(recv);

        Ok(())
    }

    fn recv_publish_ns_done(
        &mut self,
        msg: &message::PublishNamespaceDone,
    ) -> Result<(), SessionError> {
        if let Some(entry) = self
            .publish_namespaces_received
            .lock()
            .unwrap()
            .remove(&msg.track_namespace)
        {
            entry.recv_done()?;
        }

        Ok(())
    }

    fn recv_publish(&mut self, msg: &message::Publish) -> Result<(), SessionError> {
        let mut entries = self.publishes_received.lock().unwrap();

        let entry = match entries.entry(msg.id) {
            hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (publish_received, recv) = PublishReceived::new(self.clone(), msg);

        self.publish_alias_map
            .lock()
            .unwrap()
            .insert(msg.track_alias, msg.id);
        self.publish_alias_notify.notify_waiters();

        if let Err(publish_received) = self.publish_received_queue.push(publish_received) {
            publish_received.close(ServeError::Cancel)?;
            return Ok(());
        }
        entry.insert(recv);

        Ok(())
    }

    /// Handle the reception of a SubscribeOk message from the publisher.
    fn recv_subscribe_ok(&mut self, msg: &message::SubscribeOk) -> Result<(), SessionError> {
        if let Some(subscribe) = self.subscribes.lock().unwrap().get_mut(&msg.id) {
            // Map track alias to subscription id for quick lookup when receiving streams/datagrams
            self.subscribe_alias_map
                .lock()
                .unwrap()
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
        if let Some(subscribe) = self.subscribes.lock().unwrap().remove(&id) {
            // Remove from alias map if present
            if let Some(track_alias) = subscribe.track_alias() {
                self.subscribe_alias_map
                    .lock()
                    .unwrap()
                    .remove(&track_alias);
            };
            Some(subscribe)
        } else {
            None
        }
    }

    fn recv_request_ok(&mut self, msg: &message::RequestOk) -> Result<(), SessionError> {
        if let Some(subscribe_ns) = self.subscribe_namespaces.lock().unwrap().get_mut(&msg.id) {
            subscribe_ns.recv_ok()?;
            return Ok(());
        }

        log::warn!(
            "[SUBSCRIBER] recv_request_ok: request id {} not found",
            msg.id
        );
        Ok(())
    }

    fn recv_request_error(&mut self, msg: &message::RequestError) -> Result<(), SessionError> {
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.error_code))?;
            return Ok(());
        }

        if let Some(subscribe_ns) = self.subscribe_namespaces.lock().unwrap().remove(&msg.id) {
            subscribe_ns.recv_error(ServeError::Closed(msg.error_code))?;
            return Ok(());
        }

        log::warn!(
            "[SUBSCRIBER] recv_request_error: request id {} not found",
            msg.id
        );
        Ok(())
    }

    fn recv_publish_done(&mut self, msg: &message::PublishDone) -> Result<(), SessionError> {
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.status_code))?;
            return Ok(());
        }

        if let Some(mut publish_recv) = self.remove_publish_received(msg.id) {
            publish_recv.recv_done()?;
        }

        Ok(())
    }

    /// Handle the reception of a TrackStatusOk message from the publisher.
    fn recv_track_status_ok(&mut self, _msg: &message::TrackStatusOk) -> Result<(), SessionError> {
        // TODO: Expose this somehow?
        // TODO: Also add a way to send a Track Status Request in the first place

        Ok(())
    }

    fn drop_publish_namespace(&mut self, namespace: &TrackNamespace) {
        self.publish_namespaces_received
            .lock()
            .unwrap()
            .remove(namespace);
    }

    fn remove_publish_received(&mut self, id: u64) -> Option<PublishReceivedRecv> {
        if let Some(publish_recv) = self.publishes_received.lock().unwrap().remove(&id) {
            if let Some(track_alias) = publish_recv.track_alias() {
                self.publish_alias_map.lock().unwrap().remove(&track_alias);
            }
            Some(publish_recv)
        } else {
            None
        }
    }

    async fn get_subscribe_id_by_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Option<u64> {
        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => {
                return self
                    .subscribe_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned();
            }
        };

        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                let notified = self.subscribe_alias_notify.notified();

                if let Some(id) = self
                    .subscribe_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned()
                {
                    return id;
                }

                notified.await;
            }
        })
        .await
        .ok()
    }

    async fn get_publish_id_by_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Option<u64> {
        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => {
                return self
                    .publish_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned();
            }
        };

        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                let notified = self.publish_alias_notify.notified();

                if let Some(id) = self
                    .publish_alias_map
                    .lock()
                    .unwrap()
                    .get(&track_alias)
                    .cloned()
                {
                    return id;
                }

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
        log::trace!("[SUBSCRIBER] recv_stream: new stream received, decoding header");
        let mut reader = Reader::new(stream);

        // Decode the stream header
        let stream_header: data::StreamHeader = reader.decode().await?;
        log::debug!(
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
        log::trace!(
            "[SUBSCRIBER] recv_stream: stream for subscription track_alias={}",
            track_alias
        );

        let mlog = self.mlog.clone();
        let res = self.recv_stream_inner(reader, stream_header, mlog).await;
        if let Err(SessionError::Serve(err)) = &res {
            log::warn!(
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

    async fn recv_stream_inner(
        &mut self,
        reader: Reader,
        stream_header: data::StreamHeader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let track_alias = stream_header.subgroup_header.as_ref().unwrap().track_alias;
        log::trace!(
            "[SUBSCRIBER] recv_stream_inner: processing stream for track_alias={}",
            track_alias
        );

        enum Writer {
            Subgroup(serve::SubgroupWriter),
        }

        let writer = {
            if let Some(subscribe_id) = self
                .get_subscribe_id_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
                .await
            {
                let mut subscribes = self.subscribes.lock().unwrap();
                let subscribe = subscribes.get_mut(&subscribe_id).ok_or_else(|| {
                    ServeError::not_found_ctx(format!(
                        "subscribe_id={} not found for track_alias={}",
                        subscribe_id, track_alias
                    ))
                })?;

                if stream_header.header_type.is_subgroup() {
                    log::trace!(
                        "[SUBSCRIBER] recv_stream_inner: creating subgroup writer from subscribe"
                    );
                    Writer::Subgroup(
                        subscribe.subgroup(stream_header.subgroup_header.clone().unwrap())?,
                    )
                } else {
                    return Err(SessionError::Serve(ServeError::internal_ctx(format!(
                        "unsupported stream header type={}",
                        stream_header.header_type
                    ))));
                }
            } else if let Some(publish_id) = self
                .get_publish_id_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
                .await
            {
                let mut publishes = self.publishes_received.lock().unwrap();
                let publish_recv = publishes.get_mut(&publish_id).ok_or_else(|| {
                    ServeError::not_found_ctx(format!(
                        "publish_id={} not found for track_alias={}",
                        publish_id, track_alias
                    ))
                })?;

                if stream_header.header_type.is_subgroup() {
                    log::trace!(
                        "[SUBSCRIBER] recv_stream_inner: creating subgroup writer from publish"
                    );
                    Writer::Subgroup(
                        publish_recv.subgroup(stream_header.subgroup_header.clone().unwrap())?,
                    )
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

        match writer {
            Writer::Subgroup(subgroup_writer) => {
                log::trace!("[SUBSCRIBER] recv_stream_inner: receiving subgroup data");
                Self::recv_subgroup(stream_header.header_type, subgroup_writer, reader, mlog)
                    .await?
            }
        };

        log::debug!(
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
        log::debug!(
            "[SUBSCRIBER] recv_subgroup: starting - group_id={}, subgroup_id={}, priority={}",
            subgroup_writer.info.group_id,
            subgroup_writer.info.subgroup_id,
            subgroup_writer.info.priority
        );

        let mut object_count = 0;
        let mut current_object_id = 0u64;
        while !reader.done().await? {
            log::trace!(
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
                        log::debug!(
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
                            log::info!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains IMMUTABLE EXTENSIONS (type 0xB) - will be forwarded",
                                object_count + 1
                            );
                            if let Some(immutable_ext) = object.extension_headers.get(0xB) {
                                log::debug!(
                                    "[SUBSCRIBER] recv_subgroup: immutable extension details: {:?}",
                                    immutable_ext
                                );
                            }
                        }

                        // Check for Prior Group ID Gap (type 0x3C = 60)
                        if object.extension_headers.has(0x3C) {
                            log::info!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains PRIOR GROUP ID GAP (type 0x3C)",
                                object_count + 1
                            );
                            if let Some(gap_ext) = object.extension_headers.get(0x3C) {
                                log::debug!(
                                    "[SUBSCRIBER] recv_subgroup: prior group id gap details: {:?}",
                                    gap_ext
                                );
                            }
                        }

                        // Check for Prior Object ID Gap (type 0x3E = 62)
                        if object.extension_headers.has(0x3E) {
                            log::info!(
                                "[SUBSCRIBER] recv_subgroup: object #{} contains PRIOR OBJECT ID GAP (type 0x3E)",
                                object_count + 1
                            );
                            if let Some(gap_ext) = object.extension_headers.get(0x3E) {
                                log::debug!(
                                    "[SUBSCRIBER] recv_subgroup: prior object id gap details: {:?}",
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
                        log::debug!(
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
            log::trace!(
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
                        log::error!(
                            "[SUBSCRIBER] recv_subgroup: ERROR - stream ended with {} bytes remaining for object #{}",
                            remaining_bytes,
                            object_count + 1
                        );
                        SessionError::WrongSize
                    })?;
                log::trace!(
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

            log::trace!(
                "[SUBSCRIBER] recv_subgroup: completed object #{} ({} chunks)",
                object_count + 1,
                chunks_read
            );
            object_count += 1;
        }

        log::info!(
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
            log::debug!(
                "[SUBSCRIBER] recv_datagram: datagram contains extension headers: {:?}",
                ext_headers
            );

            // Check for known draft-14 extension types

            // Check for Immutable Extensions (type 0xB = 11)
            if ext_headers.has(0xB) {
                log::info!(
                    "[SUBSCRIBER] recv_datagram: datagram contains IMMUTABLE EXTENSIONS (type 0xB)"
                );
                if let Some(immutable_ext) = ext_headers.get(0xB) {
                    log::debug!(
                        "[SUBSCRIBER] recv_datagram: immutable extension details: {:?}",
                        immutable_ext
                    );
                }
            }

            // Check for Prior Group ID Gap (type 0x3C = 60)
            if ext_headers.has(0x3C) {
                log::info!(
                    "[SUBSCRIBER] recv_datagram: datagram contains PRIOR GROUP ID GAP (type 0x3C)"
                );
                if let Some(gap_ext) = ext_headers.get(0x3C) {
                    log::debug!(
                        "[SUBSCRIBER] recv_datagram: prior group id gap details: {:?}",
                        gap_ext
                    );
                }
            }

            // Check for Prior Object ID Gap (type 0x3E = 62)
            if ext_headers.has(0x3E) {
                log::info!(
                    "[SUBSCRIBER] recv_datagram: datagram contains PRIOR OBJECT ID GAP (type 0x3E)"
                );
                if let Some(gap_ext) = ext_headers.get(0x3E) {
                    log::debug!(
                        "[SUBSCRIBER] recv_datagram: prior object id gap details: {:?}",
                        gap_ext
                    );
                }
            }
        }

        if let Some(subscribe_id) = self
            .get_subscribe_id_by_alias(datagram.track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await
        {
            if let Some(subscribe) = self.subscribes.lock().unwrap().get_mut(&subscribe_id) {
                log::trace!(
                    "[SUBSCRIBER] recv_datagram: track_alias={}, group_id={}, object_id={}, publisher_priority={:?}, status={}, payload_length={}",
                    datagram.track_alias,
                    datagram.group_id,
                    datagram.object_id.unwrap_or(0),
                    datagram.publisher_priority,
                    datagram.status.as_ref().map_or("None".to_string(), |s| format!("{:?}", s)),
                    datagram.payload.as_ref().map_or(0, |p| p.len()));
                subscribe.datagram(datagram)?;
            }
        } else if let Some(publish_id) = self
            .get_publish_id_by_alias(datagram.track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await
        {
            if let Some(publish_recv) = self.publishes_received.lock().unwrap().get_mut(&publish_id)
            {
                log::trace!(
                    "[SUBSCRIBER] recv_datagram from publish: track_alias={}, group_id={}, object_id={}, publisher_priority={:?}, status={}, payload_length={}",
                    datagram.track_alias,
                    datagram.group_id,
                    datagram.object_id.unwrap_or(0),
                    datagram.publisher_priority,
                    datagram.status.as_ref().map_or("None".to_string(), |s| format!("{:?}", s)),
                    datagram.payload.as_ref().map_or(0, |p| p.len()));
                publish_recv.datagram(datagram)?;
            }
        } else {
            log::warn!(
                "[SUBSCRIBER] recv_datagram: discarded due to unknown track_alias: track_alias={}, group_id={}, object_id={}, publisher_priority={:?}, status={}, payload_length={}",
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
