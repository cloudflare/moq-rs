use std::ops;
use std::sync::{Arc, Mutex};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::coding::{Encode, Location, ReasonPhrase, TrackNamespace};
use crate::data::ExtensionHeaders;
use crate::message::ParameterType;
use crate::mlog;
use crate::serve::{ServeError, TrackReaderMode};
use crate::watch::State;
use crate::{data, message, serve};

use super::{Publisher, SessionError, Writer};

/// Callback for object observation and filtering during streaming.
/// Called for each object before it's forwarded.
///
/// Arguments:
/// - group_id: The group ID of the object
/// - object_id: The object ID within the group
/// - extension_headers: The extension headers on the object
///
/// Returns:
/// - `true` to forward the object
/// - `false` to skip/drop the object
///
/// Use cases:
/// - Update TopN tracker with audio level metrics from extension headers
/// - Filter objects based on whether the track is in top-N for the subscriber
pub type ObjectObserverFn = Box<dyn Fn(u64, u64, &ExtensionHeaders) -> bool + Send + Sync>;

#[derive(Debug, Clone)]
pub struct PublishInfo {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    pub track_alias: u64,
}

impl PublishInfo {
    pub fn new_from_publish(msg: &message::Publish) -> Self {
        Self {
            id: msg.id,
            track_namespace: msg.track_namespace.clone(),
            track_name: msg.track_name.clone(),
            track_alias: msg.track_alias,
        }
    }
}

#[derive(Debug)]
struct PublishedState {
    ok: bool,
    forward: bool,
    subscriber_priority: u8,
    group_order: message::GroupOrder,
    largest_location: Option<Location>,
    closed: Result<(), ServeError>,
}

impl PublishedState {
    fn update_largest_location(&mut self, group_id: u64, object_id: u64) -> Result<(), ServeError> {
        let new_location = Location::new(group_id, object_id);
        if let Some(current) = self.largest_location {
            if new_location > current {
                self.largest_location = Some(new_location);
            }
        } else {
            self.largest_location = Some(new_location);
        }
        Ok(())
    }
}

impl Default for PublishedState {
    fn default() -> Self {
        Self {
            ok: false,
            forward: true,
            subscriber_priority: 128,
            group_order: message::GroupOrder::Ascending,
            largest_location: None,
            closed: Ok(()),
        }
    }
}

#[must_use = "sends PUBLISH_DONE on drop"]
pub struct Published {
    publisher: Publisher,
    pub info: PublishInfo,
    state: State<PublishedState>,
    ok: bool,
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Published {
    pub(super) fn new(
        mut publisher: Publisher,
        msg: message::Publish,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> (Self, PublishedRecv) {
        let info = PublishInfo::new_from_publish(&msg);

        publisher.send_message(msg);

        let (send, recv) = State::default().split();

        let send = Self {
            publisher,
            info,
            state: send,
            ok: false,
            mlog,
        };

        let recv = PublishedRecv { state: recv };

        (send, recv)
    }

    pub async fn ok(&mut self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                if state.ok {
                    self.ok = true;
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

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }

    pub async fn serve(mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        let res = self.serve_inner(track, None).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }
        res
    }

    /// Serve with an observer callback that's called for each object.
    /// The observer receives object extension headers and can update external state (e.g., TopN tracker).
    /// All objects are forwarded regardless of observer return.
    pub async fn serve_with_observer(
        mut self,
        track: serve::TrackReader,
        observer: ObjectObserverFn,
    ) -> Result<(), SessionError> {
        let res = self.serve_inner(track, Some(Arc::new(observer))).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }
        res
    }

    /// Serve using a pre-acquired TrackReaderMode.
    /// Use this when you need to acquire the mode early (before network round trips)
    /// to avoid missing frames in late-join scenarios.
    pub async fn serve_mode(mut self, mode: TrackReaderMode) -> Result<(), SessionError> {
        let res = self.serve_mode_inner(mode).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }
        res
    }

    /// Serve immediately without waiting for PUBLISH_OK.
    /// Use this for relay scenarios where you want to start forwarding data right away.
    /// The subscriber will receive data as soon as they're ready.
    pub async fn serve_immediately(mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        let res = self.serve_immediately_inner(track).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }
        res
    }

    async fn serve_inner(
        &mut self,
        track: serve::TrackReader,
        observer: Option<Arc<ObjectObserverFn>>,
    ) -> Result<(), SessionError> {
        self.ok().await?;

        let forward = {
            let state = self.state.lock();
            state.forward
        };

        if !forward {
            self.closed().await?;
            return Ok(());
        }

        match track.mode().await? {
            TrackReaderMode::Stream(_stream) => panic!("deprecated"),
            TrackReaderMode::Subgroups(subgroups) => {
                self.serve_subgroups(subgroups, observer).await
            }
            TrackReaderMode::Datagrams(datagrams) => self.serve_datagrams(datagrams).await,
        }
    }

    async fn serve_mode_inner(&mut self, mode: TrackReaderMode) -> Result<(), SessionError> {
        self.ok().await?;

        let forward = {
            let state = self.state.lock();
            state.forward
        };

        if !forward {
            self.closed().await?;
            return Ok(());
        }

        match mode {
            TrackReaderMode::Stream(_stream) => panic!("deprecated"),
            TrackReaderMode::Subgroups(subgroups) => self.serve_subgroups(subgroups, None).await,
            TrackReaderMode::Datagrams(datagrams) => self.serve_datagrams(datagrams).await,
        }
    }

    async fn serve_immediately_inner(&mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        // Don't wait for PUBLISH_OK - start streaming immediately
        // This is useful for relay scenarios where we want minimal latency

        match track.mode().await? {
            TrackReaderMode::Stream(_stream) => panic!("deprecated"),
            TrackReaderMode::Subgroups(subgroups) => self.serve_subgroups(subgroups, None).await,
            TrackReaderMode::Datagrams(datagrams) => self.serve_datagrams(datagrams).await,
        }
    }

    async fn serve_subgroups(
        &mut self,
        mut subgroups: serve::SubgroupsReader,
        observer: Option<Arc<ObjectObserverFn>>,
    ) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();
        let mut done: Option<Result<(), ServeError>> = None;

        loop {
            tokio::select! {
                res = subgroups.next(), if done.is_none() => match res {
                    Ok(Some(subgroup)) => {
                        // Header type will be determined in serve_subgroup based on extension headers
                        let track_alias = self.info.track_alias;
                        let publisher = self.publisher.clone();
                        let state = self.state.clone();
                        let info = subgroup.info.clone();
                        let mlog = self.mlog.clone();
                        let observer = observer.clone();

                        tasks.push(async move {
                            if let Err(err) = Self::serve_subgroup(track_alias, subgroup, publisher, state, mlog, observer).await {
                                log::warn!("failed to serve subgroup: {:?}, error: {}", info, err);
                            }
                        });
                    },
                    Ok(None) => done = Some(Ok(())),
                    Err(err) => done = Some(Err(err)),
                },
                res = self.closed(), if done.is_none() => done = Some(res),
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(done.unwrap()?),
            }
        }
    }

    async fn serve_subgroup(
        track_alias: u64,
        mut subgroup_reader: serve::SubgroupReader,
        mut publisher: Publisher,
        state: State<PublishedState>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        observer: Option<Arc<ObjectObserverFn>>,
    ) -> Result<(), SessionError> {
        log::debug!(
            "[PUBLISHED] serve_subgroup: starting - track_alias={}, group_id={}, subgroup_id={:?}, priority={}",
            track_alias,
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            subgroup_reader.priority
        );

        // Read the first object to determine if we have extension headers
        let first_object = match subgroup_reader.next().await? {
            Some(obj) => obj,
            None => {
                log::debug!("[PUBLISHED] serve_subgroup: no objects in subgroup, skipping");
                return Ok(());
            }
        };

        // Call observer for first object if present (always forward first object to establish stream)
        let should_forward_first = if let Some(ref obs) = observer {
            obs(
                subgroup_reader.group_id,
                first_object.object_id,
                &first_object.extension_headers,
            )
        } else {
            true
        };

        // Use preserved header type if available, otherwise determine from extension headers
        let has_extension_headers = !first_object.extension_headers.is_empty();
        let header_type = subgroup_reader.info.header_type.unwrap_or_else(|| {
            // Fallback: determine header type based on extension headers
            if has_extension_headers {
                data::StreamHeaderType::SubgroupZeroIdExtEndOfGroup
            } else {
                data::StreamHeaderType::SubgroupZeroIdEndOfGroup
            }
        });

        // If we're not writing extension headers but the preserved header type has extensions,
        // convert to the non-Ext variant to avoid mismatch between header and object encoding
        let header_type = if !has_extension_headers && header_type.has_extension_headers() {
            log::debug!(
                "[PUBLISHED] serve_subgroup: converting header_type {:?} to non-Ext variant (objects have no extensions)",
                header_type
            );
            header_type.without_extensions()
        } else {
            header_type
        };

        // Set subgroup_id based on header type (ZeroId variants don't include it on wire)
        let subgroup_id = if header_type.has_subgroup_id() {
            Some(subgroup_reader.subgroup_id)
        } else {
            None
        };

        let header = data::SubgroupHeader {
            header_type,
            track_alias,
            group_id: subgroup_reader.group_id,
            subgroup_id,
            publisher_priority: Some(subgroup_reader.priority),
        };

        let mut send_stream = publisher.open_uni().await?;
        send_stream.set_priority(subgroup_reader.priority as i32);

        let mut writer = Writer::new(send_stream);

        log::debug!(
            "[PUBLISHED] serve_subgroup: sending header - track_alias={}, group_id={}, subgroup_id={:?}, priority={:?}, header_type={:?}, has_ext={}",
            header.track_alias,
            header.group_id,
            header.subgroup_id,
            header.publisher_priority,
            header.header_type,
            has_extension_headers
        );

        writer.encode(&header).await?;

        if let Some(ref mlog) = mlog {
            if let Ok(mut mlog_guard) = mlog.lock() {
                let time = mlog_guard.elapsed_ms();
                let stream_id = 0;
                let event = mlog::subgroup_header_created(time, stream_id, &header);
                let _ = mlog_guard.add_event(event);
            }
        }

        // Helper to write an object
        async fn write_object(
            writer: &mut Writer,
            object_reader: &mut serve::SubgroupObjectReader,
            has_extension_headers: bool,
            object_count: u64,
            subgroup_reader: &serve::SubgroupReader,
            state: &State<PublishedState>,
            mlog: &Option<Arc<Mutex<mlog::MlogWriter>>>,
        ) -> Result<(), SessionError> {
            if has_extension_headers {
                let subgroup_object = data::SubgroupObjectExt {
                    object_id_delta: 0,
                    extension_headers: object_reader.extension_headers.clone(),
                    payload_length: object_reader.size,
                    status: if object_reader.size == 0 {
                        Some(object_reader.status)
                    } else {
                        None
                    },
                };

                log::debug!(
                    "[PUBLISHED] serve_subgroup: sending object #{} (ext) - object_id={}, payload_length={}, status={:?}",
                    object_count + 1,
                    object_reader.object_id,
                    subgroup_object.payload_length,
                    subgroup_object.status
                );

                writer.encode(&subgroup_object).await?;

                if let Some(ref mlog) = mlog {
                    if let Ok(mut mlog_guard) = mlog.lock() {
                        let time = mlog_guard.elapsed_ms();
                        let stream_id = 0;
                        let event = mlog::subgroup_object_ext_created(
                            time,
                            stream_id,
                            subgroup_reader.group_id,
                            subgroup_reader.subgroup_id,
                            object_reader.object_id,
                            &subgroup_object,
                        );
                        let _ = mlog_guard.add_event(event);
                    }
                }
            } else {
                let subgroup_object = data::SubgroupObject {
                    object_id_delta: 0,
                    payload_length: object_reader.size,
                    status: if object_reader.size == 0 {
                        Some(object_reader.status)
                    } else {
                        None
                    },
                };

                log::debug!(
                    "[PUBLISHED] serve_subgroup: sending object #{} - object_id={}, payload_length={}, status={:?}",
                    object_count + 1,
                    object_reader.object_id,
                    subgroup_object.payload_length,
                    subgroup_object.status
                );

                writer.encode(&subgroup_object).await?;

                // No mlog for non-ext objects currently
            }

            state
                .lock_mut()
                .ok_or(ServeError::Done)?
                .update_largest_location(
                    subgroup_reader.group_id,
                    object_reader.object_id,
                )?;

            while let Some(chunk) = object_reader.read().await? {
                writer.write(&chunk).await?;
            }

            Ok(())
        }

        // Write the first object that we already read (if observer allows)
        let mut object_count = 0;
        let mut first_object = first_object;
        if should_forward_first {
            write_object(
                &mut writer,
                &mut first_object,
                has_extension_headers,
                object_count,
                &subgroup_reader,
                &state,
                &mlog,
            )
            .await?;
            object_count += 1;
        } else {
            // Consume the object data even if not forwarding
            while first_object.read().await?.is_some() {}
            log::debug!(
                "[PUBLISHED] serve_subgroup: skipped first object (group_id={}, object_id={}) - filtered by observer",
                subgroup_reader.group_id,
                first_object.object_id
            );
        }

        // Continue with remaining objects
        while let Some(mut subgroup_object_reader) = subgroup_reader.next().await? {
            // Call observer for each object if present - observer returns whether to forward
            let should_forward = if let Some(ref obs) = observer {
                obs(
                    subgroup_reader.group_id,
                    subgroup_object_reader.object_id,
                    &subgroup_object_reader.extension_headers,
                )
            } else {
                true
            };

            if should_forward {
                write_object(
                    &mut writer,
                    &mut subgroup_object_reader,
                    has_extension_headers,
                    object_count,
                    &subgroup_reader,
                    &state,
                    &mlog,
                )
                .await?;
                object_count += 1;
            } else {
                // Consume the object data even if not forwarding
                while subgroup_object_reader.read().await?.is_some() {}
                log::debug!(
                    "[PUBLISHED] serve_subgroup: skipped object (group_id={}, object_id={}) - filtered by observer",
                    subgroup_reader.group_id,
                    subgroup_object_reader.object_id
                );
            }
        }

        log::info!(
            "[PUBLISHED] serve_subgroup: completed subgroup (group_id={}, subgroup_id={:?}, {} objects sent, header_type={:?})",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            object_count,
            header_type
        );

        Ok(())
    }

    async fn serve_datagrams(
        &mut self,
        mut datagrams: serve::DatagramsReader,
    ) -> Result<(), SessionError> {
        log::debug!("[PUBLISHED] serve_datagrams: starting");

        let mut datagram_count = 0;
        while let Some(datagram) = datagrams.read().await? {
            let has_extension_headers = !datagram.extension_headers.is_empty();
            let datagram_type = if has_extension_headers {
                data::DatagramType::ObjectIdPayloadExt
            } else {
                data::DatagramType::ObjectIdPayload
            };

            let encoded_datagram = data::Datagram {
                datagram_type,
                track_alias: self.info.track_alias,
                group_id: datagram.group_id,
                object_id: Some(datagram.object_id),
                publisher_priority: Some(datagram.priority),
                extension_headers: if has_extension_headers {
                    Some(datagram.extension_headers.clone())
                } else {
                    None
                },
                status: None,
                payload: Some(datagram.payload),
            };

            let payload_len = encoded_datagram
                .payload
                .as_ref()
                .map(|p| p.len())
                .unwrap_or(0);
            let mut buffer = bytes::BytesMut::with_capacity(payload_len + 100);
            encoded_datagram.encode(&mut buffer)?;

            log::debug!(
                "[PUBLISHED] serve_datagrams: sending datagram #{} - track_alias={}, group_id={}, object_id={}, priority={:?}, payload_len={}",
                datagram_count + 1,
                encoded_datagram.track_alias,
                encoded_datagram.group_id,
                encoded_datagram.object_id.unwrap(),
                encoded_datagram.publisher_priority,
                payload_len
            );

            if let Some(ref mlog) = self.mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0;
                    let _ = mlog_guard.add_event(mlog::object_datagram_created(
                        time,
                        stream_id,
                        &encoded_datagram,
                    ));
                }
            }

            self.publisher.send_datagram(buffer.into()).await?;

            self.state
                .lock_mut()
                .ok_or(ServeError::Done)?
                .update_largest_location(
                    encoded_datagram.group_id,
                    encoded_datagram.object_id.unwrap(),
                )?;

            datagram_count += 1;
        }

        log::info!(
            "[PUBLISHED] serve_datagrams: completed ({} datagrams sent)",
            datagram_count
        );

        Ok(())
    }
}

impl ops::Deref for Published {
    type Target = PublishInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for Published {
    fn drop(&mut self) {
        let state = self.state.lock();
        let err = state
            .closed
            .as_ref()
            .err()
            .cloned()
            .unwrap_or(ServeError::Done);
        drop(state);

        self.publisher.send_message(message::PublishDone {
            id: self.info.id,
            status_code: err.code(),
            stream_count: 0, // TODO SLG
            reason: ReasonPhrase(err.to_string()),
        });
    }
}

pub(super) struct PublishedRecv {
    state: State<PublishedState>,
}

impl PublishedRecv {
    pub fn recv_ok(&mut self, msg: &message::PublishOk) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.ok {
            return Err(ServeError::Duplicate);
        }

        if let Some(mut state) = state.into_mut() {
            state.ok = true;

            // Extract subscription properties from parameters (draft-16)
            if let Some(v) = msg.params.get_intvalue(ParameterType::Forward.into()) {
                state.forward = v == 1;
            }
            if let Some(v) = msg.params.get_intvalue(ParameterType::SubscriberPriority.into()) {
                state.subscriber_priority = v as u8;
            }
            if let Some(v) = msg.params.get_intvalue(ParameterType::GroupOrder.into()) {
                state.group_order = match v {
                    0x0 => message::GroupOrder::Publisher,
                    0x1 => message::GroupOrder::Ascending,
                    0x2 => message::GroupOrder::Descending,
                    _ => message::GroupOrder::Ascending,
                };
            }
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
