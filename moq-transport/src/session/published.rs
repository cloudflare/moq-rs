use std::ops;
use std::sync::{Arc, Mutex};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::coding::{Encode, Location, ReasonPhrase, TrackNamespace};
use crate::mlog;
use crate::serve::{ServeError, TrackReaderMode};
use crate::watch::State;
use crate::{data, message, serve};

use super::{Publisher, SessionError, Writer};

#[derive(Debug, Clone)]
pub struct PublishInfo {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    pub track_alias: u64,
    pub group_order: message::GroupOrder,
    pub content_exists: bool,
    pub largest_location: Option<Location>,
    pub forward: bool,
}

impl PublishInfo {
    pub fn new_from_publish(msg: &message::Publish) -> Self {
        Self {
            id: msg.id,
            track_namespace: msg.track_namespace.clone(),
            track_name: msg.track_name.clone(),
            track_alias: msg.track_alias,
            group_order: msg.group_order,
            content_exists: msg.content_exists,
            largest_location: msg.largest_location,
            forward: msg.forward,
        }
    }
}

#[derive(Debug)]
struct PublishedState {
    ok: bool,
    forward: bool,
    subscriber_priority: u8,
    group_order: message::GroupOrder,
    filter_type: message::FilterType,
    start_location: Option<Location>,
    end_group_id: Option<u64>,
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
            forward: false,
            subscriber_priority: 127,
            group_order: message::GroupOrder::Ascending,
            filter_type: message::FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
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
        
        let recv = PublishedRecv {
            state: recv,
        };
        
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
        let res = self.serve_inner(track).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }
        res
    }

    async fn serve_inner(&mut self, track: serve::TrackReader) -> Result<(), SessionError> {
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
            TrackReaderMode::Subgroups(subgroups) => self.serve_subgroups(subgroups).await,
            TrackReaderMode::Datagrams(datagrams) => self.serve_datagrams(datagrams).await,
        }
    }

    async fn serve_subgroups(
        &mut self,
        mut subgroups: serve::SubgroupsReader,
    ) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();
        let mut done: Option<Result<(), ServeError>> = None;
        
        loop {
            tokio::select! {
                res = subgroups.next(), if done.is_none() => match res {
                    Ok(Some(subgroup)) => {
                        let header = data::SubgroupHeader {
                            header_type: data::StreamHeaderType::SubgroupIdExt,
                            track_alias: self.info.track_alias,
                            group_id: subgroup.group_id,
                            subgroup_id: Some(subgroup.subgroup_id),
                            publisher_priority: subgroup.priority,
                        };
                        
                        let publisher = self.publisher.clone();
                        let state = self.state.clone();
                        let info = subgroup.info.clone();
                        let mlog = self.mlog.clone();
                        
                        tasks.push(async move {
                            if let Err(err) = Self::serve_subgroup(header, subgroup, publisher, state, mlog).await {
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
        header: data::SubgroupHeader,
        mut subgroup_reader: serve::SubgroupReader,
        mut publisher: Publisher,
        state: State<PublishedState>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        log::debug!(
            "[PUBLISHED] serve_subgroup: starting - group_id={}, subgroup_id={:?}, priority={}",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            subgroup_reader.priority
        );
        
        let mut send_stream = publisher.open_uni().await?;
        send_stream.set_priority(subgroup_reader.priority as i32);
        
        let mut writer = Writer::new(send_stream);
        
        log::debug!(
            "[PUBLISHED] serve_subgroup: sending header - track_alias={}, group_id={}, subgroup_id={:?}, priority={}, header_type={:?}",
            header.track_alias,
            header.group_id,
            header.subgroup_id,
            header.publisher_priority,
            header.header_type
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
        
        let mut object_count = 0;
        while let Some(mut subgroup_object_reader) = subgroup_reader.next().await? {
            let subgroup_object = data::SubgroupObjectExt {
                object_id_delta: 0,
                extension_headers: subgroup_object_reader.extension_headers.clone(),
                payload_length: subgroup_object_reader.size,
                status: if subgroup_object_reader.size == 0 {
                    Some(subgroup_object_reader.status)
                } else {
                    None
                },
            };
            
            log::debug!(
                "[PUBLISHED] serve_subgroup: sending object #{} - object_id={}, object_id_delta={}, payload_length={}, status={:?}",
                object_count + 1,
                subgroup_object_reader.object_id,
                subgroup_object.object_id_delta,
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
                        subgroup_object_reader.object_id,
                        &subgroup_object,
                    );
                    let _ = mlog_guard.add_event(event);
                }
            }
            
            state
                .lock_mut()
                .ok_or(ServeError::Done)?
                .update_largest_location(
                    subgroup_reader.group_id,
                    subgroup_object_reader.object_id,
                )?;
            
            while let Some(chunk) = subgroup_object_reader.read().await? {
                writer.write(&chunk).await?;
            }
            
            object_count += 1;
        }
        
        log::info!(
            "[PUBLISHED] serve_subgroup: completed subgroup (group_id={}, subgroup_id={:?}, {} objects sent)",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            object_count
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
                publisher_priority: datagram.priority,
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
                "[PUBLISHED] serve_datagrams: sending datagram #{} - track_alias={}, group_id={}, object_id={}, priority={}, payload_len={}",
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
            state.forward = msg.forward;
            state.subscriber_priority = msg.subscriber_priority;
            state.group_order = msg.group_order;
            state.filter_type = msg.filter_type;
            state.start_location = msg.start_location;
            state.end_group_id = msg.end_group_id;
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
