use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::{atomic, Arc, Mutex},
};

use crate::{
    coding::{ReasonPhrase, TrackNamespace},
    message::{self, GroupOrder, Message},
    mlog,
    serve::{self, ServeError, TrackReader, TracksReader},
};

use crate::watch::Queue;

use super::{
    PublishNamespace, PublishNamespaceRecv, Published, PublishedRecv, Session, SessionError,
    SubscribeNamespaceReceived, SubscribeNamespaceReceivedRecv, Subscribed, SubscribedRecv,
    TrackStatusRequested,
};

#[derive(Clone)]
pub struct Publisher {
    webtransport: web_transport::Session,

    publish_namespaces: Arc<Mutex<HashMap<TrackNamespace, PublishNamespaceRecv>>>,

    filtered_namespaces: Arc<Mutex<HashSet<TrackNamespace>>>,

    subscribeds: Arc<Mutex<HashMap<u64, SubscribedRecv>>>,

    unknown_subscribed: Queue<Subscribed>,

    unknown_track_status_requested: Queue<TrackStatusRequested>,

    subscribe_namespaces_received: Arc<Mutex<HashMap<u64, SubscribeNamespaceReceivedRecv>>>,

    subscribe_namespace_received_queue: Queue<SubscribeNamespaceReceived>,

    publisheds: Arc<Mutex<HashMap<u64, PublishedRecv>>>,

    next_track_alias: Arc<atomic::AtomicU64>,

    outgoing: Queue<Message>,

    next_requestid: Arc<atomic::AtomicU64>,

    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Publisher {
    pub(crate) fn new(
        outgoing: Queue<Message>,
        webtransport: web_transport::Session,
        next_requestid: Arc<atomic::AtomicU64>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Self {
        Self {
            webtransport,
            publish_namespaces: Default::default(),
            filtered_namespaces: Default::default(),
            subscribeds: Default::default(),
            unknown_subscribed: Default::default(),
            unknown_track_status_requested: Default::default(),
            subscribe_namespaces_received: Default::default(),
            subscribe_namespace_received_queue: Default::default(),
            publisheds: Default::default(),
            next_track_alias: Arc::new(atomic::AtomicU64::new(1)),
            outgoing,
            next_requestid,
            mlog,
        }
    }

    pub async fn accept(
        session: web_transport::Session,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::accept(session, None).await?;
        Ok((session, publisher.unwrap()))
    }

    pub async fn connect(
        session: web_transport::Session,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::connect(session, None).await?;
        Ok((session, publisher))
    }

    pub async fn publish_namespace(
        &mut self,
        namespace: TrackNamespace,
    ) -> Result<PublishNamespace, SessionError> {
        if self
            .filtered_namespaces
            .lock()
            .unwrap()
            .contains(&namespace)
        {
            return Err(ServeError::Cancel.into());
        }

        let publish_ns = match self
            .publish_namespaces
            .lock()
            .unwrap()
            .entry(namespace.clone())
        {
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),

            hash_map::Entry::Vacant(entry) => {
                let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);

                let (send, recv) = PublishNamespace::new(self.clone(), request_id, namespace);
                entry.insert(recv);
                send
            }
        };

        Ok(publish_ns)
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

    pub async fn subscribed(&mut self) -> Option<Subscribed> {
        self.unknown_subscribed.pop().await
    }

    pub async fn track_status_requested(&mut self) -> Option<TrackStatusRequested> {
        self.unknown_track_status_requested.pop().await
    }

    pub async fn subscribe_namespace_received(&mut self) -> Option<SubscribeNamespaceReceived> {
        self.subscribe_namespace_received_queue.pop().await
    }

    pub async fn publish(&mut self, track: &TrackReader) -> Result<Published, SessionError> {
        let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);
        let track_alias = self
            .next_track_alias
            .fetch_add(1, atomic::Ordering::Relaxed);

        let largest_location = track.largest_location();
        let content_exists = largest_location.is_some();

        let msg = message::Publish {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            track_alias,
            group_order: GroupOrder::Ascending,
            content_exists,
            largest_location,
            forward: true,
            params: Default::default(),
        };

        let (send, recv) = Published::new(self.clone(), msg, self.mlog.clone());

        self.publisheds.lock().unwrap().insert(request_id, recv);

        Ok(send)
    }

    pub async fn publish_with_options(
        &mut self,
        track: serve::TrackReader,
        group_order: GroupOrder,
        forward: bool,
    ) -> Result<Published, SessionError> {
        let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);
        let track_alias = self
            .next_track_alias
            .fetch_add(1, atomic::Ordering::Relaxed);

        let largest_location = track.largest_location();
        let content_exists = largest_location.is_some();

        let msg = message::Publish {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            track_alias,
            group_order,
            content_exists,
            largest_location,
            forward,
            params: Default::default(),
        };

        let (send, recv) = Published::new(self.clone(), msg, self.mlog.clone());

        self.publisheds.lock().unwrap().insert(request_id, recv);

        Ok(send)
    }

    pub(crate) fn recv_message(&mut self, msg: message::Subscriber) -> Result<(), SessionError> {
        let res = match msg {
            message::Subscriber::Subscribe(msg) => self.recv_subscribe(msg),
            message::Subscriber::SubscribeUpdate(msg) => self.recv_subscribe_update(msg),
            message::Subscriber::Unsubscribe(msg) => self.recv_unsubscribe(msg),
            message::Subscriber::Fetch(_msg) => Err(SessionError::unimplemented("FETCH")),
            message::Subscriber::FetchCancel(_msg) => {
                Err(SessionError::unimplemented("FETCH_CANCEL"))
            }
            message::Subscriber::TrackStatus(msg) => self.recv_track_status(msg),
            message::Subscriber::SubscribeNamespace(msg) => self.recv_subscribe_namespace(msg),
            message::Subscriber::UnsubscribeNamespace(msg) => self.recv_unsubscribe_namespace(msg),
            message::Subscriber::PublishNamespaceCancel(msg) => {
                self.recv_publish_namespace_cancel(msg)
            }
            message::Subscriber::PublishNamespaceOk(msg) => self.recv_publish_namespace_ok(msg),
            message::Subscriber::PublishNamespaceError(msg) => {
                self.recv_publish_namespace_error(msg)
            }
            message::Subscriber::PublishOk(msg) => self.recv_publish_ok(msg),
            message::Subscriber::PublishError(msg) => self.recv_publish_error(msg),
        };

        if let Err(err) = res {
            tracing::warn!("failed to process message: {}", err);
        }

        Ok(())
    }

    fn recv_publish_namespace_ok(
        &mut self,
        msg: message::PublishNamespaceOk,
    ) -> Result<(), SessionError> {
        let mut publish_namespaces = self.publish_namespaces.lock().unwrap();
        let entry = publish_namespaces
            .iter_mut()
            .find(|(_k, v)| v.request_id == msg.id);

        if let Some(entry) = entry {
            entry.1.recv_ok()?;
        }

        Ok(())
    }

    fn recv_publish_namespace_error(
        &mut self,
        msg: message::PublishNamespaceError,
    ) -> Result<(), SessionError> {
        let mut publish_namespaces = self.publish_namespaces.lock().unwrap();

        let key_opt = publish_namespaces
            .iter()
            .find(|(_k, v)| v.request_id == msg.id)
            .map(|(k, _)| k.clone());

        if let Some(key) = key_opt {
            if let Some((_ns, v)) = publish_namespaces.remove_entry(&key) {
                v.recv_error(ServeError::Closed(msg.error_code))?;
            }
        }

        Ok(())
    }

    fn recv_publish_namespace_cancel(
        &mut self,
        msg: message::PublishNamespaceCancel,
    ) -> Result<(), SessionError> {
        if let Some(entry) = self
            .publish_namespaces
            .lock()
            .unwrap()
            .remove(&msg.track_namespace)
        {
            entry.recv_error(ServeError::Cancel)?;
        }

        Ok(())
    }

    fn recv_publish_ok(&mut self, msg: message::PublishOk) -> Result<(), SessionError> {
        if let Some(published) = self.publisheds.lock().unwrap().get_mut(&msg.id) {
            published.recv_ok(&msg)?;
        }

        Ok(())
    }

    fn recv_publish_error(&mut self, msg: message::PublishError) -> Result<(), SessionError> {
        if let Some(published) = self.publisheds.lock().unwrap().remove(&msg.id) {
            published.recv_error(ServeError::Closed(msg.error_code))?;
        }

        Ok(())
    }

    fn recv_subscribe(&mut self, msg: message::Subscribe) -> Result<(), SessionError> {
        let namespace = msg.track_namespace.clone();

        let subscribed = {
            let mut subscribeds = self.subscribeds.lock().unwrap();

            let entry = match subscribeds.entry(msg.id) {
                hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
                hash_map::Entry::Vacant(entry) => entry,
            };

            let (send, recv) = Subscribed::new(self.clone(), msg, self.mlog.clone());
            entry.insert(recv);

            send
        };

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
        // TODO: Implement updating subscriptions.
        Err(SessionError::unimplemented("SUBSCRIBE_UPDATE"))
    }

    fn recv_track_status(&mut self, msg: message::TrackStatus) -> Result<(), SessionError> {
        let track_status_requested = TrackStatusRequested::new(self.clone(), msg);

        if let Err(mut err) = self
            .unknown_track_status_requested
            .push(track_status_requested)
        {
            err.respond_error(0, "Internal error")?;
        }

        Ok(())
    }

    fn recv_unsubscribe(&mut self, msg: message::Unsubscribe) -> Result<(), SessionError> {
        if let Some(subscribed) = self.subscribeds.lock().unwrap().get_mut(&msg.id) {
            subscribed.recv_unsubscribe()?;
        }

        Ok(())
    }

    fn recv_subscribe_namespace(
        &mut self,
        msg: message::SubscribeNamespace,
    ) -> Result<(), SessionError> {
        let namespace_prefix = msg.track_namespace_prefix.clone();

        self.filtered_namespaces
            .lock()
            .unwrap()
            .remove(&namespace_prefix);

        // Check for namespace prefix overlap (MUST per draft-14 9.28).
        // "if a publisher receives a SUBSCRIBE_NAMESPACE with a Track Namespace Prefix
        //  that is a prefix of, suffix of, or equal to an active SUBSCRIBE_NAMESPACE,
        //  it MUST respond with SUBSCRIBE_NAMESPACE_ERROR, with error code
        //  NAMESPACE_PREFIX_OVERLAP (0x5)."
        let has_overlap = {
            let entries = self.subscribe_namespaces_received.lock().unwrap();
            entries.iter().any(|(_id, recv)| {
                Self::namespaces_overlap(recv.namespace_prefix(), &namespace_prefix)
            })
        };

        if has_overlap {
            self.send_message(message::SubscribeNamespaceError {
                id: msg.id,
                error_code: 0x5u64,
                reason_phrase: ReasonPhrase(
                    "Namespace prefix overlaps with existing subscription".to_string(),
                ),
            });
            return Ok(());
        }

        let mut entries = self.subscribe_namespaces_received.lock().unwrap();

        let entry = match entries.entry(msg.id) {
            hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (send, recv) = SubscribeNamespaceReceived::new(self.clone(), msg.id, namespace_prefix);

        if let Err(send) = self.subscribe_namespace_received_queue.push(send) {
            send.reject(0x0, "Internal error")?;
            return Ok(());
        }

        entry.insert(recv);

        Ok(())
    }

    /// True if either prefix is a prefix of the other (or equal). Draft-14 §9.28.
    fn namespaces_overlap(a: &TrackNamespace, b: &TrackNamespace) -> bool {
        let min_len = a.fields.len().min(b.fields.len());
        a.fields[..min_len] == b.fields[..min_len]
    }

    fn recv_unsubscribe_namespace(
        &mut self,
        msg: message::UnsubscribeNamespace,
    ) -> Result<(), SessionError> {
        self.filtered_namespaces
            .lock()
            .unwrap()
            .insert(msg.track_namespace_prefix.clone());

        let mut entries = self.subscribe_namespaces_received.lock().unwrap();
        let to_remove: Vec<u64> = entries
            .iter_mut()
            .filter_map(|(id, recv)| {
                if recv.namespace_prefix() == &msg.track_namespace_prefix {
                    recv.recv_unsubscribe().ok();
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        for id in to_remove {
            entries.remove(&id);
        }

        Ok(())
    }

    fn act_on_message_to_send<T: Into<message::Publisher>>(
        &mut self,
        msg: T,
    ) -> message::Publisher {
        let msg = msg.into();
        match &msg {
            message::Publisher::PublishDone(m) => {
                self.drop_subscribe(m.id);
                self.drop_published(m.id);
            }
            message::Publisher::SubscribeError(m) => self.drop_subscribe(m.id),
            message::Publisher::PublishNamespaceDone(m) => {
                self.drop_publish_namespace(&m.track_namespace);
            }
            _ => {}
        }
        msg
    }

    /// Send a message without waiting for it to be sent.
    pub(super) fn send_message<T: Into<message::Publisher> + Into<Message>>(&mut self, msg: T) {
        let msg = self.act_on_message_to_send(msg);
        self.outgoing.push(msg.into()).ok();
    }

    /// Send a message and wait until it is sent (or at least popped off the outgoing control message queue)
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
        self.subscribeds.lock().unwrap().remove(&id);
    }

    fn drop_publish_namespace(&mut self, namespace: &TrackNamespace) {
        self.publish_namespaces.lock().unwrap().remove(namespace);
    }

    fn drop_published(&mut self, id: u64) {
        self.publisheds.lock().unwrap().remove(&id);
    }

    pub(super) async fn open_uni(&mut self) -> Result<web_transport::SendStream, SessionError> {
        Ok(self.webtransport.open_uni().await?)
    }

    pub(super) async fn send_datagram(&mut self, data: bytes::Bytes) -> Result<(), SessionError> {
        Ok(self.webtransport.send_datagram(data).await?)
    }
}
