// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Context;
use bytes::Bytes;
use moq_transport::{
    coding::{Location, VarInt},
    data::{ExtensionHeaders, FetchRecord, FetchRecordObject},
    message::{GroupOrder, RequestErrorCode},
    serve::{FullTrackName, ServeError},
    session::{FetchOkInfo, FetchRejection, FetchRequested, Publisher},
};
use tokio::task::JoinSet;

#[derive(Clone)]
pub struct FetchHistory {
    inner: Arc<Mutex<History>>,
}

#[derive(Default)]
struct History {
    capacity: usize,
    tracks: HashMap<FullTrackName, TrackHistory>,
}

#[derive(Default)]
struct TrackHistory {
    groups: VecDeque<ArchivedGroup>,
    evicted_watermark: Option<u64>,
}

struct ArchivedGroup {
    group_id: u64,
    complete: bool,
    objects: Vec<ArchivedObject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArchivedObject {
    pub(crate) group_id: u64,
    pub(crate) subgroup_id: u64,
    pub(crate) object_id: u64,
    pub(crate) publisher_priority: u8,
    pub(crate) extension_headers: ExtensionHeaders,
    pub(crate) payload: Bytes,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct HistorySnapshot {
    pub(crate) objects: Vec<ArchivedObject>,
    pub(crate) end_location: Location,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchError {
    Disabled,
    DoesNotExist,
    NotRetained,
    InvalidRange,
}

impl FetchError {
    fn rejection(self) -> Result<FetchRejection, ServeError> {
        let (code, reason) = match self {
            Self::Disabled => (RequestErrorCode::NotSupported, "FETCH is disabled"),
            Self::DoesNotExist => (RequestErrorCode::DoesNotExist, "track is not retained"),
            Self::NotRetained => (
                RequestErrorCode::DoesNotExist,
                "requested range is not retained",
            ),
            Self::InvalidRange => (
                RequestErrorCode::InvalidRange,
                "requested range is unavailable",
            ),
        };
        FetchRejection::new(code, 0, reason)
    }
}

impl FetchHistory {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(History {
                capacity: capacity.get(),
                tracks: HashMap::new(),
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, History> {
        match self.inner.lock() {
            Ok(history) => history,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(crate) fn add_track(&self, track: FullTrackName) {
        self.lock().tracks.entry(track).or_default();
    }

    #[cfg(test)]
    fn begin_group(&self, track: &FullTrackName, group_id: u64) {
        let mut history = self.lock();
        Self::begin_group_locked(&mut history, track, group_id);
    }

    fn begin_group_locked(history: &mut History, track: &FullTrackName, group_id: u64) {
        let capacity = history.capacity;
        let Some(track) = history.tracks.get_mut(track) else {
            return;
        };
        if track
            .groups
            .back()
            .is_some_and(|group| group.group_id == group_id)
        {
            return;
        }

        while track.groups.len() >= capacity {
            if let Some(evicted) = track.groups.pop_front() {
                track.evicted_watermark = Some(
                    track
                        .evicted_watermark
                        .map_or(evicted.group_id, |watermark| {
                            watermark.max(evicted.group_id)
                        }),
                );
            }
        }
        track.groups.push_back(ArchivedGroup {
            group_id,
            complete: false,
            objects: Vec::new(),
        });
    }

    pub(crate) fn complete_group(&self, track: &FullTrackName, group_id: u64) {
        if let Some(group) = self
            .lock()
            .tracks
            .get_mut(track)
            .and_then(|track| track.groups.back_mut())
            .filter(|group| group.group_id == group_id)
        {
            group.complete = true;
        }
    }

    pub(crate) fn write_object(
        &self,
        track: &FullTrackName,
        object: ArchivedObject,
        write_live: impl FnOnce() -> Result<(), ServeError>,
    ) -> Result<(), ServeError> {
        let mut history = self.lock();
        write_live()?;
        Self::begin_group_locked(&mut history, track, object.group_id);
        let group = history
            .tracks
            .get_mut(track)
            .and_then(|track| track.groups.back_mut())
            .filter(|group| group.group_id == object.group_id)
            .ok_or_else(|| ServeError::Internal("FETCH history group is missing".to_string()))?;
        group.objects.push(object);
        Ok(())
    }

    #[cfg(test)]
    fn insert_object(&self, track: &FullTrackName, object: ArchivedObject) {
        let mut history = self.lock();
        let group = history
            .tracks
            .get_mut(track)
            .and_then(|track| track.groups.back_mut())
            .filter(|group| group.group_id == object.group_id)
            .expect("test group must exist");
        group.objects.push(object);
    }

    pub(crate) fn snapshot(
        &self,
        track: &FullTrackName,
        start: Location,
        end: Location,
        order: GroupOrder,
    ) -> Result<HistorySnapshot, FetchError> {
        let history = self.lock();
        let track = history.tracks.get(track).ok_or(FetchError::DoesNotExist)?;
        if start == end && end.object_id != 0 {
            return Ok(HistorySnapshot {
                objects: Vec::new(),
                end_location: end,
            });
        }
        if track
            .evicted_watermark
            .is_some_and(|watermark| start.group_id <= watermark)
        {
            return Err(FetchError::NotRetained);
        }
        let largest = track
            .groups
            .iter()
            .flat_map(|group| &group.objects)
            .map(|object| Location::new(object.group_id, object.object_id))
            .max()
            .ok_or(FetchError::InvalidRange)?;
        if start > largest {
            return Err(FetchError::InvalidRange);
        }

        let requested_end = inclusive_end(end);
        let covered_through = requested_end.min(largest);
        let end_location = if end.object_id == 0
            && track
                .groups
                .iter()
                .find(|group| group.group_id == end.group_id)
                .is_some_and(|group| group.complete)
        {
            end
        } else {
            exclusive_end(covered_through)
        };
        let in_range = |object: &&ArchivedObject| {
            let location = Location::new(object.group_id, object.object_id);
            location >= start && location <= requested_end && location <= covered_through
        };
        let mut objects = Vec::new();
        match order {
            GroupOrder::Descending => {
                for group in track.groups.iter().rev() {
                    objects.extend(group.objects.iter().filter(in_range).cloned());
                }
            }
            GroupOrder::Publisher | GroupOrder::Ascending => {
                for group in &track.groups {
                    objects.extend(group.objects.iter().filter(in_range).cloned());
                }
            }
        }

        Ok(HistorySnapshot {
            objects,
            end_location,
        })
    }

    #[cfg(test)]
    fn evicted_watermark(&self, track: &FullTrackName) -> Option<u64> {
        self.lock()
            .tracks
            .get(track)
            .and_then(|track| track.evicted_watermark)
    }
}

fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, VarInt::MAX.into_inner())
    } else {
        Location::new(end.group_id, end.object_id - 1)
    }
}

fn exclusive_end(end: Location) -> Location {
    if end.object_id == VarInt::MAX.into_inner() {
        Location::new(end.group_id, 0)
    } else {
        Location::new(end.group_id, end.object_id + 1)
    }
}

pub async fn serve_fetches(
    mut publisher: Publisher,
    history: Option<FetchHistory>,
) -> anyhow::Result<()> {
    let mut handlers = JoinSet::new();
    let mut accepting = true;

    loop {
        tokio::select! {
            request = publisher.fetch_requested(), if accepting => match request {
                Some(request) => {
                    let history = history.clone();
                    handlers.spawn(async move {
                        if let Err(error) = serve_fetch(request, history).await {
                            tracing::debug!(error = %error, "failed serving FETCH");
                        }
                    });
                }
                None => accepting = false,
            },
            Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                result.context("FETCH handler panicked")?;
            },
            else => return Ok(()),
        }
    }
}

async fn serve_fetch(request: FetchRequested, history: Option<FetchHistory>) -> anyhow::Result<()> {
    let Some(history) = history else {
        request.reject_with(FetchError::Disabled.rejection()?)?;
        return Ok(());
    };
    let order = request
        .request
        .params
        .group_order()
        .context("invalid FETCH group order")?
        .unwrap_or(GroupOrder::Ascending);
    let Some(range) = request.resolve().await? else {
        return Ok(());
    };
    let track = FullTrackName {
        namespace: range.track_namespace,
        name: range.track_name,
    };
    let snapshot = match history.snapshot(&track, range.start_location, range.end_location, order) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            request.reject_with(error.rejection()?)?;
            return Ok(());
        }
    };
    let mut writer = request
        .prepare_response(FetchOkInfo {
            end_of_track: false,
            end_location: snapshot.end_location,
            params: Default::default(),
            track_extensions: Default::default(),
        })
        .await?;

    for object in snapshot.objects {
        let payload = object.payload;
        writer
            .write_record(&FetchRecord::Object(FetchRecordObject {
                group_id: object.group_id,
                subgroup_id: Some(object.subgroup_id),
                object_id: object.object_id,
                publisher_priority: object.publisher_priority,
                extension_headers: object.extension_headers,
                payload_length: payload.len() as u64,
            }))
            .await?;
        writer.write_payload(payload).await?;
    }
    writer.finish().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{future::Future, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

    use bytes::Bytes;
    use moq_native_ietf::quic;
    use moq_transport::{
        coding::{KeyValuePairs, Location, TrackNamespace},
        data::{ExtensionHeaders, FetchRecord, FetchRecordObject},
        message::{self, GroupOrder, RequestErrorCode, SubscriptionFilter},
        serve::{FullTrackName, Subgroup, Track, TrackReaderMode},
        session::{Fetch, JoiningStart, Session, Subscriber, Transport},
    };
    use tokio::{sync::oneshot, task::JoinSet};
    use url::Url;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);
    const BLOCKED_PAYLOAD_SIZE: usize = 8 * 1024 * 1024;

    struct TestEndpoint {
        client: quic::Client,
        server: quic::Server,
        url: Url,
        addr: SocketAddr,
    }

    fn test_endpoint() -> TestEndpoint {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
        );
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = moq_native_ietf::tls::Config {
            client: client_tls,
            server: Some(server_tls),
            fingerprints: Vec::new(),
        };
        let endpoint = quic::Endpoint::new(
            quic::Config::new("0.0.0.0:0".parse().unwrap(), None, tls).unwrap(),
        )
        .unwrap();
        let client = endpoint.client;
        let server = endpoint.server.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], server.local_addr().unwrap().port()));

        TestEndpoint {
            client,
            server,
            url: Url::parse("moqt://localhost/").unwrap(),
            addr,
        }
    }

    struct RawSession {
        subscriber: Subscriber,
        publisher: Publisher,
        tasks: JoinSet<anyhow::Result<()>>,
        _client: quic::Client,
        _server: quic::Server,
    }

    impl RawSession {
        async fn start(fetch_history: Option<FetchHistory>) -> Self {
            let TestEndpoint {
                client,
                mut server,
                url,
                addr,
            } = test_endpoint();
            let (client_connection, server_connection) =
                tokio::join!(client.connect(&url, Some(addr)), server.accept());
            let (client_transport, _, client_kind) = client_connection.unwrap();
            let (server_transport, server_info) = server_connection.unwrap();
            assert_eq!(client_kind, Transport::RawQuic);
            assert_eq!(server_info.transport, Transport::RawQuic);

            let (client_parts, server_parts) = tokio::join!(
                Session::connect(client_transport, None, client_kind),
                Session::accept(server_transport, None, server_info.transport),
            );
            let (client_session, _, subscriber) = client_parts.unwrap();
            let (server_session, publisher, _) = server_parts.unwrap();
            let publisher = publisher.unwrap();

            let mut tasks = JoinSet::new();
            tasks.spawn(async move { client_session.run().await.map_err(Into::into) });
            tasks.spawn(async move { server_session.run().await.map_err(Into::into) });
            let fetch_publisher = publisher.clone();
            tasks.spawn(async move { serve_fetches(fetch_publisher, fetch_history).await });

            Self {
                subscriber,
                publisher,
                tasks,
                _client: client,
                _server: server,
            }
        }

        async fn shutdown(mut self) {
            self.tasks.abort_all();
            while let Some(result) = self.tasks.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Err(error) if error.is_cancelled() => {}
                    Ok(Err(error)) => panic!("background task failed: {error:#}"),
                    Err(error) => panic!("background task panicked: {error}"),
                }
            }
        }
    }

    async fn with_deadlock_guard<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(TEST_TIMEOUT, future)
            .await
            .expect("raw QUIC test deadlocked")
    }

    #[derive(Debug, Eq, PartialEq)]
    struct ReceivedObject {
        record: FetchRecordObject,
        payload: Bytes,
    }

    async fn collect_fetch(mut fetch: Fetch) -> (message::FetchOk, Vec<ReceivedObject>) {
        let ok = fetch.ok().await.unwrap();
        let mut objects = Vec::new();
        while let Some(record) = fetch.next().await.unwrap() {
            let FetchRecord::Object(record) = record else {
                panic!("expected typed FETCH Object, got {record:?}");
            };
            let mut payload = Vec::with_capacity(record.payload_length as usize);
            while let Some(chunk) = fetch.read_payload_chunk(64 * 1024).await.unwrap() {
                payload.extend_from_slice(&chunk);
            }
            assert_eq!(payload.len() as u64, record.payload_length);
            objects.push(ReceivedObject {
                record,
                payload: payload.into(),
            });
        }
        (ok, objects)
    }

    fn fetch_request(
        track: &FullTrackName,
        start: Location,
        end: Location,
    ) -> message::StandaloneFetch {
        message::StandaloneFetch {
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            start_location: start,
            end_location: end,
        }
    }

    fn track() -> FullTrackName {
        track_named("1.m4s")
    }

    fn track_named(name: &str) -> FullTrackName {
        FullTrackName {
            namespace: TrackNamespace::from_utf8_path("test/broadcast"),
            name: name.into(),
        }
    }

    fn history(capacity: usize) -> FetchHistory {
        FetchHistory::new(NonZeroUsize::new(capacity).unwrap())
    }

    fn object(group_id: u64, object_id: u64) -> ArchivedObject {
        let mut extension_headers = ExtensionHeaders::new();
        extension_headers.set_intvalue(2, group_id * 10 + object_id);
        ArchivedObject {
            group_id,
            subgroup_id: 3,
            object_id,
            publisher_priority: 127,
            extension_headers,
            payload: Bytes::from(format!("g{group_id}-o{object_id}")),
        }
    }

    fn append_group(
        history: &FetchHistory,
        track: &FullTrackName,
        group_id: u64,
        objects: u64,
        complete: bool,
    ) {
        history.begin_group(track, group_id);
        for object_id in 0..objects {
            history.insert_object(track, object(group_id, object_id));
        }
        if complete {
            history.complete_group(track, group_id);
        }
    }

    fn insert_group(
        history: &FetchHistory,
        track: &FullTrackName,
        group_id: u64,
        objects: impl IntoIterator<Item = ArchivedObject>,
        complete: bool,
    ) {
        history.begin_group(track, group_id);
        for object in objects {
            history.insert_object(track, object);
        }
        if complete {
            history.complete_group(track, group_id);
        }
    }

    #[tokio::test]
    async fn raw_quic_standalone_fetch_returns_typed_payload_and_exact_ok() {
        with_deadlock_guard(async {
            let history = history(2);
            let track = track_named("standalone.m4s");
            history.add_track(track.clone());
            append_group(&history, &track, 4, 2, true);
            let expected_object = object(4, 1);

            let mut peer = RawSession::start(Some(history)).await;
            let fetch = peer
                .subscriber
                .fetch(
                    fetch_request(&track, Location::new(4, 1), Location::new(4, 2)),
                    KeyValuePairs::default(),
                )
                .unwrap();
            let request_id = fetch.request.id;
            let (ok, objects) = collect_fetch(fetch).await;

            assert_eq!(
                ok,
                message::FetchOk {
                    id: request_id,
                    end_of_track: false,
                    end_location: Location::new(4, 2),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                }
            );
            assert_eq!(
                objects,
                [ReceivedObject {
                    record: FetchRecordObject {
                        group_id: expected_object.group_id,
                        subgroup_id: Some(expected_object.subgroup_id),
                        object_id: expected_object.object_id,
                        publisher_priority: expected_object.publisher_priority,
                        extension_headers: expected_object.extension_headers,
                        payload_length: expected_object.payload.len() as u64,
                    },
                    payload: expected_object.payload,
                }]
            );

            peer.shutdown().await;
        })
        .await;
    }

    #[tokio::test]
    async fn raw_quic_fetch_rejections_are_typed_for_disabled_missing_and_evicted() {
        with_deadlock_guard(async {
            enum Case {
                Disabled,
                Missing,
                Evicted,
            }

            for (case, code, reason) in [
                (
                    Case::Disabled,
                    RequestErrorCode::NotSupported,
                    "FETCH is disabled",
                ),
                (
                    Case::Missing,
                    RequestErrorCode::DoesNotExist,
                    "track is not retained",
                ),
                (
                    Case::Evicted,
                    RequestErrorCode::DoesNotExist,
                    "requested range is not retained",
                ),
            ] {
                let requested_track = track_named(match case {
                    Case::Disabled => "disabled.m4s",
                    Case::Missing => "missing.m4s",
                    Case::Evicted => "evicted.m4s",
                });
                let fetch_history = match case {
                    Case::Disabled => None,
                    Case::Missing => Some(history(1)),
                    Case::Evicted => {
                        let history = history(1);
                        history.add_track(requested_track.clone());
                        append_group(&history, &requested_track, 0, 1, true);
                        append_group(&history, &requested_track, 1, 1, true);
                        Some(history)
                    }
                };
                let mut peer = RawSession::start(fetch_history).await;
                let rejected = peer
                    .subscriber
                    .fetch(
                        fetch_request(&requested_track, Location::new(0, 0), Location::new(1, 0)),
                        KeyValuePairs::default(),
                    )
                    .unwrap();

                assert!(rejected.ok().await.is_err());
                let rejection = rejected.rejection().expect("missing typed rejection");
                assert_eq!(rejection.error_code(), code as u64);
                assert_eq!(rejection.retry_interval(), 0);
                assert_eq!(rejection.reason().0, reason);

                peer.shutdown().await;
            }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raw_quic_blocked_then_cancelled_fetch_does_not_block_siblings() {
        with_deadlock_guard(async {
            let history = history(1);
            let blocked_track = track_named("blocked.m4s");
            let sibling_track = track_named("sibling.m4s");
            history.add_track(blocked_track.clone());
            history.add_track(sibling_track.clone());

            let mut blocked_object = object(0, 0);
            blocked_object.payload = Bytes::from(vec![0x5a; BLOCKED_PAYLOAD_SIZE]);
            insert_group(&history, &blocked_track, 0, [blocked_object], true);
            append_group(&history, &sibling_track, 0, 1, true);

            let mut peer = RawSession::start(Some(history)).await;
            let blocked = peer
                .subscriber
                .fetch(
                    fetch_request(&blocked_track, Location::new(0, 0), Location::new(1, 0)),
                    KeyValuePairs::default(),
                )
                .unwrap();
            blocked.ok().await.unwrap();

            let cancel = Arc::new(tokio::sync::Notify::new());
            let cancel_wait = cancel.clone();
            let mut blocked_holder = Some(tokio::spawn(async move {
                cancel_wait.notified().await;
                drop(blocked);
            }));

            for expected_round in 0..2 {
                let sibling = peer
                    .subscriber
                    .fetch(
                        fetch_request(&sibling_track, Location::new(0, 0), Location::new(1, 0)),
                        KeyValuePairs::default(),
                    )
                    .unwrap();
                let (_, objects) = collect_fetch(sibling).await;
                assert_eq!(objects.len(), 1, "sibling round {expected_round}");
                assert_eq!(objects[0].payload, Bytes::from_static(b"g0-o0"));

                if expected_round == 0 {
                    cancel.notify_one();
                    blocked_holder.take().unwrap().await.unwrap();
                }
            }

            peer.shutdown().await;
        })
        .await;
    }

    async fn run_joining_case(start: JoiningStart) {
        let history = history(2);
        let track = track_named(match start {
            JoiningStart::Relative(_) => "relative.m4s",
            JoiningStart::Absolute(_) => "absolute.m4s",
        });
        history.add_track(track.clone());
        append_group(&history, &track, 6, 1, true);
        append_group(&history, &track, 7, 1, false);

        let (source_writer, source_reader) =
            Track::new(track.namespace.clone(), track.name.clone()).produce();
        let mut source = source_writer.subgroups().unwrap();
        let mut source_group = source
            .create(Subgroup {
                group_id: 7,
                subgroup_id: 3,
                priority: 127,
            })
            .unwrap();
        source_group.write(Bytes::from_static(b"g7-o0")).unwrap();

        let mut peer = RawSession::start(Some(history)).await;
        let mut subscribe_publisher = peer.publisher.clone();
        let subscribed_task = tokio::spawn(async move {
            let subscribed = subscribe_publisher.subscribed().await.unwrap();
            subscribed.serve(source_reader).await
        });

        let (release_live, live_released) = oneshot::channel();
        let (finish_source, source_finished) = oneshot::channel();
        let source_task = tokio::spawn(async move {
            live_released.await.unwrap();
            source_group
                .write(Bytes::from_static(b"g7-o1-live"))
                .unwrap();
            source_finished.await.unwrap();
            drop(source_group);
            drop(source);
        });

        let (destination_writer, destination_reader) =
            Track::new(track.namespace.clone(), track.name.clone()).produce();
        let mut params = KeyValuePairs::default();
        params
            .set_subscription_filter(&SubscriptionFilter::largest_object())
            .unwrap();
        let subscribe = peer
            .subscriber
            .subscribe_open_with_params(destination_writer, params)
            .await
            .unwrap();
        release_live.send(()).unwrap();

        let fetch = subscribe
            .fetch_joining(start, KeyValuePairs::default())
            .unwrap();
        let (ok, fetched) = collect_fetch(fetch).await;
        assert_eq!(ok.end_location, Location::new(7, 1));
        assert_eq!(
            fetched
                .iter()
                .map(|object| (
                    object.record.group_id,
                    object.record.object_id,
                    &object.payload
                ))
                .collect::<Vec<_>>(),
            [
                (6, 0, &Bytes::from_static(b"g6-o0")),
                (7, 0, &Bytes::from_static(b"g7-o0")),
            ]
        );

        let TrackReaderMode::Subgroups(mut live_groups) = destination_reader.mode().await.unwrap()
        else {
            panic!("expected live subgroup delivery");
        };
        let mut live_group = live_groups.next().await.unwrap().unwrap();
        let mut live_object = live_group.next().await.unwrap().unwrap();
        let live_payload = live_object.read_all().await.unwrap();
        assert_eq!(live_payload, Bytes::from_static(b"g7-o1-live"));

        finish_source.send(()).unwrap();
        source_task.await.unwrap();
        assert!(live_group.next().await.unwrap().is_none());
        assert!(matches!(
            live_groups.next().await,
            Ok(None) | Err(ServeError::Done)
        ));
        subscribed_task.await.unwrap().unwrap();

        let all_payloads = fetched
            .iter()
            .map(|object| object.payload.clone())
            .chain([live_payload])
            .collect::<Vec<_>>();
        assert_eq!(
            all_payloads,
            [
                Bytes::from_static(b"g6-o0"),
                Bytes::from_static(b"g7-o0"),
                Bytes::from_static(b"g7-o1-live"),
            ]
        );
        assert_eq!(
            all_payloads
                .iter()
                .filter(|payload| payload.as_ref() == b"g7-o0")
                .count(),
            1
        );
        assert_eq!(
            all_payloads
                .iter()
                .filter(|payload| payload.as_ref() == b"g7-o1-live")
                .count(),
            1
        );

        drop(subscribe);
        peer.shutdown().await;
    }

    #[tokio::test]
    async fn raw_quic_relative_and_absolute_joining_are_contiguous_and_non_overlapping() {
        with_deadlock_guard(async {
            for start in [JoiningStart::Relative(1), JoiningStart::Absolute(6)] {
                run_joining_case(start).await;
            }
        })
        .await;
    }

    #[test]
    fn snapshot_honors_range_and_group_order_without_reordering_objects() {
        let history = history(3);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 2, true);
        append_group(&history, &track, 1, 3, true);
        append_group(&history, &track, 2, 2, false);

        let ascending = history
            .snapshot(
                &track,
                Location::new(0, 1),
                Location::new(2, 1),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(
            ascending
                .objects
                .iter()
                .map(|object| (object.group_id, object.object_id))
                .collect::<Vec<_>>(),
            [(0, 1), (1, 0), (1, 1), (1, 2), (2, 0)]
        );
        assert_eq!(ascending.end_location, Location::new(2, 1));

        let descending = history
            .snapshot(
                &track,
                Location::new(0, 1),
                Location::new(2, 1),
                GroupOrder::Descending,
            )
            .unwrap();
        assert_eq!(
            descending
                .objects
                .iter()
                .map(|object| (object.group_id, object.object_id))
                .collect::<Vec<_>>(),
            [(2, 0), (1, 0), (1, 1), (1, 2), (0, 1)]
        );
        assert_eq!(descending.end_location, ascending.end_location);
    }

    #[test]
    fn snapshot_uses_whole_group_end_and_clamps_an_open_group() {
        let history = history(2);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 2, true);
        append_group(&history, &track, 1, 2, false);

        let completed = history
            .snapshot(
                &track,
                Location::new(0, 0),
                Location::new(0, 0),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(completed.end_location, Location::new(0, 0));

        let current = history
            .snapshot(
                &track,
                Location::new(1, 0),
                Location::new(1, 0),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(current.end_location, Location::new(1, 2));

        history.complete_group(&track, 1);
        let now_completed = history
            .snapshot(
                &track,
                Location::new(1, 0),
                Location::new(1, 0),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(now_completed.end_location, Location::new(1, 0));
    }

    #[test]
    fn newest_group_limit_evicts_only_whole_groups_with_completed_objects() {
        let history = history(2);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 3, true);
        append_group(&history, &track, 1, 2, true);
        append_group(&history, &track, 2, 1, false);

        let retained = history
            .snapshot(
                &track,
                Location::new(1, 0),
                Location::new(2, 0),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(
            retained
                .objects
                .iter()
                .map(|object| (object.group_id, object.object_id))
                .collect::<Vec<_>>(),
            [(1, 0), (1, 1), (2, 0)]
        );
        assert_eq!(history.evicted_watermark(&track), Some(0));
        assert_eq!(
            history.snapshot(
                &track,
                Location::new(0, 2),
                Location::new(2, 0),
                GroupOrder::Ascending,
            ),
            Err(FetchError::NotRetained)
        );
    }

    #[test]
    fn equal_nonzero_range_is_a_valid_empty_fetch() {
        let history = history(1);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 1, false);

        assert_eq!(
            history
                .snapshot(
                    &track,
                    Location::new(7, 3),
                    Location::new(7, 3),
                    GroupOrder::Ascending,
                )
                .unwrap(),
            HistorySnapshot {
                objects: Vec::new(),
                end_location: Location::new(7, 3),
            }
        );
    }

    #[test]
    fn equal_nonzero_range_stays_empty_after_its_group_is_evicted() {
        let history = history(1);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 1, true);
        append_group(&history, &track, 1, 1, false);

        assert_eq!(
            history
                .snapshot(
                    &track,
                    Location::new(0, 3),
                    Location::new(0, 3),
                    GroupOrder::Ascending,
                )
                .unwrap(),
            HistorySnapshot {
                objects: Vec::new(),
                end_location: Location::new(0, 3),
            }
        );
    }

    #[test]
    fn completed_group_clamps_non_sentinel_end_to_largest_object() {
        let history = history(1);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 2, true);

        let snapshot = history
            .snapshot(
                &track,
                Location::new(0, 0),
                Location::new(0, 99),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(snapshot.end_location, Location::new(0, 2));
        assert_eq!(snapshot.objects.len(), 2);
    }

    #[test]
    fn completed_group_preserves_known_gap_before_a_later_group() {
        let history = history(2);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 2, true);
        append_group(&history, &track, 1, 1, false);

        let snapshot = history
            .snapshot(
                &track,
                Location::new(0, 0),
                Location::new(0, 99),
                GroupOrder::Ascending,
            )
            .unwrap();
        assert_eq!(snapshot.end_location, Location::new(0, 99));
        assert_eq!(snapshot.objects.len(), 2);
    }

    #[test]
    fn unknown_track_and_application_failures_have_typed_rejections() {
        let history = history(1);
        assert_eq!(
            history.snapshot(
                &track(),
                Location::new(0, 0),
                Location::new(0, 0),
                GroupOrder::Ascending,
            ),
            Err(FetchError::DoesNotExist)
        );

        assert_eq!(
            FetchError::Disabled.rejection().unwrap().error_code(),
            RequestErrorCode::NotSupported as u64
        );
        assert_eq!(
            FetchError::DoesNotExist.rejection().unwrap().error_code(),
            RequestErrorCode::DoesNotExist as u64
        );
        assert_eq!(
            FetchError::NotRetained.rejection().unwrap().error_code(),
            RequestErrorCode::DoesNotExist as u64
        );
        assert_eq!(
            FetchError::InvalidRange.rejection().unwrap().error_code(),
            RequestErrorCode::InvalidRange as u64
        );
    }

    #[test]
    fn snapshot_rejects_a_start_beyond_the_largest_published_object() {
        let history = history(2);
        let track = track();
        history.add_track(track.clone());
        append_group(&history, &track, 0, 2, true);
        history.begin_group(&track, 1);

        assert_eq!(
            history.snapshot(
                &track,
                Location::new(1, 0),
                Location::new(1, 0),
                GroupOrder::Ascending,
            ),
            Err(FetchError::InvalidRange)
        );
    }
}
