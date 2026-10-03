// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{io::Cursor, sync::Arc};

use anyhow::Context;
use moq_transport::{
    coding::{KeyValuePairs, Location},
    data::{FetchRecord, ObjectStatus},
    message::{StandaloneFetch, SubscriptionFilter},
    serve::{
        SubgroupObjectReader, SubgroupReader, TrackReader, TrackReaderMode, TrackWriter, Tracks,
        TracksReader, TracksWriter,
    },
    session::{Fetch, FetchRejection, JoiningStart, Subscribe, Subscriber},
};
use mp4::ReadBox;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tracing::{debug, info, trace, warn};

const FETCH_CHUNK_SIZE: usize = 64 * 1024;
const LIVE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const LIVE_BUFFER_OBJECTS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchMode {
    Standalone { start: Location, end: Location },
    Joining(JoiningStart),
}

#[derive(Clone)]
struct LiveBuffer {
    available: Arc<Semaphore>,
    capacity: usize,
}

impl LiveBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            available: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    fn reserve(&self, bytes: usize) -> anyhow::Result<OwnedSemaphorePermit> {
        anyhow::ensure!(
            bytes <= self.capacity,
            "object size {bytes} exceeds live buffer capacity {}",
            self.capacity
        );
        let bytes = u32::try_from(bytes).context("live object is too large to account for")?;
        self.available
            .clone()
            .try_acquire_many_owned(bytes)
            .context("live buffer capacity exceeded")
    }
}

struct BufferedObject {
    payload: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}

pub struct Media<O> {
    subscriber: Subscriber,
    broadcast: TracksReader,
    tracks_writer: TracksWriter,
    output: Arc<Mutex<O>>,
    request_catalog: bool,
    fetch: Option<FetchMode>,
}

impl<O: AsyncWrite + Send + Unpin + 'static> Media<O> {
    pub async fn new(
        subscriber: Subscriber,
        tracks: Tracks,
        output: O,
        request_catalog: bool,
    ) -> anyhow::Result<Self> {
        Self::new_with_fetch(subscriber, tracks, output, request_catalog, None).await
    }

    pub async fn new_with_fetch(
        subscriber: Subscriber,
        tracks: Tracks,
        output: O,
        request_catalog: bool,
        fetch: Option<FetchMode>,
    ) -> anyhow::Result<Self> {
        let (tracks_writer, _tracks_request, tracks_reader) = tracks.produce();
        let broadcast = tracks_reader; // breadcrumb for navigating API name changes
        Ok(Self {
            subscriber,
            broadcast,
            tracks_writer,
            output: Arc::new(Mutex::new(output)),
            request_catalog,
            fetch,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let catalog = if self.request_catalog {
            // The catalog track has no standardized name, but
            // both moq-pub of moq-rs and gst-moq-pub uses ".catalog".
            let buf = self.download_first_object(".catalog", "catalog").await?;
            let s = std::str::from_utf8(&buf)?;
            let c: moq_catalog::Root = serde_json::from_str(s)?;
            info!("catalog: {c:#?}");
            anyhow::ensure!(c.version == 1, "Unknown catalog version");
            Some(c)
        } else {
            None
        };
        let moov = {
            let init_track_name = match catalog {
                Some(ref catalog) => catalog
                    .tracks
                    .first()
                    .context("catalog contains no tracks")?
                    .init_track
                    .clone()
                    .context("catalog track contains no init track")?,
                None => "0.mp4".to_string(),
            };
            let buf = self.download_first_object(&init_track_name, "init").await?;
            self.output.lock().await.write_all(&buf).await?;
            let mut reader = Cursor::new(&buf);

            let ftyp = read_atom(&mut reader).await?;
            anyhow::ensure!(&ftyp[4..8] == b"ftyp", "expected ftyp atom");

            let moov = read_atom(&mut reader).await?;
            anyhow::ensure!(&moov[4..8] == b"moov", "expected moov atom");
            let mut moov_reader = Cursor::new(&moov);
            let moov_header = mp4::BoxHeader::read(&mut moov_reader)?;

            mp4::MoovBox::read_box(&mut moov_reader, moov_header.size)?
        };

        let mut has_video = false;
        let mut has_audio = false;
        let mut tracks = vec![];
        for (idx, trak) in moov.traks.into_iter().enumerate() {
            let id = trak.tkhd.track_id;
            let name: String = match catalog {
                Some(ref catalog) => catalog
                    .tracks
                    .get(idx)
                    .context("catalog has fewer tracks than the init segment")?
                    .name
                    .clone(),
                None => format!("{id}.m4s"),
            };
            info!("found track {name}");
            let mut active = false;
            if !has_video && trak.mdia.minf.stbl.stsd.avc1.is_some() {
                active = true;
                has_video = true;
                info!("using {name} for video");
            }
            if !has_audio && trak.mdia.minf.stbl.stsd.mp4a.is_some() {
                active = true;
                has_audio = true;
                info!("using {name} for audio");
            }
            if active {
                tracks.push(name);
            }
        }

        info!("playing {} tracks", tracks.len());
        match self.fetch {
            None => self.run_live(tracks).await,
            Some(FetchMode::Standalone { start, end }) => {
                self.run_standalone(tracks, start, end).await
            }
            Some(FetchMode::Joining(start)) => self.run_joining(tracks, start).await,
        }
    }

    async fn download_first_object(
        &mut self,
        track_name: &str,
        alias: &'static str,
    ) -> anyhow::Result<Vec<u8>> {
        let (writer, track) = self.create_track(track_name, alias)?;
        let _subscribe = self
            .subscriber
            .subscribe_open(writer)
            .await
            .with_context(|| format!("failed to subscribe to {alias} track"))?;
        let mut group = match track.mode().await? {
            TrackReaderMode::Subgroups(mut groups) => {
                groups.next().await?.context(format!("no {alias} group"))?
            }
            _ => anyhow::bail!("expected {alias} segment"),
        };

        let object = group
            .next()
            .await?
            .context(format!("no {alias} fragment"))?;
        let buf = Self::recv_object(object).await?;
        Ok(buf)
    }

    fn create_track(
        &mut self,
        track_name: &str,
        alias: &str,
    ) -> anyhow::Result<(TrackWriter, TrackReader)> {
        let writer = self
            .tracks_writer
            .create(track_name)
            .with_context(|| format!("failed to create {alias} track"))?;
        let reader = self
            .broadcast
            .subscribe(self.broadcast.namespace.clone(), track_name)
            .with_context(|| format!("no {alias} track"))?;
        Ok((writer, reader))
    }

    async fn run_live(&mut self, track_names: Vec<String>) -> anyhow::Result<()> {
        let mut tracks = Vec::with_capacity(track_names.len());
        let mut subscriptions = Vec::with_capacity(track_names.len());
        for name in track_names {
            let (writer, reader) = self.create_track(&name, "media")?;
            let subscribe = self
                .subscriber
                .subscribe_open(writer)
                .await
                .with_context(|| format!("failed to subscribe to media track {name}"))?;
            tracks.push(reader);
            subscriptions.push(subscribe);
        }

        let mut tasks = JoinSet::new();
        for track in tracks {
            let out = self.output.clone();
            let name = track.name.to_string();
            tasks.spawn(async move {
                Self::recv_track(track, out)
                    .await
                    .with_context(|| format!("failed to play track {name}"))
            });
        }
        Self::wait_for_tasks(&mut tasks, "live media", &self.output).await?;
        drop(subscriptions);
        Ok(())
    }

    async fn run_standalone(
        &mut self,
        track_names: Vec<String>,
        start: Location,
        end: Location,
    ) -> anyhow::Result<()> {
        let namespace = self.broadcast.namespace.clone();
        let mut fetches = Vec::with_capacity(track_names.len());
        for name in track_names {
            let fetch = self
                .subscriber
                .fetch(
                    StandaloneFetch {
                        track_namespace: namespace.clone(),
                        track_name: name.clone().into(),
                        start_location: start,
                        end_location: end,
                    },
                    KeyValuePairs::default(),
                )
                .with_context(|| format!("failed to request FETCH for track {name}"))?;
            fetches.push((name, fetch));
        }

        let budget = LiveBuffer::new(LIVE_BUFFER_BYTES);
        let mut tasks = Self::spawn_fetches(fetches, self.output.clone(), budget);
        Self::wait_for_tasks(&mut tasks, "FETCH", &self.output).await?;
        self.output.lock().await.flush().await?;
        Ok(())
    }

    async fn run_joining(
        &mut self,
        track_names: Vec<String>,
        start: JoiningStart,
    ) -> anyhow::Result<()> {
        let (send, receive) = mpsc::channel(LIVE_BUFFER_OBJECTS);
        let buffer = LiveBuffer::new(LIVE_BUFFER_BYTES);
        let mut collectors = JoinSet::new();
        let mut subscriptions: Vec<(String, Subscribe)> = Vec::with_capacity(track_names.len());

        for name in track_names {
            let (writer, reader) = self.create_track(&name, "media")?;
            let (started_send, started_receive) = oneshot::channel();
            let collector_send = send.clone();
            let collector_buffer = buffer.clone();
            let collector_name = name.clone();
            collectors.spawn(async move {
                Self::collect_live(reader, collector_send, collector_buffer, started_send)
                    .await
                    .with_context(|| format!("failed to collect live track {collector_name}"))
            });
            started_receive
                .await
                .with_context(|| format!("live collector for track {name} failed to start"))?;

            let mut params = KeyValuePairs::default();
            params
                .set_subscription_filter(&SubscriptionFilter::largest_object())
                .context("failed to encode Largest Object subscription filter")?;
            let subscribe = self.subscriber.subscribe_open_with_params(writer, params);
            tokio::pin!(subscribe);
            let subscribe = tokio::select! {
                result = &mut subscribe => result
                    .with_context(|| format!("failed to subscribe to media track {name}"))?,
                result = collectors.join_next() => {
                    let result = result.context("live collector task set closed during setup")?;
                    result.context("live collector task was cancelled during setup")??;
                    anyhow::bail!("live collector ended during joining setup");
                }
            };
            subscriptions.push((name, subscribe));
        }
        drop(send);

        let mut fetches = Vec::with_capacity(subscriptions.len());
        for (name, subscribe) in &subscriptions {
            let fetch = subscribe
                .fetch_joining(start, KeyValuePairs::default())
                .with_context(|| format!("failed to request joining FETCH for track {name}"))?;
            fetches.push((name.clone(), fetch));
        }
        let mut fetch_tasks = Self::spawn_fetches(fetches, self.output.clone(), buffer.clone());
        Self::wait_for_fetches(&mut fetch_tasks, &mut collectors, &self.output).await?;
        self.output
            .lock()
            .await
            .flush()
            .await
            .context("failed to flush fetched media")?;

        info!("joining FETCH complete; releasing buffered live media");
        Self::drain_live(receive, &mut collectors, self.output.clone()).await?;
        drop(subscriptions);
        Ok(())
    }

    fn spawn_fetches(
        fetches: Vec<(String, Fetch)>,
        output: Arc<Mutex<O>>,
        budget: LiveBuffer,
    ) -> JoinSet<anyhow::Result<()>> {
        let mut tasks = JoinSet::new();
        for (name, fetch) in fetches {
            let output = output.clone();
            let budget = budget.clone();
            tasks.spawn(async move {
                Self::drain_fetch(fetch, output, budget)
                    .await
                    .with_context(|| format!("FETCH failed for track {name}"))
            });
        }
        tasks
    }

    async fn drain_fetch(
        mut fetch: Fetch,
        output: Arc<Mutex<O>>,
        budget: LiveBuffer,
    ) -> anyhow::Result<()> {
        loop {
            let record = match fetch.next().await {
                Ok(record) => record,
                Err(error) => {
                    return Err(fetch_error(&fetch, "failed to read FETCH record", error))
                }
            };
            let Some(record) = record else {
                break;
            };
            match record {
                FetchRecord::Object(object) => {
                    let capacity = usize::try_from(object.payload_length)
                        .context("FETCH object payload does not fit in memory")?;
                    let _permit = budget
                        .reserve(capacity)
                        .context("FETCH object exceeds the shared byte budget")?;
                    let mut payload = Vec::new();
                    payload
                        .try_reserve_exact(capacity)
                        .context("failed to allocate FETCH object payload")?;
                    while let Some(chunk) = fetch
                        .read_payload_chunk(FETCH_CHUNK_SIZE)
                        .await
                        .map_err(|error| {
                            fetch_error(&fetch, "failed to read FETCH object payload", error)
                        })?
                    {
                        payload.extend_from_slice(&chunk);
                    }
                    anyhow::ensure!(
                        payload.len() == capacity,
                        "FETCH object payload length mismatch: expected {capacity}, received {}",
                        payload.len()
                    );
                    output.lock().await.write_all(&payload).await?;
                }
                FetchRecord::NotExist { end } => {
                    warn!(
                        group = end.group_id,
                        object = end.object_id,
                        "FETCH range does not exist; marker emits no bytes"
                    );
                }
                FetchRecord::Unknown { end } => {
                    warn!(
                        group = end.group_id,
                        object = end.object_id,
                        "FETCH range status is unknown; marker emits no bytes"
                    );
                }
            }
        }

        fetch
            .ok()
            .await
            .map_err(|error| fetch_error(&fetch, "FETCH did not complete successfully", error))?;
        Ok(())
    }

    async fn collect_live(
        track: TrackReader,
        send: mpsc::Sender<BufferedObject>,
        buffer: LiveBuffer,
        started: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        let name = track.name.to_string();
        let _ = started.send(());
        let TrackReaderMode::Subgroups(mut groups) = track.mode().await? else {
            anyhow::bail!("expected media track {name} to use subgroups");
        };
        while let Some(mut group) = groups.next().await? {
            while let Some(object) = group.next().await? {
                let group_id = object.group_id;
                let object_id = object.object_id;
                let status = object.status;
                let size = object.size;
                let permit = buffer
                    .reserve(size)
                    .with_context(|| format!("failed to buffer {name} {group_id}:{object_id}"))?;
                let payload = Self::recv_object(object).await?;
                anyhow::ensure!(
                    payload.len() == size,
                    "live object payload length mismatch: expected {}, received {}",
                    size,
                    payload.len()
                );
                if status != ObjectStatus::NormalObject {
                    warn!(
                        track = %name,
                        group = group_id,
                        object = object_id,
                        ?status,
                        "live object marker emits no bytes"
                    );
                    continue;
                }
                send.try_send(BufferedObject {
                    payload,
                    _permit: permit,
                })
                .context("live object buffer capacity exceeded")?;
            }
        }
        Ok(())
    }

    async fn wait_for_fetches(
        fetches: &mut JoinSet<anyhow::Result<()>>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
        output: &Arc<Mutex<O>>,
    ) -> anyhow::Result<()> {
        while !fetches.is_empty() {
            let result = if collectors.is_empty() {
                fetches
                    .join_next()
                    .await
                    .context("FETCH task set closed")?
                    .context("FETCH task was cancelled")
                    .and_then(|result| result)
            } else {
                tokio::select! {
                result = fetches.join_next() => {
                    let result = result.context("FETCH task set closed")?;
                    result.context("FETCH task was cancelled").and_then(|result| result)
                }
                result = collectors.join_next() => {
                    let result = result.context("live collector task set closed")?;
                    result.context("live collector task was cancelled").and_then(|result| result)
                }
                }
            };
            if let Err(error) = result {
                Self::abort_joining_tasks(fetches, collectors, output).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn drain_live(
        mut receive: mpsc::Receiver<BufferedObject>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
        output: Arc<Mutex<O>>,
    ) -> anyhow::Result<()> {
        let mut buffer_closed = false;
        loop {
            if buffer_closed {
                let Some(result) = collectors.join_next().await else {
                    output.lock().await.flush().await?;
                    return Ok(());
                };
                result.context("live collector task was cancelled")??;
                continue;
            }
            if collectors.is_empty() {
                match receive.recv().await {
                    Some(object) => output.lock().await.write_all(&object.payload).await?,
                    None => {
                        output.lock().await.flush().await?;
                        return Ok(());
                    }
                }
                continue;
            }
            tokio::select! {
                object = receive.recv() => match object {
                    Some(object) => output.lock().await.write_all(&object.payload).await?,
                    None => buffer_closed = true,
                },
                result = collectors.join_next() => {
                    let result = result.context("live collector task set closed")?;
                    result.context("live collector task was cancelled")??;
                }
            }
        }
    }

    async fn wait_for_tasks(
        tasks: &mut JoinSet<anyhow::Result<()>>,
        kind: &str,
        output: &Arc<Mutex<O>>,
    ) -> anyhow::Result<()> {
        while let Some(result) = tasks.join_next().await {
            let result = result
                .with_context(|| format!("{kind} task was cancelled"))
                .and_then(|result| result);
            if let Err(error) = result {
                Self::abort_tasks(tasks, output).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn abort_tasks(tasks: &mut JoinSet<anyhow::Result<()>>, output: &Arc<Mutex<O>>) {
        let _output = output.lock().await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    async fn abort_joining_tasks(
        fetches: &mut JoinSet<anyhow::Result<()>>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
        output: &Arc<Mutex<O>>,
    ) {
        let _output = output.lock().await;
        fetches.abort_all();
        collectors.abort_all();
        while fetches.join_next().await.is_some() {}
        while collectors.join_next().await.is_some() {}
    }

    async fn recv_track(track: TrackReader, out: Arc<Mutex<O>>) -> anyhow::Result<()> {
        let name = track.name.clone();
        debug!("track {name}: start");
        let TrackReaderMode::Subgroups(mut groups) = track.mode().await? else {
            anyhow::bail!("expected media track {name} to use subgroups");
        };
        while let Some(group) = groups.next().await? {
            Self::recv_group(group, out.clone()).await?;
        }
        debug!("track {name}: finish");
        Ok(())
    }

    async fn recv_group(mut group: SubgroupReader, out: Arc<Mutex<O>>) -> anyhow::Result<()> {
        trace!("group={} start", group.group_id);
        while let Some(object) = group.next().await? {
            trace!(
                "group={} fragment={} start",
                group.group_id,
                object.object_id
            );
            let out = out.clone();
            let buf = Self::recv_object(object).await?;

            out.lock().await.write_all(&buf).await?;
        }

        Ok(())
    }

    async fn recv_object(mut object: SubgroupObjectReader) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(object.size);
        while let Some(chunk) = object.read().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

fn format_rejection(rejection: &FetchRejection) -> String {
    format!(
        "error_code={} retry_interval={} reason={:?}",
        rejection.error_code(),
        rejection.retry_interval(),
        rejection.reason().0
    )
}

fn fetch_error(fetch: &Fetch, operation: &str, source: impl std::fmt::Display) -> anyhow::Error {
    match fetch.rejection() {
        Some(rejection) => anyhow::anyhow!(
            "{operation}: FETCH rejected: {}: {source}",
            format_rejection(&rejection)
        ),
        None => anyhow::anyhow!("{operation}: {source}"),
    }
}

// Read a full MP4 atom into a vector.
async fn read_atom<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<Vec<u8>> {
    // Read the 8 bytes for the size + type
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;

    // Convert the first 4 bytes into the size.
    let size = u32::from_be_bytes(buf[0..4].try_into()?) as u64;

    let mut raw = buf.to_vec();

    let mut limit = match size {
        // Runs until the end of the file.
        0 => reader.take(u64::MAX),

        // The next 8 bytes are the extended size to be used instead.
        1 => {
            reader.read_exact(&mut buf).await?;
            let size_large = u64::from_be_bytes(buf);
            anyhow::ensure!(
                size_large >= 16,
                "impossible extended box size: {}",
                size_large
            );

            reader.take(size_large - 16)
        }

        2..=7 => {
            anyhow::bail!("impossible box size: {}", size)
        }

        size => reader.take(size - 8),
    };

    // Append to the vector and return it.
    let _read_bytes = limit.read_to_end(&mut raw).await?;

    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::message::RequestErrorCode;

    #[test]
    fn legacy_media_constructor_remains_available() {
        async fn compile_old_call(subscriber: Subscriber, tracks: Tracks) {
            let _ = Media::new(subscriber, tracks, tokio::io::sink(), false).await;
        }

        let _ = compile_old_call;
    }

    #[test]
    fn rejection_context_preserves_wire_metadata() {
        let rejection = FetchRejection::new(RequestErrorCode::DoesNotExist, 17, "gone").unwrap();

        assert_eq!(
            format_rejection(&rejection),
            "error_code=16 retry_interval=17 reason=\"gone\""
        );
    }

    #[tokio::test]
    async fn live_buffer_is_bounded_by_payload_bytes() {
        let buffer = LiveBuffer::new(4);
        let first = buffer.reserve(3).unwrap();

        assert!(buffer.reserve(2).is_err());
        drop(first);
        assert!(buffer.reserve(2).is_ok());
    }

    #[tokio::test]
    async fn live_buffer_rejects_an_oversized_object() {
        let buffer = LiveBuffer::new(4);

        let error = buffer.reserve(5).expect_err("reserve should fail");
        assert!(error.to_string().contains("exceeds live buffer capacity"));
    }

    #[tokio::test]
    async fn fetch_gate_propagates_live_collector_errors() {
        let mut fetches = JoinSet::new();
        fetches.spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let mut collectors = JoinSet::new();
        collectors.spawn(async { anyhow::bail!("collector failed") });
        let output = Arc::new(Mutex::new(tokio::io::sink()));

        let error =
            Media::<tokio::io::Sink>::wait_for_fetches(&mut fetches, &mut collectors, &output)
                .await
                .expect_err("collector failure should cross the FETCH gate");

        assert_eq!(error.to_string(), "collector failed");
    }

    #[tokio::test]
    async fn task_failure_waits_for_in_progress_object_before_aborting_siblings() {
        let output = Arc::new(Mutex::new(tokio::io::sink()));
        let writer_output = output.clone();
        let (locked_send, locked_receive) = oneshot::channel();
        let (release_send, release_receive) = oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let _object_write = writer_output.lock().await;
            let _ = locked_send.send(());
            let _ = release_receive.await;
            Ok(())
        });
        locked_receive.await.unwrap();
        tasks.spawn(async { anyhow::bail!("sibling failed") });

        let wait = Media::<tokio::io::Sink>::wait_for_tasks(&mut tasks, "test", &output);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut wait)
                .await
                .is_err()
        );

        release_send.send(()).unwrap();
        let error = wait.await.expect_err("sibling error should propagate");
        assert_eq!(error.to_string(), "sibling failed");
    }
}
