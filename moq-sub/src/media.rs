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
const FETCH_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const FETCH_BUFFER_OBJECTS: usize = 32;
const LIVE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const LIVE_BUFFER_OBJECTS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchMode {
    Standalone { start: Location, end: Location },
    Joining(JoiningStart),
}

#[derive(Clone)]
struct BufferBudget {
    bytes: Arc<Semaphore>,
    objects: Arc<Semaphore>,
    byte_capacity: usize,
}

impl BufferBudget {
    fn new(byte_capacity: usize, object_capacity: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(byte_capacity)),
            objects: Arc::new(Semaphore::new(object_capacity)),
            byte_capacity,
        }
    }

    async fn reserve(&self, bytes: usize) -> anyhow::Result<BufferPermit> {
        anyhow::ensure!(
            bytes <= self.byte_capacity,
            "object size {bytes} exceeds buffer byte capacity {}",
            self.byte_capacity
        );
        let bytes = u32::try_from(bytes).context("object is too large to account for")?;
        let object = self
            .objects
            .clone()
            .acquire_owned()
            .await
            .context("buffer object budget closed")?;
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(bytes)
            .await
            .context("buffer byte budget closed")?;
        Ok(BufferPermit {
            _bytes: bytes,
            _object: object,
        })
    }

    fn close(&self) {
        self.bytes.close();
        self.objects.close();
    }
}

#[derive(Debug)]
struct BufferPermit {
    _bytes: OwnedSemaphorePermit,
    _object: OwnedSemaphorePermit,
}

struct BufferedObject {
    payload: Vec<u8>,
    _permit: BufferPermit,
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
        Self::wait_for_tasks(&mut tasks, "live media", &[]).await?;
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

        let budget = BufferBudget::new(FETCH_BUFFER_BYTES, FETCH_BUFFER_OBJECTS);
        let mut tasks = Self::spawn_fetches(fetches, self.output.clone(), budget.clone());
        Self::wait_for_tasks(&mut tasks, "FETCH", &[&budget]).await?;
        self.output
            .lock()
            .await
            .flush()
            .await
            .context("failed to flush fetched media")?;
        Ok(())
    }

    async fn run_joining(
        &mut self,
        track_names: Vec<String>,
        start: JoiningStart,
    ) -> anyhow::Result<()> {
        let (send, receive) = mpsc::channel(LIVE_BUFFER_OBJECTS);
        let live_budget = BufferBudget::new(LIVE_BUFFER_BYTES, LIVE_BUFFER_OBJECTS);
        let mut collectors = JoinSet::new();
        let mut subscriptions: Vec<(String, Subscribe)> = Vec::with_capacity(track_names.len());

        for name in track_names {
            let (writer, reader) = self.create_track(&name, "media")?;
            let (started_send, started_receive) = oneshot::channel();
            let collector_send = send.clone();
            let collector_budget = live_budget.clone();
            let collector_name = name.clone();
            collectors.spawn(async move {
                Self::collect_live(reader, collector_send, collector_budget, started_send)
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
        let fetch_budget = BufferBudget::new(FETCH_BUFFER_BYTES, FETCH_BUFFER_OBJECTS);
        let mut fetch_tasks =
            Self::spawn_fetches(fetches, self.output.clone(), fetch_budget.clone());
        Self::wait_for_fetches(
            &mut fetch_tasks,
            &mut collectors,
            &fetch_budget,
            &live_budget,
        )
        .await?;
        if let Err(error) = self.output.lock().await.flush().await {
            live_budget.close();
            collectors.abort_all();
            while collectors.join_next().await.is_some() {}
            return Err(error).context("failed to flush fetched media");
        }

        info!("joining FETCH complete; releasing buffered live media");
        Self::drain_live(receive, &mut collectors, self.output.clone(), live_budget).await?;
        drop(subscriptions);
        Ok(())
    }

    fn spawn_fetches(
        fetches: Vec<(String, Fetch)>,
        output: Arc<Mutex<O>>,
        budget: BufferBudget,
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
        budget: BufferBudget,
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
                        .await
                        .context("failed to reserve FETCH object budget")?;
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
                    output
                        .lock()
                        .await
                        .write_all(&payload)
                        .await
                        .context("failed to write FETCH object payload")?;
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
        budget: BufferBudget,
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
                let permit = budget
                    .reserve(size)
                    .await
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
                send.send(BufferedObject {
                    payload,
                    _permit: permit,
                })
                .await
                .map_err(|_| anyhow::anyhow!("live object buffer closed"))?;
            }
        }
        Ok(())
    }

    async fn wait_for_fetches(
        fetches: &mut JoinSet<anyhow::Result<()>>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
        fetch_budget: &BufferBudget,
        live_budget: &BufferBudget,
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
                fetch_budget.close();
                live_budget.close();
                Self::abort_joining_tasks(fetches, collectors).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn drain_live(
        mut receive: mpsc::Receiver<BufferedObject>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
        output: Arc<Mutex<O>>,
        budget: BufferBudget,
    ) -> anyhow::Result<()> {
        let result = async {
            let mut buffer_closed = false;
            loop {
                if buffer_closed {
                    let Some(result) = collectors.join_next().await else {
                        Self::flush_live_output(&output, &budget).await?;
                        return Ok(());
                    };
                    result.context("live collector task was cancelled")??;
                    continue;
                }
                if collectors.is_empty() {
                    match receive.recv().await {
                        Some(object) => Self::write_live_object(&output, &object, &budget).await?,
                        None => {
                            Self::flush_live_output(&output, &budget).await?;
                            return Ok(());
                        }
                    }
                    continue;
                }
                tokio::select! {
                    object = receive.recv() => match object {
                        Some(object) => {
                            Self::write_live_object(&output, &object, &budget).await?
                        }
                        None => buffer_closed = true,
                    },
                    result = collectors.join_next() => {
                        let result = result.context("live collector task set closed")?;
                        result.context("live collector task was cancelled")??;
                    }
                }
            }
        }
        .await;

        if result.is_err() {
            receive.close();
            budget.close();
            collectors.abort_all();
            while collectors.join_next().await.is_some() {}
        }
        result
    }

    async fn write_live_object(
        output: &Arc<Mutex<O>>,
        object: &BufferedObject,
        budget: &BufferBudget,
    ) -> anyhow::Result<()> {
        if let Err(error) = output.lock().await.write_all(&object.payload).await {
            budget.close();
            return Err(error).context("failed to write live object payload");
        }
        Ok(())
    }

    async fn flush_live_output(
        output: &Arc<Mutex<O>>,
        budget: &BufferBudget,
    ) -> anyhow::Result<()> {
        if let Err(error) = output.lock().await.flush().await {
            budget.close();
            return Err(error).context("failed to flush live media");
        }
        Ok(())
    }

    async fn wait_for_tasks(
        tasks: &mut JoinSet<anyhow::Result<()>>,
        kind: &str,
        budgets: &[&BufferBudget],
    ) -> anyhow::Result<()> {
        while let Some(result) = tasks.join_next().await {
            let result = result
                .with_context(|| format!("{kind} task was cancelled"))
                .and_then(|result| result);
            if let Err(error) = result {
                for budget in budgets {
                    budget.close();
                }
                Self::abort_tasks(tasks).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn abort_tasks(tasks: &mut JoinSet<anyhow::Result<()>>) {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    async fn abort_joining_tasks(
        fetches: &mut JoinSet<anyhow::Result<()>>,
        collectors: &mut JoinSet<anyhow::Result<()>>,
    ) {
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
    use moq_transport::{
        coding::TrackNamespace, message::RequestErrorCode, reexports::bytes::Bytes, serve::Track,
    };
    use std::{
        io,
        pin::Pin,
        task::{Context as TaskContext, Poll},
        time::Duration,
    };

    struct FailingWriter;

    struct PendingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition did not become true");
    }

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
    async fn concurrent_budget_contention_waits_then_progresses_within_aggregate_limits() {
        let budget = BufferBudget::new(5, 2);
        let first = budget.reserve(3).await.unwrap();

        let second_budget = budget.clone();
        let (second_started_send, second_started_receive) = oneshot::channel();
        let second = tokio::spawn(async move {
            let _ = second_started_send.send(());
            second_budget.reserve(3).await
        });
        second_started_receive.await.unwrap();
        wait_until(|| budget.objects.available_permits() == 0).await;
        assert!(!second.is_finished(), "byte budget did not block producer");

        let third_budget = budget.clone();
        let (third_started_send, third_started_receive) = oneshot::channel();
        let third = tokio::spawn(async move {
            let _ = third_started_send.send(());
            third_budget.reserve(1).await
        });
        third_started_receive.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!third.is_finished(), "third object exceeded object budget");

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("byte waiter did not progress")
            .unwrap()
            .unwrap();
        let third = tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("object waiter did not progress")
            .unwrap()
            .unwrap();
        drop(second);
        drop(third);

        let exact = budget.reserve(5).await.unwrap();
        drop(exact);
        assert_eq!(budget.bytes.available_permits(), 5);
        assert_eq!(budget.objects.available_permits(), 2);
    }

    #[tokio::test]
    async fn buffer_budget_rejects_an_oversized_object_without_consuming_capacity() {
        let budget = BufferBudget::new(4, 1);

        let error = budget.reserve(5).await.expect_err("reserve should fail");
        assert!(error.to_string().contains("exceeds buffer byte capacity"));
        assert_eq!(budget.bytes.available_permits(), 4);
        assert_eq!(budget.objects.available_permits(), 1);
    }

    #[tokio::test]
    async fn collect_live_blocks_the_33rd_object_then_resumes_after_long_fetch() {
        let fetch_budget = BufferBudget::new(4, 1);
        let fetch = fetch_budget.reserve(4).await.unwrap();
        let live_budget = BufferBudget::new(LIVE_BUFFER_BYTES, LIVE_BUFFER_OBJECTS);
        let (send, mut receive) = mpsc::channel(LIVE_BUFFER_OBJECTS);
        let total = LIVE_BUFFER_OBJECTS + 8;
        let (writer, reader) = Track::new(
            TrackNamespace::from_utf8_path("test/backpressure"),
            "video.m4s",
        )
        .produce();
        let mut groups = writer.subgroups().unwrap();
        let mut group = groups.append(127).unwrap();
        let (started_send, started_receive) = oneshot::channel();
        let collector_budget = live_budget.clone();
        let collector = tokio::spawn(async move {
            Media::<tokio::io::Sink>::collect_live(reader, send, collector_budget, started_send)
                .await
        });
        started_receive.await.unwrap();

        for value in 0..total {
            group
                .write(Bytes::from(vec![u8::try_from(value).unwrap()]))
                .unwrap();
        }

        wait_until(|| {
            receive.len() == LIVE_BUFFER_OBJECTS && live_budget.objects.available_permits() == 0
        })
        .await;
        assert_eq!(fetch_budget.bytes.available_permits(), 0);
        assert_eq!(live_budget.objects.available_permits(), 0);

        let mut values = Vec::with_capacity(total);
        for _ in 0..total {
            let object = tokio::time::timeout(Duration::from_secs(1), receive.recv())
                .await
                .expect("live producer did not resume")
                .expect("live buffer closed early");
            values.push(object.payload[0]);
        }
        values.sort_unstable();
        assert_eq!(
            values,
            (0..u8::try_from(total).unwrap()).collect::<Vec<_>>()
        );

        drop(group);
        drop(groups);
        tokio::time::timeout(Duration::from_secs(1), collector)
            .await
            .expect("live collector did not finish")
            .unwrap()
            .unwrap();
        assert!(receive.recv().await.is_none());
        drop(fetch);
        assert_eq!(fetch_budget.bytes.available_permits(), 4);
        assert_eq!(live_budget.bytes.available_permits(), LIVE_BUFFER_BYTES);
        assert_eq!(live_budget.objects.available_permits(), LIVE_BUFFER_OBJECTS);

        let continued = live_budget.reserve(LIVE_BUFFER_BYTES).await.unwrap();
        drop(continued);
    }

    #[tokio::test]
    async fn output_failure_closes_budget_and_wakes_blocked_producer() {
        let budget = BufferBudget::new(1, 1);
        let (send, receive) = mpsc::channel(1);
        let first = budget.reserve(1).await.unwrap();
        send.send(BufferedObject {
            payload: vec![1],
            _permit: first,
        })
        .await
        .unwrap();
        drop(send);

        let waiting_budget = budget.clone();
        let (waiting_send, waiting_receive) = oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _ = waiting_send.send(());
            waiting_budget.reserve(1).await
        });
        waiting_receive.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let output = Arc::new(Mutex::new(FailingWriter));
        let mut collectors = JoinSet::new();
        let error =
            Media::<FailingWriter>::drain_live(receive, &mut collectors, output, budget.clone())
                .await
                .expect_err("output should fail");
        assert!(error
            .to_string()
            .contains("failed to write live object payload"));

        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("budget waiter did not wake")
            .unwrap()
            .expect_err("closed budget should fail");
        assert!(error.to_string().contains("buffer object budget closed"));
        assert!(budget.bytes.is_closed());
        assert!(budget.objects.is_closed());
    }

    #[tokio::test]
    async fn fetch_gate_propagates_live_collector_errors() {
        let mut fetches = JoinSet::new();
        fetches.spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let mut collectors = JoinSet::new();
        collectors.spawn(async { anyhow::bail!("collector failed") });
        let fetch_budget = BufferBudget::new(1, 1);
        let live_budget = BufferBudget::new(1, 1);

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            Media::<tokio::io::Sink>::wait_for_fetches(
                &mut fetches,
                &mut collectors,
                &fetch_budget,
                &live_budget,
            ),
        )
        .await
        .expect("collector failure did not cross FETCH gate")
        .expect_err("collector failure should cross the FETCH gate");

        assert_eq!(error.to_string(), "collector failed");
        assert!(fetch_budget.bytes.is_closed());
        assert!(live_budget.bytes.is_closed());
    }

    #[tokio::test]
    async fn permanently_pending_write_is_aborted_after_sibling_failure() {
        let output = Arc::new(Mutex::new(PendingWriter));
        let writer_output = output.clone();
        let (started_send, started_receive) = oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut output = writer_output.lock().await;
            let _ = started_send.send(());
            output
                .write_all(b"pending object")
                .await
                .context("pending write failed")
        });
        started_receive.await.unwrap();
        tasks.spawn(async { anyhow::bail!("sibling failed") });

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            Media::<PendingWriter>::wait_for_tasks(&mut tasks, "test", &[]),
        )
        .await
        .expect("pending writer was not aborted")
        .expect_err("sibling error should propagate");
        assert_eq!(error.to_string(), "sibling failed");
        assert!(
            output.try_lock().is_ok(),
            "aborted writer retained output lock"
        );
    }
}
