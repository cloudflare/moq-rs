// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bounded history/live playback for the single-subgroup CMAF media profile.

use std::{sync::Arc, time::Duration};

use anyhow::{ensure, Context};
use moq_transport::{
    coding::{KeyValuePairs, Location, TrackName, TrackNamespace, VarInt},
    data::{FetchRecord, ObjectStatus},
    message::{FetchType, GroupOrder, StandaloneFetch, SubscriptionFilter},
    serve::{Track, TrackReader, TrackReaderMode},
    session::{Fetch, Subscribe, Subscriber},
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{mpsc, Barrier, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBJECTS: usize = 32;
const TIMEOUT: Duration = Duration::from_secs(10);

/// The first group of a Joining FETCH. Relative offsets count groups before
/// the subscription's Joining Location; zero fetches the boundary group only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoiningStart {
    Relative(u64),
    Absolute(u64),
}

/// Fetch a finite range, or replay history and continue the live subscription.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchMode {
    Standalone { start: Location, end: Location },
    Joining(JoiningStart),
}

struct BufferedObject {
    location: Location,
    payload: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}

/// Ordered, duplicate-free timeline for moq-pub's consecutive group/Object IDs.
#[derive(Default)]
struct Timeline {
    last: Option<Location>,
    first: Option<Location>,
}

impl Timeline {
    fn accept(&mut self, location: Location) -> anyhow::Result<bool> {
        if let Some(first) = self.first {
            ensure!(
                location == first,
                "FETCH is missing its requested first media Object"
            );
            self.first = None;
        }
        if let Some(last) = self.last {
            if location <= last {
                return Ok(false);
            }
            ensure!(
                (location.group_id == last.group_id
                    && last.object_id.checked_add(1) == Some(location.object_id))
                    || (last.group_id.checked_add(1) == Some(location.group_id)
                        && location.object_id == 0),
                "missing media Object at history/live boundary"
            );
        }
        self.last = Some(location);
        Ok(true)
    }
}

async fn open_join(
    subscriber: &mut Subscriber,
    namespace: TrackNamespace,
    name: TrackName,
) -> anyhow::Result<(Subscribe, TrackReader, Location)> {
    let mut params = KeyValuePairs::default();
    params.set_group_order(GroupOrder::Ascending);
    params.set_subscription_filter(&SubscriptionFilter {
        filter_type: moq_transport::message::FilterType::LargestObject,
        start_location: None,
        end_group_id: None,
    })?;
    let (writer, mut reader) = Track::new(namespace.clone(), name.clone()).produce();
    let mut subscription = subscriber
        .subscribe_open_with_params(writer, params.clone())
        .await?;
    if subscription
        .subscribe_ok()
        .context("missing SUBSCRIBE_OK")?
        .params
        .largest_object()?
        .is_none()
    {
        // A cold pull-through relay acknowledges before its upstream has data.
        // Prime its cache, discard the warm-up, then obtain a joinable handle.
        let TrackReaderMode::Subgroups(mut groups) = reader.mode().await? else {
            anyhow::bail!("expected CMAF subgroups");
        };
        let mut group = groups.next().await?.context("no warm-up group")?;
        let mut object = group.next().await?.context("no warm-up Object")?;
        ensure!(object.size <= MAX_BYTES, "oversized warm-up Object");
        object.read_all().await?;
        drop(object);
        drop(group);
        drop(groups);
        subscription.unsubscribe().await;
        let (writer, fresh) = Track::new(namespace, name).produce();
        reader = fresh;
        subscription = subscriber
            .subscribe_open_with_params(writer, params)
            .await?;
    }
    let joining = subscription
        .subscribe_ok()
        .context("missing SUBSCRIBE_OK")?
        .params
        .largest_object()?
        .context("track has no Joining Location")?;
    Ok((subscription, reader, joining))
}

async fn collect_live(
    reader: TrackReader,
    send: mpsc::Sender<anyhow::Result<BufferedObject>>,
    joining: Location,
) -> anyhow::Result<()> {
    let budget = Arc::new(Semaphore::new(MAX_BYTES));
    let TrackReaderMode::Subgroups(mut groups) = reader.mode().await? else {
        anyhow::bail!("expected CMAF subgroups");
    };
    while let Some(mut group) = groups.next().await? {
        ensure!(
            group.subgroup_id == 0,
            "expected one CMAF subgroup per group"
        );
        while let Some(mut object) = group.next().await? {
            let location = Location::new(group.group_id, object.object_id);
            if location <= joining {
                continue;
            }
            ensure!(
                object.status == ObjectStatus::NormalObject,
                "unavailable live media Object"
            );
            ensure!(object.size <= MAX_BYTES, "oversized live media Object");
            let permit = budget
                .clone()
                .try_acquire_many_owned(object.size as u32)
                .context("live media byte buffer exhausted")?;
            let payload = object.read_all().await?.to_vec();
            send.try_send(Ok(BufferedObject {
                location,
                payload,
                _permit: permit,
            }))
            .map_err(|_| anyhow::anyhow!("live media Object buffer exhausted"))?;
        }
    }
    Ok(())
}

async fn drain_fetch<O: AsyncWrite + Unpin>(
    mut fetch: Fetch,
    out: &Arc<Mutex<O>>,
    timeline: &mut Timeline,
    expected: Option<Location>,
) -> anyhow::Result<()> {
    let ok = fetch
        .ok()
        .await
        .map_err(|err| anyhow::anyhow!("FETCH rejected: {:?}: {err}", fetch.rejection()))?;
    if let Some(joining) = expected {
        let inclusive = Location::new(
            ok.end_location.group_id,
            ok.end_location
                .object_id
                .checked_sub(1)
                .unwrap_or(VarInt::MAX.into_inner()),
        );
        ensure!(
            inclusive == joining,
            "FETCH did not reach the Joining Location"
        );
    }
    while let Some(record) = fetch.next().await? {
        let FetchRecord::Object(object) = record else {
            anyhow::bail!("FETCH contains unavailable media Objects");
        };
        ensure!(
            object.subgroup_id == Some(0) && object.payload_length <= MAX_BYTES as u64,
            "invalid CMAF FETCH Object"
        );
        let emit = timeline.accept(object.location())?;
        let mut payload = Vec::new();
        while let Some(chunk) = fetch.read_payload_chunk(64 * 1024).await? {
            payload.extend_from_slice(&chunk);
        }
        ensure!(
            payload.len() as u64 == object.payload_length,
            "truncated FETCH payload"
        );
        if emit {
            out.lock().await.write_all(&payload).await?;
        }
    }
    if let Some(joining) = expected {
        ensure!(
            timeline.last == Some(joining),
            "FETCH ended before its advertised boundary"
        );
    }
    Ok(())
}

pub(crate) async fn play<O: AsyncWrite + Send + Unpin + 'static>(
    mut subscriber: Subscriber,
    namespace: TrackNamespace,
    name: TrackName,
    out: Arc<Mutex<O>>,
    mode: FetchMode,
    history_complete: Arc<Barrier>,
) -> anyhow::Result<()> {
    let mut timeline = Timeline::default();
    match mode {
        FetchMode::Standalone { start, end } => {
            let mut params = KeyValuePairs::default();
            params.set_group_order(GroupOrder::Ascending);
            let history = async {
                let fetch = subscriber
                    .fetch(
                        StandaloneFetch {
                            track_namespace: namespace,
                            track_name: name,
                            start_location: start,
                            end_location: end,
                        },
                        params,
                    )
                    .await?;
                drain_fetch(fetch, &out, &mut timeline, None).await
            };
            tokio::time::timeout(TIMEOUT, history)
                .await
                .context("standalone FETCH timed out")??;
        }
        FetchMode::Joining(start) => {
            let (subscription, reader, joining) =
                tokio::time::timeout(TIMEOUT, open_join(&mut subscriber, namespace, name))
                    .await
                    .context("SUBSCRIBE warm-up timed out")??;
            let (send, mut receive) = mpsc::channel(MAX_OBJECTS);
            let mut collector = JoinSet::new();
            collector.spawn(async move {
                let result = collect_live(reader, send.clone(), joining).await;
                if let Err(err) = result {
                    let _ = send.send(Err(err)).await;
                }
            });
            let (kind, first, first_group) = match start {
                JoiningStart::Relative(groups) => (
                    FetchType::RelativeJoining,
                    groups.min(joining.group_id),
                    joining.group_id.saturating_sub(groups),
                ),
                JoiningStart::Absolute(group) => (FetchType::AbsoluteJoining, group, group),
            };
            let history = async {
                let mut params = KeyValuePairs::default();
                params.set_group_order(GroupOrder::Ascending);
                let fetch = subscriber
                    .joining_fetch(kind, subscription.id, first, params)
                    .await?;
                drain_fetch_joined(fetch, &out, &mut timeline, joining, first_group).await
            };
            tokio::time::timeout(TIMEOUT, history)
                .await
                .context("Joining FETCH timed out")??;
            history_complete.wait().await;
            let live = async {
                while let Some(object) = receive.recv().await {
                    let object = object?;
                    if timeline.accept(object.location)? {
                        out.lock().await.write_all(&object.payload).await?;
                    }
                }
                if let Some(result) = collector.join_next().await {
                    result.context("live collector panicked")?;
                }
                Ok::<_, anyhow::Error>(())
            };
            tokio::pin!(live);
            tokio::select! {
                biased;
                result = &mut live => result?,
                result = subscription.closed() => {
                    result.context("media subscription ended")?;
                    // A clean PUBLISH_DONE means the wire streams arrived,
                    // not that the application consumed its buffered Objects.
                    live.await?;
                }
            }
        }
    }
    out.lock().await.flush().await?;
    Ok(())
}

async fn drain_fetch_joined<O: AsyncWrite + Unpin>(
    fetch: Fetch,
    out: &Arc<Mutex<O>>,
    timeline: &mut Timeline,
    joining: Location,
    first_group: u64,
) -> anyhow::Result<()> {
    timeline.first = Some(Location::new(first_group, 0));
    drain_fetch(fetch, out, timeline, Some(joining)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handoff_skips_overlap_and_rejects_missing_objects_or_groups() {
        let mut timeline = Timeline::default();
        let mut emitted = Vec::new();
        for (group, object) in [
            (4, 0),
            (4, 1),
            (5, 0),
            (5, 1),
            (5, 0),
            (5, 1),
            (5, 2),
            (6, 0),
        ] {
            let location = Location::new(group, object);
            if timeline.accept(location).unwrap() {
                emitted.push(location);
            }
        }
        assert_eq!(emitted.len(), 6);
        assert!(timeline.accept(Location::new(6, 2)).is_err());
        assert!(timeline.accept(Location::new(8, 0)).is_err());
    }

    #[test]
    fn joining_requires_the_requested_first_object_even_at_group_zero() {
        for group in [0, 7] {
            let mut timeline = Timeline {
                first: Some(Location::new(group, 0)),
                ..Default::default()
            };
            assert!(timeline.accept(Location::new(group, 1)).is_err());
            assert!(timeline.accept(Location::new(group + 1, 0)).is_err());
            assert!(timeline.accept(Location::new(group, 0)).unwrap());
        }
    }

    #[tokio::test]
    async fn live_collector_bounds_objects_and_discards_history_overlap() {
        let namespace = TrackNamespace::from_utf8_path("test");
        let (writer, reader) = Track::new(namespace, "1.m4s").produce();
        let mut groups = writer.subgroups().unwrap();
        let mut group = groups.append(127).unwrap();
        for _ in 0..MAX_OBJECTS + 2 {
            group.write(vec![1].into()).unwrap();
        }
        let (send, mut receive) = mpsc::channel(MAX_OBJECTS);
        // Object zero is the boundary; exactly 32 successors fit, the next
        // Object fails explicitly instead of silently dropping media.
        let result = collect_live(reader, send, Location::new(0, 0)).await;
        assert!(result.unwrap_err().to_string().contains("buffer exhausted"));
        assert_eq!(receive.len(), MAX_OBJECTS);
        assert_eq!(
            receive.recv().await.unwrap().unwrap().location,
            Location::new(0, 1)
        );
    }

    #[tokio::test]
    async fn live_collector_rejects_oversized_object_before_allocating_payload() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "1.m4s").produce();
        let mut groups = writer.subgroups().unwrap();
        let mut group = groups.append(127).unwrap();
        group.write(vec![0].into()).unwrap();
        let _pending = group.create(MAX_BYTES + 1, None).unwrap();
        let (send, _) = mpsc::channel(MAX_OBJECTS);
        let result = collect_live(reader, send, Location::new(0, 0)).await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("oversized live media Object"));
    }
}
