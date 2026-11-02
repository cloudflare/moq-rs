// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Publisher-owned, bounded snapshots for draft-18 standalone and Joining FETCH.

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{ensure, Context};
use bytes::Bytes;
use moq_transport::{
    coding::{Location, VarInt},
    data::{FetchRecord, FetchRecordObject},
    message::{GroupOrder, RequestErrorCode, StandaloneFetch},
    serve::{FullTrackName, SubgroupWriter},
    session::{FetchOkInfo, FetchRequested, Publisher},
};
use tokio::task::JoinSet;

const MAX_TRACKS: usize = 64;
const MAX_RESPONSES: usize = 8;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Retains complete Objects, evicting whole groups so a retained CMAF group
/// always starts at its first Object. Clones share history; response snapshots
/// share immutable payloads and survive later eviction.
#[derive(Clone)]
pub struct FetchHistory {
    inner: Arc<Mutex<History>>,
    groups: usize,
    bytes: usize,
    objects: usize,
}

#[derive(Default)]
struct History {
    tracks: HashMap<FullTrackName, TrackHistory>,
}

#[derive(Default)]
struct TrackHistory {
    groups: VecDeque<Vec<ArchivedObject>>,
    bytes: usize,
    objects: usize,
    evicted: Option<u64>,
}

#[derive(Clone)]
struct ArchivedObject {
    location: Location,
    subgroup: u64,
    priority: u8,
    payload: Bytes,
}

impl FetchHistory {
    /// Retain at most `capacity` groups per track, additionally bounded to
    /// 16 MiB and 4096 Objects per track. At most 64 tracks can be retained.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Default::default(),
            groups: capacity.get(),
            bytes: 16 * 1024 * 1024,
            objects: 4096,
        }
    }

    /// Set group, payload-byte and Object limits per track. A current group
    /// that cannot fit is an error, rather than retaining an undecodable tail.
    pub fn with_limits(groups: NonZeroUsize, bytes: NonZeroUsize, objects: NonZeroUsize) -> Self {
        Self {
            inner: Default::default(),
            groups: groups.get(),
            bytes: bytes.get(),
            objects: objects.get(),
        }
    }

    /// The lock spans the live write and archive insertion. A FETCH resolving
    /// an advertised Joining Location cannot observe a half-committed Object.
    pub(crate) fn write(
        &self,
        subgroup: &mut SubgroupWriter,
        payload: Bytes,
    ) -> anyhow::Result<()> {
        let name = FullTrackName {
            namespace: subgroup.track.namespace.clone(),
            name: subgroup.track.name.clone(),
        };
        let object = ArchivedObject {
            location: Location::new(subgroup.group_id, subgroup.len() as u64),
            subgroup: subgroup.subgroup_id,
            priority: subgroup.priority,
            payload,
        };
        self.insert(name, object.clone(), || {
            subgroup.write(object.payload.clone()).map_err(Into::into)
        })
    }

    fn insert(
        &self,
        name: FullTrackName,
        object: ArchivedObject,
        write: impl FnOnce() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("FETCH history lock poisoned"))?;
        ensure!(
            state.tracks.contains_key(&name) || state.tracks.len() < MAX_TRACKS,
            "too many retained tracks"
        );
        let track = state.tracks.entry(name).or_default();
        let group = object.location.group_id;
        let last = track.groups.back().and_then(|g| g.last());
        ensure!(
            last.is_none_or(|last| last.location < object.location),
            "history Objects must increase"
        );
        let same_group = last.is_some_and(|last| last.location.group_id == group);
        let current_bytes = if same_group {
            track
                .groups
                .back()
                .map_or(0, |g| g.iter().map(|o| o.payload.len()).sum())
        } else {
            0
        };
        let current_objects = if same_group {
            track.groups.back().map_or(0, Vec::len)
        } else {
            0
        };
        ensure!(
            object.payload.len() <= self.bytes.saturating_sub(current_bytes)
                && current_objects < self.objects,
            "current group exceeds FETCH history limits"
        );
        // Do not mutate retained history if the live writer rejects this Object.
        write()?;
        while track.groups.len() + usize::from(!same_group) > self.groups
            || track.bytes > self.bytes - object.payload.len()
            || track.objects >= self.objects
        {
            let removed = track
                .groups
                .pop_front()
                .context("history limit is too small")?;
            track.bytes -= removed.iter().map(|o| o.payload.len()).sum::<usize>();
            track.objects -= removed.len();
            track.evicted = removed.last().map(|o| o.location.group_id);
        }
        if !same_group {
            track.groups.push_back(Vec::new());
        }
        track.bytes += object.payload.len();
        track.objects += 1;
        track
            .groups
            .back_mut()
            .context("missing history group")?
            .push(object);
        Ok(())
    }

    fn snapshot(
        &self,
        range: &StandaloneFetch,
        descending: bool,
    ) -> anyhow::Result<Vec<ArchivedObject>> {
        let state = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("FETCH history lock poisoned"))?;
        let name = FullTrackName {
            namespace: range.track_namespace.clone(),
            name: range.track_name.clone(),
        };
        let track = state.tracks.get(&name).context("track is not retained")?;
        // Equal non-zero bounds describe a valid empty range, even if the
        // group's Objects have since been evicted.
        if range.start_location == range.end_location && range.end_location.object_id != 0 {
            return Ok(Vec::new());
        }
        ensure!(
            track
                .evicted
                .is_none_or(|g| range.start_location.group_id > g),
            "requested history was evicted"
        );
        let end = inclusive_end(range.end_location);
        let mut groups = track
            .groups
            .iter()
            .map(|g| {
                g.iter()
                    .filter(|o| o.location >= range.start_location && o.location <= end)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if descending {
            groups.reverse();
        }
        let objects = groups.into_iter().flatten().collect::<Vec<_>>();
        ensure!(!objects.is_empty(), "no retained Objects in range");
        Ok(objects)
    }
}

fn inclusive_end(end: Location) -> Location {
    Location::new(
        end.group_id,
        end.object_id
            .checked_sub(1)
            .unwrap_or(VarInt::MAX.into_inner()),
    )
}

async fn serve(request: FetchRequested, history: Option<FetchHistory>) -> anyhow::Result<()> {
    let Some(history) = history else {
        request.reject(RequestErrorCode::NotSupported, "FETCH history is disabled")?;
        return Ok(());
    };
    let Some(range) = request.resolve().await? else {
        return Ok(());
    };
    let snapshot = match history.snapshot(
        &range,
        request.group_order() == Some(GroupOrder::Descending),
    ) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            request.reject(
                RequestErrorCode::DoesNotExist,
                "requested history is not retained",
            )?;
            return Ok(());
        }
    };
    let last = snapshot.iter().map(|o| o.location).max();
    let end_location = last.map_or(range.end_location, |last| {
        Location::new(
            last.group_id,
            if last.object_id == VarInt::MAX.into_inner() {
                0
            } else {
                last.object_id + 1
            },
        )
    });
    let mut writer = request
        .respond(FetchOkInfo {
            end_of_track: false,
            end_location,
            params: Default::default(),
            track_extensions: Default::default(),
        })
        .await?;
    for object in snapshot {
        writer
            .write_record(&FetchRecord::Object(FetchRecordObject {
                group_id: object.location.group_id,
                subgroup_id: Some(object.subgroup),
                object_id: object.location.object_id,
                publisher_priority: object.priority,
                extension_headers: Default::default(),
                payload_length: object.payload.len() as u64,
            }))
            .await?;
        writer.write_payload(&object.payload).await?;
    }
    writer.finish().await?;
    Ok(())
}

/// Opt in to FETCH handling and serve standalone/normalized Joining requests.
/// Drive this concurrently with the session and live namespace publication.
/// Eight bounded response tasks have ten-second deadlines; cancellation or
/// failure of one request does not stop live publishing or sibling FETCHes.
pub async fn serve_fetches(
    mut publisher: Publisher,
    history: Option<FetchHistory>,
) -> anyhow::Result<()> {
    publisher.accept_fetch();
    let mut tasks = JoinSet::new();
    let mut accepting = true;
    loop {
        tokio::select! {
            request = publisher.fetch_requested(), if accepting && tasks.len() < MAX_RESPONSES => {
                match request {
                    Some(request) => {
                        let history = history.clone();
                        tasks.spawn(async move { tokio::time::timeout(RESPONSE_TIMEOUT, serve(request, history)).await });
                    }
                    None => accepting = false,
                }
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                if !matches!(result, Some(Ok(Ok(Ok(()))))) { tracing::debug!(?result, "FETCH response ended"); }
            }
            else => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::coding::TrackNamespace;

    fn name() -> FullTrackName {
        FullTrackName {
            namespace: TrackNamespace::from_utf8_path("test"),
            name: "1.m4s".into(),
        }
    }
    fn object(group: u64, object: u64, size: usize) -> ArchivedObject {
        ArchivedObject {
            location: Location::new(group, object),
            subgroup: 0,
            priority: 127,
            payload: vec![42; size].into(),
        }
    }
    fn range(start: u64, end: u64) -> StandaloneFetch {
        StandaloneFetch {
            track_namespace: name().namespace,
            track_name: name().name,
            start_location: Location::new(start, 0),
            end_location: Location::new(end, 0),
        }
    }

    #[test]
    fn bounds_evict_whole_groups_and_snapshot_survives() {
        let history = FetchHistory::with_limits(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(6).unwrap(),
            NonZeroUsize::new(3).unwrap(),
        );
        history.insert(name(), object(0, 0, 2), || Ok(())).unwrap();
        history.insert(name(), object(0, 1, 2), || Ok(())).unwrap();
        let snapshot = history.snapshot(&range(0, 0), false).unwrap();
        history.insert(name(), object(1, 0, 2), || Ok(())).unwrap();
        history.insert(name(), object(2, 0, 2), || Ok(())).unwrap();
        assert!(history.snapshot(&range(0, 2), false).is_err());
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[1].payload.len(), 2);
        let selected = history.snapshot(&range(1, 2), true).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|o| o.location.group_id)
                .collect::<Vec<_>>(),
            [2, 1]
        );
    }

    #[test]
    fn current_group_limits_and_failed_writes_preserve_history() {
        let history = FetchHistory::with_limits(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        history.insert(name(), object(0, 0, 2), || Ok(())).unwrap();
        assert!(history.insert(name(), object(0, 1, 3), || Ok(())).is_err());
        assert!(history
            .insert(name(), object(1, 0, 2), || anyhow::bail!(
                "live write failed"
            ))
            .is_err());
        history.insert(name(), object(0, 1, 2), || Ok(())).unwrap();
        assert!(history.insert(name(), object(0, 2, 0), || Ok(())).is_err());
        assert_eq!(history.snapshot(&range(0, 1), false).unwrap().len(), 2);
    }

    #[test]
    fn each_limit_independently_evicts_only_whole_groups() {
        for (groups, bytes, objects) in [(2, 100, 100), (100, 4, 100), (100, 100, 2)] {
            let history = FetchHistory::with_limits(
                NonZeroUsize::new(groups).unwrap(),
                NonZeroUsize::new(bytes).unwrap(),
                NonZeroUsize::new(objects).unwrap(),
            );
            for group in 0..3 {
                history
                    .insert(name(), object(group, 0, 2), || Ok(()))
                    .unwrap();
            }
            assert!(history.snapshot(&range(0, 2), false).is_err());
            assert_eq!(history.snapshot(&range(1, 2), false).unwrap().len(), 2);
        }
    }

    #[test]
    fn range_end_and_descending_group_order_preserve_object_order() {
        let history = FetchHistory::new(NonZeroUsize::new(3).unwrap());
        for group in 0..2 {
            for id in 0..3 {
                history
                    .insert(name(), object(group, id, 1), || Ok(()))
                    .unwrap();
            }
        }
        let mut range = range(0, 1);
        range.start_location.object_id = 1;
        range.end_location.object_id = 2;
        let snapshot = history.snapshot(&range, true).unwrap();
        assert_eq!(
            snapshot.iter().map(|o| o.location).collect::<Vec<_>>(),
            [
                Location::new(1, 0),
                Location::new(1, 1),
                Location::new(0, 1),
                Location::new(0, 2)
            ]
        );
        range.start_location = range.end_location;
        assert!(history.snapshot(&range, false).unwrap().is_empty());
    }
}
