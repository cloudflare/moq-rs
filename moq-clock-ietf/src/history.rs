// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bounded, immutable snapshots of the clock's single-subgroup history.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Context};
use moq_transport::{
    coding::{KeyValuePairs, Location, TrackName, TrackNamespace},
    data::{FetchRecord, FetchRecordObject},
    message::{GroupOrder, RequestErrorCode, StandaloneFetch},
    reexports::bytes::Bytes,
    session::{FetchOkInfo, FetchRequested, Publisher},
};
use tokio::task::JoinSet;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_OBJECTS_PER_GROUP: usize = 64;
const MAX_RESPONSES: usize = 8;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct History {
    inner: Arc<Mutex<HistoryState>>,
}

struct HistoryState {
    groups: BTreeMap<u64, Vec<Bytes>>,
    max_groups: usize,
    max_bytes: usize,
    bytes: usize,
}

impl History {
    pub fn new(max_groups: usize) -> Self {
        Self::with_limits(max_groups, MAX_BYTES)
    }

    fn with_limits(max_groups: usize, max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HistoryState {
                groups: BTreeMap::new(),
                max_groups,
                max_bytes,
                bytes: 0,
            })),
        }
    }

    /// Record before exposing the Object to live readers, so SUBSCRIBE_OK can
    /// never advertise a Joining Location we have not retained yet.
    pub fn push(&self, location: Location, payload: Bytes) -> anyhow::Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("history lock poisoned"))?;
        if state.max_groups == 0 || payload.len() > state.max_bytes {
            bail!("clock object exceeds history limits");
        }
        if state
            .groups
            .last_key_value()
            .is_some_and(|(group, _)| *group > location.group_id)
        {
            bail!("clock groups must increase");
        }
        let objects = state.groups.get(&location.group_id).map_or(0, Vec::len);
        if location.object_id != objects as u64 || objects >= MAX_OBJECTS_PER_GROUP {
            bail!("clock objects must be consecutive and bounded");
        }
        // Evict whole groups, preserving the base Object of every retained
        // minute. Never keep an undecodable tail of the current group.
        while state.groups.len() + usize::from(objects == 0) > state.max_groups
            || state.bytes + payload.len() > state.max_bytes
        {
            if state.groups.len() == 1 && state.groups.contains_key(&location.group_id) {
                bail!("current clock group exceeds history byte limit");
            }
            let (_, removed) = state
                .groups
                .pop_first()
                .context("history limit is too small")?;
            state.bytes -= removed.iter().map(Bytes::len).sum::<usize>();
        }
        state.bytes += payload.len();
        state
            .groups
            .entry(location.group_id)
            .or_default()
            .push(payload);
        Ok(())
    }

    /// Reject unavailable prefixes explicitly rather than quietly presenting a
    /// partial join as gap-free. Snapshots share immutable payload allocations;
    /// a later eviction cannot change an in-flight FETCH.
    fn snapshot(
        &self,
        range: &StandaloneFetch,
        descending: bool,
    ) -> anyhow::Result<Vec<(Location, Bytes)>> {
        let state = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("history lock poisoned"))?;
        let first = state
            .groups
            .first_key_value()
            .context("clock history is empty")?
            .0;
        if range.start_location.group_id < *first {
            bail!("requested clock history is no longer retained");
        }
        let end = inclusive_end(range.end_location);
        let mut result = Vec::new();
        for (&group, objects) in state
            .groups
            .range(range.start_location.group_id..=end.group_id)
        {
            let selected = objects
                .iter()
                .enumerate()
                .filter_map(|(object, bytes)| {
                    let location = Location::new(group, object as u64);
                    (location >= range.start_location && location <= end)
                        .then(|| (location, bytes.clone()))
                })
                .collect::<Vec<_>>();
            result.push(selected);
        }
        if descending {
            result.reverse();
        }
        let result = result.into_iter().flatten().collect::<Vec<_>>();
        if result.is_empty() {
            bail!("no retained clock objects in range");
        }
        Ok(result)
    }

    async fn serve(
        &self,
        request: FetchRequested,
        namespace: &TrackNamespace,
        track: &TrackName,
    ) -> anyhow::Result<()> {
        let Some(range) = request.resolve().await? else {
            return Ok(());
        };
        if &range.track_namespace != namespace || &range.track_name != track {
            request.reject(RequestErrorCode::DoesNotExist, "unknown clock track")?;
            return Ok(());
        }
        let descending = request.group_order() == Some(GroupOrder::Descending);
        let snapshot = match self.snapshot(&range, descending) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                request.reject(
                    RequestErrorCode::DoesNotExist,
                    "requested clock history is not retained",
                )?;
                return Ok(());
            }
        };
        let last = snapshot
            .iter()
            .map(|(location, _)| *location)
            .max()
            .context("empty history snapshot")?;
        let mut writer = request
            .respond(FetchOkInfo {
                end_of_track: false,
                end_location: end_after(last),
                params: KeyValuePairs::default(),
                track_extensions: Default::default(),
            })
            .await?;
        for (location, payload) in snapshot {
            writer
                .write_record(&FetchRecord::Object(FetchRecordObject {
                    group_id: location.group_id,
                    subgroup_id: Some(0),
                    object_id: location.object_id,
                    publisher_priority: 0,
                    extension_headers: Default::default(),
                    payload_length: payload.len() as u64,
                }))
                .await?;
            writer.write_payload(&payload).await?;
        }
        writer.finish().await?;
        Ok(())
    }

    pub async fn run(
        self,
        mut publisher: Publisher,
        namespace: TrackNamespace,
        track: TrackName,
    ) -> anyhow::Result<()> {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                request = publisher.fetch_requested(), if tasks.len() < MAX_RESPONSES => {
                    let Some(request) = request else { return Ok(()); };
                    let history = self.clone();
                    let namespace = namespace.clone();
                    let track = track.clone();
                    tasks.spawn(async move {
                        match tokio::time::timeout(RESPONSE_TIMEOUT, history.serve(request, &namespace, &track)).await {
                            Ok(Ok(())) => {},
                            Ok(Err(err)) => tracing::debug!(%err, "clock FETCH ended"),
                            Err(_) => tracing::debug!("clock FETCH timed out"),
                        }
                    });
                }
                result = tasks.join_next(), if !tasks.is_empty() => { result.context("missing FETCH task")??; }
            }
        }
    }
}

pub(crate) fn inclusive_end(end: Location) -> Location {
    Location::new(
        end.group_id,
        end.object_id.checked_sub(1).unwrap_or(u64::MAX),
    )
}

pub(crate) fn end_after(last: Location) -> Location {
    Location::new(last.group_id, last.object_id.checked_add(1).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: Location, end: Location) -> StandaloneFetch {
        StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path("clock"),
            track_name: "now".into(),
            start_location: start,
            end_location: end,
        }
    }

    #[test]
    fn bounds_evict_whole_groups_and_snapshots_survive_eviction() {
        let history = History::with_limits(2, 6);
        history
            .push(Location::new(10, 0), Bytes::from_static(b"ab"))
            .unwrap();
        history
            .push(Location::new(10, 1), Bytes::from_static(b"cd"))
            .unwrap();
        let snapshot = history
            .snapshot(&range(Location::new(10, 0), Location::new(10, 0)), false)
            .unwrap();
        history
            .push(Location::new(11, 0), Bytes::from_static(b"ef"))
            .unwrap();
        history
            .push(Location::new(12, 0), Bytes::from_static(b"gh"))
            .unwrap();
        let state = history.inner.lock().unwrap();
        assert_eq!(state.groups.keys().copied().collect::<Vec<_>>(), [11, 12]);
        assert_eq!(state.bytes, 4);
        drop(state);
        assert_eq!(snapshot[1].1, b"cd"[..]);
        assert!(history
            .snapshot(&range(Location::new(10, 0), Location::new(12, 0)), false)
            .is_err());
    }

    #[test]
    fn byte_limit_and_object_limit_cannot_drop_current_base() {
        let history = History::with_limits(8, 3);
        history
            .push(Location::new(1, 0), Bytes::from_static(b"ab"))
            .unwrap();
        assert!(history
            .push(Location::new(1, 1), Bytes::from_static(b"cd"))
            .is_err());
        assert_eq!(history.inner.lock().unwrap().bytes, 2);
        let history = History::new(2);
        for object in 0..MAX_OBJECTS_PER_GROUP {
            history
                .push(Location::new(1, object as u64), Bytes::new())
                .unwrap();
        }
        assert!(history
            .push(Location::new(1, MAX_OBJECTS_PER_GROUP as u64), Bytes::new())
            .is_err());
    }

    #[test]
    fn range_end_is_exclusive_except_zero_and_descending_keeps_object_order() {
        let history = History::new(4);
        for group in 3..6 {
            for object in 0..3 {
                history
                    .push(Location::new(group, object), Bytes::from_static(b"x"))
                    .unwrap();
            }
        }
        let selected = history
            .snapshot(&range(Location::new(3, 1), Location::new(4, 2)), true)
            .unwrap();
        assert_eq!(
            selected.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
            [
                Location::new(4, 0),
                Location::new(4, 1),
                Location::new(3, 1),
                Location::new(3, 2)
            ]
        );
        assert_eq!(
            inclusive_end(Location::new(4, 0)),
            Location::new(4, u64::MAX)
        );
        assert_eq!(end_after(Location::new(4, u64::MAX)), Location::new(4, 0));
    }
}
