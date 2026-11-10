// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Objects and groups a session carries, for usage reporting.
//!
//! An object is counted as it crosses the session on a subgroup stream, as a
//! datagram, or as a FETCH record, including one relayed by
//! [`super::FetchRequested::proxy`]. A group is counted when its first object
//! crosses, so a group split across several subgroup streams counts once.
//! Datagrams are counted on receipt, before they are delivered, deferred or
//! dropped.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
};

/// Tracks whose recent groups are remembered per direction. Past this, the
/// least recently used track is forgotten, so a group in flight on that track
/// can be counted twice. Bounding a long-lived session's memory matters more.
const MAX_TRACKED_TRACKS: usize = 4096;

/// Groups below a track's newest that are still told apart. A group whose
/// first object arrives more than this far behind the newest is not counted:
/// counting it could count twice a group whose mark has left the window.
const GROUP_WINDOW: u64 = u64::BITS as u64;

/// Which recent groups of one track have been seen.
#[derive(Debug)]
struct TrackGroups {
    newest: u64,
    /// Bit `i` is set once group `newest - i` has been seen.
    seen: u64,
    /// When the track was last used, for least-recently-used eviction.
    last_used: u64,
}

impl TrackGroups {
    fn new(group_id: u64, now: u64) -> Self {
        Self {
            newest: group_id,
            seen: 1,
            last_used: now,
        }
    }

    /// Mark `group_id` seen, returning whether it is new.
    fn mark(&mut self, group_id: u64) -> bool {
        if group_id > self.newest {
            let shift = group_id - self.newest;
            self.seen = if shift >= GROUP_WINDOW {
                0
            } else {
                self.seen << shift
            };
            self.seen |= 1;
            self.newest = group_id;
            return true;
        }
        let age = self.newest - group_id;
        if age >= GROUP_WINDOW {
            return false;
        }
        let bit = 1 << age;
        let new = self.seen & bit == 0;
        self.seen |= bit;
        new
    }
}

/// Recent groups per track alias, bounded by [`MAX_TRACKED_TRACKS`].
#[derive(Debug, Default)]
struct GroupWindows {
    tracks: HashMap<u64, TrackGroups>,
    /// Logical clock for least-recently-used eviction.
    clock: u64,
}

impl GroupWindows {
    /// Mark `group_id` seen on `track_alias`, returning whether it is new.
    fn first_sight(&mut self, track_alias: u64, group_id: u64) -> bool {
        self.clock += 1;
        let now = self.clock;
        if let Some(track) = self.tracks.get_mut(&track_alias) {
            track.last_used = now;
            return track.mark(group_id);
        }
        if self.tracks.len() >= MAX_TRACKED_TRACKS {
            // Runs only when a new track arrives at the cap.
            if let Some(oldest) = self
                .tracks
                .iter()
                .min_by_key(|(_, track)| track.last_used)
                .map(|(alias, _)| *alias)
            {
                self.tracks.remove(&oldest);
            }
        }
        self.tracks
            .insert(track_alias, TrackGroups::new(group_id, now));
        true
    }
}

/// Counts the objects and groups crossing a session in one direction.
#[derive(Debug, Default)]
pub(crate) struct TrafficCounter {
    objects: AtomicU64,
    groups: AtomicU64,
    /// Recent groups seen per track alias.
    group_windows: Mutex<GroupWindows>,
}

impl TrafficCounter {
    pub(crate) fn object(&self) {
        self.objects.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a group that the caller knows has just started.
    pub(crate) fn group(&self) {
        self.groups.fetch_add(1, Ordering::Relaxed);
    }

    /// Count `group_id` the first time an object of it crosses on `track_alias`.
    ///
    /// Subgroup streams arrive concurrently and in any order, including the
    /// first streams of different groups, so each track remembers which of
    /// its recent groups it has seen; see [`GROUP_WINDOW`].
    pub(crate) fn track_group(&self, track_alias: u64, group_id: u64) {
        let new = self
            .group_windows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first_sight(track_alias, group_id);
        if new {
            self.group();
        }
    }

    /// [`Self::track_group`], skipped when `last` already holds this group.
    ///
    /// Repeating the last group checked never adds a count, because that
    /// group is already marked seen (or too old to count), so a datagram path
    /// can skip the lock for consecutive datagrams of one group.
    pub(crate) fn track_group_once(
        &self,
        last: &mut Option<(u64, u64)>,
        track_alias: u64,
        group_id: u64,
    ) {
        if last.replace((track_alias, group_id)) != Some((track_alias, group_id)) {
            self.track_group(track_alias, group_id);
        }
    }

    /// Count one FETCH object of `group_id`, and its group if it differs from
    /// `last_group`, the previous object's group on the same FETCH stream.
    ///
    /// A FETCH response carries each group's objects together (draft-18
    /// §10.12), so comparing with the previous object counts each group once.
    pub(crate) fn fetch_object(&self, last_group: &mut Option<u64>, group_id: u64) {
        self.object();
        if last_group.replace(group_id) != Some(group_id) {
            self.group();
        }
    }

    fn objects(&self) -> u64 {
        self.objects.load(Ordering::Relaxed)
    }

    fn groups(&self) -> u64 {
        self.groups.load(Ordering::Relaxed)
    }
}

/// Read-only handle to the objects and groups a session has carried.
///
/// Cheap to clone, and still readable after the session ends. The default
/// handle belongs to no session and reads zero.
#[derive(Clone, Debug, Default)]
pub struct SessionTraffic {
    ingress: Arc<TrafficCounter>,
    egress: Arc<TrafficCounter>,
}

impl SessionTraffic {
    pub(crate) fn new(ingress: Arc<TrafficCounter>, egress: Arc<TrafficCounter>) -> Self {
        Self { ingress, egress }
    }

    /// Totals since the session started.
    pub fn snapshot(&self) -> TrafficSnapshot {
        TrafficSnapshot {
            ingress_objects: self.ingress.objects(),
            ingress_groups: self.ingress.groups(),
            egress_objects: self.egress.objects(),
            egress_groups: self.egress.groups(),
        }
    }
}

/// Objects and groups a session has carried since it started. Each value only
/// increases.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct TrafficSnapshot {
    /// Objects received from the peer.
    pub ingress_objects: u64,
    /// Groups received from the peer.
    pub ingress_groups: u64,
    /// Objects sent to the peer.
    pub egress_objects: u64,
    /// Groups sent to the peer.
    pub egress_groups: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_counts_once_across_its_subgroups() {
        let counter = TrafficCounter::default();
        for (track, group) in [(1, 5), (1, 5), (2, 5), (1, 6), (1, 5)] {
            counter.track_group(track, group);
        }
        assert_eq!(counter.groups(), 3);
    }

    #[test]
    fn repeating_the_last_group_skips_the_lookup_without_changing_counts() {
        let counter = TrafficCounter::default();
        let mut last = None;
        for (track, group) in [(1, 5), (1, 5), (1, 6), (2, 6), (1, 6), (1, 5)] {
            counter.track_group_once(&mut last, track, group);
        }
        assert_eq!(counter.groups(), 3);

        // With the track forgotten, a lookup would count (1, 5) again, so an
        // unchanged count shows the repeat skipped it.
        counter.group_windows.lock().unwrap().tracks.clear();
        counter.track_group_once(&mut last, 1, 5);
        assert_eq!(counter.groups(), 3);
    }

    #[test]
    fn a_fetch_group_counts_once_per_run_of_its_objects() {
        let counter = TrafficCounter::default();
        let mut last = None;
        for group in [4, 4, 5, 5, 5, 3] {
            counter.fetch_object(&mut last, group);
        }
        assert_eq!((counter.objects(), counter.groups()), (6, 3));
    }

    #[test]
    fn forgetting_tracks_is_bounded() {
        let counter = TrafficCounter::default();
        for track in 0..MAX_TRACKED_TRACKS as u64 * 2 {
            counter.track_group(track, 0);
        }
        assert_eq!(
            counter.group_windows.lock().unwrap().tracks.len(),
            MAX_TRACKED_TRACKS
        );
        assert_eq!(counter.groups(), MAX_TRACKED_TRACKS as u64 * 2);
    }

    #[test]
    fn a_late_group_inside_the_window_counts_once() {
        let counter = TrafficCounter::default();
        // Group 3's first stream arrives after group 5's, then repeats.
        for group in [5, 3, 4, 3, 5, 4] {
            counter.track_group(1, group);
        }
        assert_eq!(counter.groups(), 3);
    }

    #[test]
    fn a_group_older_than_the_window_is_not_counted() {
        let counter = TrafficCounter::default();
        counter.track_group(1, 100);
        counter.track_group(1, 100 - (GROUP_WINDOW - 1));
        assert_eq!(counter.groups(), 2, "the oldest group the window holds");
        counter.track_group(1, 100 - GROUP_WINDOW);
        assert_eq!(counter.groups(), 2, "beyond the window");
        // A jump past the window forgets every earlier mark.
        counter.track_group(1, 100 + GROUP_WINDOW);
        counter.track_group(1, 100 + 1);
        assert_eq!(counter.groups(), 4);
    }

    #[test]
    fn the_least_recently_used_track_is_forgotten_first() {
        let counter = TrafficCounter::default();
        for track in 0..MAX_TRACKED_TRACKS as u64 {
            counter.track_group(track, 0);
        }
        // Track 0 stays in use, so track 1 is the least recently used.
        counter.track_group(0, 0);
        counter.track_group(MAX_TRACKED_TRACKS as u64, 0);
        let counted = counter.groups();

        counter.track_group(0, 0);
        assert_eq!(counter.groups(), counted, "the active track is remembered");
        counter.track_group(1, 0);
        assert_eq!(
            counter.groups(),
            counted + 1,
            "the evicted track starts over"
        );
    }

    #[test]
    fn the_snapshot_reads_each_direction() {
        let ingress = Arc::new(TrafficCounter::default());
        let egress = Arc::new(TrafficCounter::default());
        let traffic = SessionTraffic::new(ingress.clone(), egress.clone());
        ingress.object();
        ingress.object();
        ingress.track_group(1, 0);
        egress.object();
        egress.group();
        assert_eq!(
            traffic.snapshot(),
            TrafficSnapshot {
                ingress_objects: 2,
                ingress_groups: 1,
                egress_objects: 1,
                egress_groups: 1,
            }
        );
    }
}

/// Counting over real sessions, on both sides of the wire.
#[cfg(test)]
mod session_tests {
    use std::time::Duration;

    use super::super::{test_support, Session, Transport};
    use super::SessionTraffic;
    use crate::{
        coding::{KeyValuePairs, TrackNamespace},
        data, serve,
    };

    /// Wait until both sides have counted `objects` objects.
    async fn forwarded(sent: &SessionTraffic, received: &SessionTraffic, objects: u64) {
        while sent.snapshot().egress_objects < objects
            || received.snapshot().ingress_objects < objects
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_count_the_objects_and_groups_they_carry() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (client, server) = test_support::loopback_session_pair().await;
            let (client, server) = tokio::join!(
                Session::connect(client, None, Transport::WebTransport),
                Session::accept(server, None, Transport::WebTransport),
            );
            let (client_session, _, mut subscriber) = client.unwrap();
            let (server_session, publisher, _) = server.unwrap();
            let mut publisher = publisher.unwrap();
            let received = client_session.traffic();
            let sent = server_session.traffic();
            let _client = tokio::spawn(client_session.run());
            let _server = tokio::spawn(server_session.run());

            let namespace = TrackNamespace::from_utf8_path("traffic");
            let mut sources = Vec::new();
            let mut subscriptions = Vec::new();
            for name in ["video", "audio"] {
                let (source, served) = serve::Track::new(namespace.clone(), name).produce();
                let (writer, reader) = serve::Track::new(namespace.clone(), name).produce();
                let subscription = subscriber
                    .subscribe_start_with_params(writer, KeyValuePairs::default())
                    .await
                    .unwrap();
                let subscribed = publisher.subscribed().await.unwrap();
                tokio::spawn(subscribed.serve(served));
                subscription.ok().await.unwrap();
                sources.push(source);
                // Dropping a subscription unsubscribes.
                subscriptions.push((subscription, reader));
            }
            let mut sources = sources.into_iter();
            let (video, audio) = (sources.next().unwrap(), sources.next().unwrap());

            // The serve model hands readers only the latest subgroup or
            // datagram, so each step waits until it has been forwarded.
            // Group 0 split across two subgroup streams, then group 1, then
            // two datagram groups: 6 objects in 4 groups.
            let mut video = video.subgroups().unwrap();
            let mut subgroups = Vec::new();
            for (group_id, subgroup_id, objects, total) in
                [(0, 0, 1, 1), (0, 1, 1, 2), (1, 0, 2, 4)]
            {
                let mut subgroup = video
                    .create(serve::Subgroup {
                        group_id,
                        subgroup_id,
                        priority: 1,
                    })
                    .unwrap();
                for _ in 0..objects {
                    subgroup.write(bytes::Bytes::from_static(b"v")).unwrap();
                }
                forwarded(&sent, &received, total).await;
                subgroups.push(subgroup);
            }
            let mut audio = audio.datagrams().unwrap();
            for (group_id, total) in [(3, 5), (4, 6)] {
                audio
                    .write(serve::Datagram {
                        group_id,
                        object_id: 0,
                        priority: 1,
                        status: data::ObjectStatus::NormalObject,
                        end_of_group: false,
                        payload: bytes::Bytes::from_static(b"a"),
                        extension_headers: Default::default(),
                    })
                    .unwrap();
                forwarded(&sent, &received, total).await;
            }

            let (sent, received) = (sent.snapshot(), received.snapshot());
            assert_eq!((sent.egress_objects, received.ingress_objects), (6, 6));
            assert_eq!((sent.egress_groups, received.ingress_groups), (4, 4));
            assert_eq!((sent.ingress_objects, received.egress_objects), (0, 0));
            drop((subscriptions, video, subgroups, audio));
        })
        .await
        .expect("every object is counted on both sides");
    }
}
