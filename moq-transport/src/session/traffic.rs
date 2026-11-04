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

/// Tracks whose largest group ID is remembered per direction.
///
/// A session that has carried more tracks than this forgets them all and
/// starts again, so a group in flight at that moment can be counted twice.
/// Bounding the memory of a long-lived session matters more.
const MAX_TRACKED_TRACKS: usize = 4096;

/// Counts the objects and groups crossing a session in one direction.
#[derive(Debug, Default)]
pub(crate) struct TrafficCounter {
    objects: AtomicU64,
    groups: AtomicU64,
    /// Largest group ID seen per track alias.
    largest_groups: Mutex<HashMap<u64, u64>>,
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
    /// Subgroup streams of one group arrive concurrently and in any order, so
    /// a group is new only if it is larger than every group seen on the track.
    /// A group whose first object arrives after a newer group's is therefore
    /// not counted.
    pub(crate) fn track_group(&self, track_alias: u64, group_id: u64) {
        let mut largest_groups = self
            .largest_groups
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match largest_groups.get_mut(&track_alias) {
            Some(largest) if *largest >= group_id => return,
            Some(largest) => *largest = group_id,
            None => {
                if largest_groups.len() >= MAX_TRACKED_TRACKS {
                    largest_groups.clear();
                }
                largest_groups.insert(track_alias, group_id);
            }
        }
        drop(largest_groups);
        self.group();
    }

    /// [`Self::track_group`], skipped when `last` already holds this group.
    ///
    /// Repeating the last group checked is always a no-op, because the
    /// largest group per track only grows, so a datagram path can skip the
    /// lock for consecutive datagrams of one group.
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
        counter.largest_groups.lock().unwrap().clear();
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
        assert!(counter.largest_groups.lock().unwrap().len() <= MAX_TRACKED_TRACKS);
        assert_eq!(counter.groups(), MAX_TRACKED_TRACKS as u64 * 2);
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
