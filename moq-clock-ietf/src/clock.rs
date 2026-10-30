// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::Context;
use moq_transport::serve::{
    Datagram, DatagramsReader, DatagramsWriter, StreamReader, Subgroup, SubgroupWriter,
    SubgroupsReader, SubgroupsWriter, TrackReader, TrackReaderMode,
};

use crate::history::History;
use chrono::prelude::*;
use moq_transport::{coding::Location, reexports::bytes::Bytes};
use tokio::task;

/// Publishes the current time every second in the format "YYYY-MM-DD HH:MM:SS"
pub struct Publisher {
    track_subgroups_writer: Option<SubgroupsWriter>,
    track_datagrams_writer: Option<DatagramsWriter>,
    history: Option<History>,
}

impl Publisher {
    pub fn new(track_subgroups_writer: SubgroupsWriter, history: History) -> Self {
        Self {
            track_subgroups_writer: Some(track_subgroups_writer),
            track_datagrams_writer: None,
            history: Some(history),
        }
    }

    pub fn new_datagram(track_datagrams_writer: DatagramsWriter) -> Self {
        Self {
            track_subgroups_writer: None,
            track_datagrams_writer: Some(track_datagrams_writer),
            history: None,
        }
    }

    /// Runs the publisher, sending the current time every second.  Creates a new group for each minute.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let start = Utc::now();
        let mut now = start;

        // Just for fun, don't start at zero.
        let mut next_group_id = start.minute();

        // Create a new group for each minute.
        loop {
            let mut next: DateTime<Utc>;
            if let Some(track_subgroups_writer) = &mut self.track_subgroups_writer {
                let subgroup_writer = track_subgroups_writer
                    .create(Subgroup {
                        group_id: next_group_id as u64,
                        subgroup_id: 0,
                        priority: 0,
                    })
                    .context("failed to create minute segment")?;

                // Finish each minute before creating the next, keeping live
                // Object IDs and the retained snapshot on the same timeline.
                Self::send_subgroup_objects(
                    subgroup_writer,
                    now,
                    self.history.as_ref().context("missing clock history")?,
                )
                .await?;

                next = now + chrono::Duration::try_minutes(1).unwrap();
                next = next.with_second(0).unwrap().with_nanosecond(0).unwrap();
            } else if let Some(track_datagrams_writer) = &mut self.track_datagrams_writer {
                let time_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
                track_datagrams_writer
                    .write(Datagram {
                        group_id: next_group_id as u64,
                        object_id: 0,
                        priority: 127,
                        status: moq_transport::data::ObjectStatus::NormalObject,
                        end_of_group: false,
                        payload: time_str.clone().into_bytes().into(),
                        extension_headers: Default::default(),
                    })
                    .context("failed to write datagram")?;

                println!("{}", time_str);

                next = now + chrono::Duration::try_seconds(1).unwrap();
                next = next.with_nanosecond(0).unwrap();
            } else {
                return Err(anyhow::anyhow!("no track writer available"));
            }

            next_group_id += 1;

            // Sleep until the start of the next minute (stream mode) or next second (datagram mode)
            let delay = (next - Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(delay).await;

            now = next; // just assume we didn't undersleep
        }
    }

    /// Sends the current time every second within a minute group.
    async fn send_subgroup_objects(
        mut subgroup_writer: SubgroupWriter,
        mut now: DateTime<Utc>,
        history: &History,
    ) -> anyhow::Result<()> {
        // Everything but the second.
        let base = now.format("%Y-%m-%d %H:%M:").to_string();

        let group_id = subgroup_writer.group_id;
        let payload = Bytes::from(base.clone());
        history.push(Location::new(group_id, 0), payload.clone())?;
        subgroup_writer
            .write(payload)
            .context("failed to write base")?;

        let mut object_id = 1;
        loop {
            let delta = now.format("%S").to_string();
            let payload = Bytes::from(delta.clone());
            history.push(Location::new(group_id, object_id), payload.clone())?;
            subgroup_writer
                .write(payload)
                .context("failed to write delta")?;

            println!("{base}{delta}");
            object_id += 1;

            let next = now + chrono::Duration::try_seconds(1).unwrap();
            let next = next.with_nanosecond(0).unwrap();

            // Sleep until the next second
            let delay = (next - now).to_std().unwrap();
            tokio::time::sleep(delay).await;

            // Get the current time again to check if we overslept
            let next = Utc::now();
            if next.minute() != now.minute() {
                subgroup_writer.finish()?;
                return Ok(());
            }

            now = next;
        }
    }
}

/// Subscribes to the clock and prints received time updates to stdout.
pub struct Subscriber {
    track_reader: TrackReader,
}

impl Subscriber {
    pub fn new(track_reader: TrackReader) -> Self {
        Self { track_reader }
    }

    /// Keep the SUBSCRIBE handle alive while FETCH fills its historical
    /// prefix. Live delivery is buffered by the track's bounded serve cache.
    pub async fn run_joined(
        self,
        subscriber: moq_transport::session::Subscriber,
        writer: moq_transport::serve::TrackWriter,
        groups: u64,
    ) -> anyhow::Result<()> {
        self.run_joined_with_output(subscriber, writer, groups, |time| println!("{time}"))
            .await
    }

    pub(super) async fn run_joined_with_output(
        mut self,
        mut subscriber: moq_transport::session::Subscriber,
        writer: moq_transport::serve::TrackWriter,
        groups: u64,
        mut output: impl FnMut(String),
    ) -> anyhow::Result<()> {
        use moq_transport::{
            coding::KeyValuePairs,
            data::FetchRecord,
            message::{FetchType, GroupOrder},
        };
        use std::time::Duration;

        let mut params = KeyValuePairs::default();
        params.set_group_order(GroupOrder::Ascending);
        params.set_subscription_filter(&moq_transport::message::SubscriptionFilter {
            filter_type: moq_transport::message::FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
        })?;
        let namespace = writer.namespace.clone();
        let name = writer.name.clone();
        let subscription = tokio::time::timeout(Duration::from_secs(10), async {
            let subscription = subscriber
                .subscribe_open_with_params(writer, params.clone())
                .await?;
            if subscription
                .subscribe_ok()
                .context("missing SUBSCRIBE_OK")?
                .params
                .largest_object()?
                .is_some()
            {
                return Ok::<_, anyhow::Error>(subscription);
            }
            // A cold relay snapshots its empty cache before establishing its
            // upstream subscription. That SUBSCRIBE cannot be joined (§10.12.2).
            // Prime one Object, then use a fresh SUBSCRIBE with an actual
            // Joining Location. No warm-up Objects are printed.
            let TrackReaderMode::Subgroups(mut warmup) = self.track_reader.mode().await? else {
                anyhow::bail!("Joining FETCH clock demo requires stream publishing");
            };
            let mut subgroup = warmup
                .next()
                .await?
                .context("clock warm-up has no subgroup")?;
            let mut object = subgroup
                .next()
                .await?
                .context("clock warm-up has no Object")?;
            anyhow::ensure!(object.size <= 64, "oversized warm-up clock Object");
            object.read_all().await?;
            drop(object);
            drop(subgroup);
            drop(warmup);
            subscription.unsubscribe().await;
            let (writer, reader) = moq_transport::serve::Track::new(namespace, name).produce();
            self.track_reader = reader;
            Ok(subscriber
                .subscribe_open_with_params(writer, params)
                .await?)
        })
        .await
        .context("clock SUBSCRIBE warm-up timed out")??;
        let joining = subscription
            .subscribe_ok()
            .context("missing SUBSCRIBE_OK")?
            .params
            .largest_object()?
            .context("clock has no Objects to join yet")?;
        let mut decoder = JoinedClock::default();
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut fetch = subscriber
                .joining_fetch(
                    FetchType::RelativeJoining,
                    subscription.id,
                    groups.min(joining.group_id),
                    KeyValuePairs::default(),
                )
                .await?;
            let ok = fetch.ok().await?;
            anyhow::ensure!(
                crate::history::inclusive_end(ok.end_location) == joining,
                "FETCH did not reach the SUBSCRIBE Joining Location"
            );
            while let Some(record) = fetch.next().await? {
                let FetchRecord::Object(object) = record else {
                    anyhow::bail!("clock history contains unavailable Objects");
                };
                anyhow::ensure!(
                    object.subgroup_id == Some(0) && object.payload_length <= 64,
                    "invalid clock history Object"
                );
                let mut payload = Vec::new();
                while let Some(chunk) = fetch.read_payload_chunk(64).await? {
                    payload.extend_from_slice(&chunk);
                }
                if let Some(time) = decoder.object(object.location(), &payload)? {
                    output(time);
                }
            }
            anyhow::ensure!(
                decoder.last == Some(joining),
                "clock FETCH ended before its advertised boundary"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Joining FETCH timed out")??;

        tracing::info!(
            group = joining.group_id,
            object = joining.object_id,
            "history complete; continuing live"
        );
        let TrackReaderMode::Subgroups(mut subgroups) = self.track_reader.mode().await? else {
            anyhow::bail!("Joining FETCH clock demo requires stream publishing");
        };
        let live = async {
            while let Some(mut subgroup) = subgroups.next().await? {
                let group = subgroup.group_id;
                while let Some(mut object) = subgroup.next().await? {
                    let location = Location::new(group, object.object_id);
                    // A live SUBSCRIBE can repeat the Joining Location (or
                    // earlier Objects in its group). History already emitted
                    // them, including the base needed to decode live deltas.
                    if location <= joining {
                        continue;
                    }
                    anyhow::ensure!(object.size <= 64, "oversized live clock Object");
                    let payload = object.read_all().await?;
                    if let Some(time) = decoder.object(location, &payload)? {
                        output(time);
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {
            result = live => result,
            result = subscription.closed() => result.context("clock subscription ended"),
        }
    }

    /// Runs the subscriber, receiving time updates and printing them to stdout.
    pub async fn run(self) -> anyhow::Result<()> {
        match self
            .track_reader
            .mode()
            .await
            .context("failed to get mode")?
        {
            TrackReaderMode::Stream(stream) => Self::recv_stream(stream).await,
            TrackReaderMode::Subgroups(subgroups) => Self::recv_subgroups(subgroups).await,
            TrackReaderMode::Datagrams(datagrams) => Self::recv_datagrams(datagrams).await,
        }
    }

    /// Receives time updates from a stream and prints them to stdout.
    async fn recv_stream(mut stream_reader: StreamReader) -> anyhow::Result<()> {
        while let Some(mut stream_group_reader) = stream_reader.next().await? {
            while let Some(object) = stream_group_reader.read_next().await? {
                let str = String::from_utf8_lossy(&object);
                println!("{str}");
            }
        }

        Ok(())
    }

    /// Receives time updates from subgroups and prints them to stdout.
    async fn recv_subgroups(mut subgroups_reader: SubgroupsReader) -> anyhow::Result<()> {
        while let Some(mut subgroup_reader) = subgroups_reader.next().await? {
            // Spawn a new task to handle the subgroup concurrently, so we
            // don't rely on the publisher ending the previous stream before starting a new one.
            task::spawn(async move {
                if let Err(e) = async {
                    let base = subgroup_reader
                        .read_next()
                        .await
                        .context("failed to get first object")?
                        .context("empty subgroup")?;

                    let base = String::from_utf8_lossy(&base);

                    while let Some(object) = subgroup_reader.read_next().await? {
                        let str = String::from_utf8_lossy(&object);
                        println!("{base}{str}");
                    }

                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    eprintln!("Error handling subgroup: {:?}", e);
                }
            });
        }

        Ok(())
    }

    /// Receives time updates from datagrams and prints them to stdout.
    async fn recv_datagrams(mut datagrams_reader: DatagramsReader) -> anyhow::Result<()> {
        while let Some(datagram) = datagrams_reader.read().await? {
            let str = String::from_utf8_lossy(&datagram.payload);
            println!("{str}");
        }

        Ok(())
    }
}

/// A single ordered timeline across FETCH and live, preserving the minute
/// base across the handoff and failing visibly on a missing delta.
#[derive(Default)]
struct JoinedClock {
    last: Option<Location>,
    base: String,
}

impl JoinedClock {
    fn object(&mut self, location: Location, payload: &[u8]) -> anyhow::Result<Option<String>> {
        if self.last.is_some_and(|last| location <= last) {
            return Ok(None);
        }
        if let Some(last) = self.last {
            if location.group_id == last.group_id {
                anyhow::ensure!(
                    location.object_id == last.object_id + 1,
                    "missing clock Object at live/history boundary"
                );
            } else {
                anyhow::ensure!(
                    location.group_id == last.group_id + 1 && location.object_id == 0,
                    "missing clock group"
                );
            }
        } else {
            anyhow::ensure!(
                location.object_id == 0,
                "clock history must start with a minute base"
            );
        }
        let value = std::str::from_utf8(payload)?;
        let result = if location.object_id == 0 {
            self.base = value.to_owned();
            None
        } else {
            Some(format!("{}{value}", self.base))
        };
        self.last = Some(location);
        Ok(result)
    }
}

#[cfg(test)]
mod joining_tests {
    use super::*;

    #[test]
    fn history_then_overlapping_live_is_one_timeline() {
        let mut clock = JoinedClock::default();
        let mut output = Vec::new();
        for (group, object, payload) in [
            (7, 0, "2026-10-29 10:07:"),
            (7, 1, "58"),
            (7, 2, "59"),
            (8, 0, "2026-10-29 10:08:"),
            (8, 1, "00"),
            // Live repeats its cached prefix through Joining Location {8,1}.
            (8, 0, "2026-10-29 10:08:"),
            (8, 1, "00"),
            (8, 2, "01"),
            (9, 0, "2026-10-29 10:09:"),
            (9, 1, "00"),
        ] {
            if let Some(time) = clock
                .object(Location::new(group, object), payload.as_bytes())
                .unwrap()
            {
                output.push(time);
            }
        }
        assert_eq!(
            output,
            [
                "2026-10-29 10:07:58",
                "2026-10-29 10:07:59",
                "2026-10-29 10:08:00",
                "2026-10-29 10:08:01",
                "2026-10-29 10:09:00"
            ]
        );
    }

    #[test]
    fn missing_boundary_object_is_an_error_not_a_silent_gap() {
        let mut clock = JoinedClock::default();
        clock.object(Location::new(7, 0), b"base").unwrap();
        clock.object(Location::new(7, 1), b"00").unwrap();
        assert!(clock.object(Location::new(7, 3), b"02").is_err());
        assert!(clock.object(Location::new(9, 0), b"base").is_err());
    }
}
