// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! An implementation of the MoQ Transport protocol.
//!
//! MoQ Transport is a pub/sub protocol over QUIC.
//! While originally designed for live media, MoQ Transport is generic and can be used for other live applications.
//! The specification is a work in progress and will change.
//! See the [specification](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) and [github](https://github.com/moq-wg/moq-transport) for any updates.
//!
//! # FETCH history before continuing live
//!
//! Drive [`session::Session::run`] concurrently with these APIs. A Joining
//! FETCH ends at the subscription's Joining Location, inclusive. Consume the
//! history first, buffer live delivery, and discard live duplicates through
//! that Location. Keep the [`session::Subscribe`] handle alive throughout.
//! See `moq-clock-ietf --join-groups` for a relay-backed example, including
//! cold-cache warm-up and retention limits.
//!
//! ```no_run
//! # async fn join(mut subscriber: moq_transport::session::Subscriber, writer: moq_transport::serve::TrackWriter) -> Result<(), Box<dyn std::error::Error>> {
//! use moq_transport::{coding::KeyValuePairs, data::FetchRecord, message::FetchType};
//! let subscription = subscriber.subscribe_open(writer).await?;
//! let mut history = subscriber.joining_fetch(
//!     FetchType::RelativeJoining, subscription.id, 2, KeyValuePairs::default(),
//! ).await?;
//! let boundary = history.ok().await?.end_location; // Object + 1; 0 means whole group
//! while let Some(record) = history.next().await? {
//!     if let FetchRecord::Object(object) = record {
//!         // Consume the payload before requesting another record.
//!         while let Some(chunk) = history.read_payload_chunk(64 * 1024).await? {
//!             // Deliver (object.location(), chunk) to the application.
//!         }
//!     }
//! }
//! // Continue consuming the live track, omitting Objects covered by boundary.
//! # let _ = boundary;
//! # Ok(()) }
//! ```
//!
//! [`session::Subscriber::fetch`] initiates a standalone range without a
//! subscription. Publishers opt in with [`session::Publisher::accept_fetch`]
//! and receive typed [`session::FetchRequested`] handles from
//! [`session::Publisher::fetch_requested`]. For example, a publisher answering
//! one retained Object (applications normally select their retained range):
//!
//! ```no_run
//! # async fn answer(request: moq_transport::session::FetchRequested) -> Result<(), Box<dyn std::error::Error>> {
//! use moq_transport::{coding::{KeyValuePairs, Location}, data::{FetchRecord, FetchRecordObject}, session::FetchOkInfo};
//! let Some(range) = request.resolve().await? else { return Ok(()); };
//! let start = range.start_location;
//! let payload = b"retained object";
//! let mut writer = request.respond(FetchOkInfo {
//!     end_of_track: false,
//!     end_location: Location::new(start.group_id, start.object_id.checked_add(1).unwrap_or(0)),
//!     params: KeyValuePairs::default(), track_extensions: Default::default(),
//! }).await?;
//! writer.write_record(&FetchRecord::Object(FetchRecordObject {
//!     group_id: start.group_id, subgroup_id: Some(0), object_id: start.object_id,
//!     publisher_priority: 0, extension_headers: Default::default(),
//!     payload_length: payload.len() as u64,
//! })).await?;
//! writer.write_payload(payload).await?;
//! writer.finish().await?;
//! # Ok(()) }
//! ```
pub mod coding;
pub mod data;
pub mod error;
pub mod message;
pub mod mlog;
pub mod reexports {
    pub use bytes;
    pub use tracing;
    pub use url;
    pub use uuid;
    pub use web_transport_trait;
}
pub mod serve;
pub mod session;
pub mod setup;
pub mod watch;
