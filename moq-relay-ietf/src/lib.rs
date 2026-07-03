// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MoQ Relay library for building Media over QUIC relay servers.
//!
//! This crate provides the core relay functionality that can be embedded
//! into other applications. The relay handles:
//!
//! - Accepting QUIC connections from publishers and subscribers
//! - Routing media between local and remote endpoints
//! - Coordinating namespace/track registration across relay clusters
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use moq_relay_ietf::{Relay, RelayConfig, FileCoordinator};
//!
//! // Create a coordinator (FileCoordinator for multi-relay deployments)
//! let coordinator = FileCoordinator::new("/path/to/coordination/file", "https://relay.example.com");
//!
//! // Configure and create the relay
//! let relay = Relay::new(RelayConfig {
//!     bind: "[::]:443".parse().unwrap(),
//!     tls: tls_config,
//!     coordinator,
//!     // ... other options
//! })?;
//!
//! // Run the relay
//! relay.run().await?;
//! ```

mod api;
mod consumer;
mod coordinator;
mod local;
pub mod metrics;
mod producer;
mod relay;
mod remote;
mod session;
mod web;

/// How long the relay keeps a warm upstream subscription alive after its last
/// downstream subscriber leaves, before tearing it down.
///
/// A relay deduplicates many downstream subscribers onto a single upstream
/// subscription. When the last downstream subscriber unsubscribes we do NOT tear
/// the upstream subscription down immediately: a brief linger lets an instant
/// late-joiner (a page reload, a player reconnect, a channel flip back) reattach
/// to the still-warm subscription instead of paying for a fresh upstream
/// SUBSCRIBE and the startup latency that comes with it (waiting for the next
/// group / keyframe).
///
/// Once the linger elapses with no downstream interest, the upstream `Subscribe`
/// handle is dropped — which is the only place the transport crate emits an
/// UNSUBSCRIBE — and the dedup cache entry is evicted so a future subscriber
/// starts a fresh upstream subscription.
///
/// Chosen at 3s: long enough to absorb brief gaps and reconnects, short enough
/// that a publisher isn't served (and billed) for long after nobody is watching.
pub const WARM_CACHE_LINGER: std::time::Duration = std::time::Duration::from_secs(3);

pub use api::*;
pub use consumer::*;
pub use coordinator::*;
pub use local::*;
pub use producer::*;
pub use relay::*;
pub use remote::RemoteManager;
pub use session::*;
pub use web::*;
