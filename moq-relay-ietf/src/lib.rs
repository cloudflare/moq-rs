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
//! use moq_relay_ietf::{Relay, RelayConfig, LocalCoordinator};
//!
//! // Create a coordinator (LocalCoordinator for single-relay deployments)
//! let coordinator = LocalCoordinator::new();
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
mod producer;
mod relay;
mod remote;
mod session;
mod web;

pub use api::*;
pub use consumer::*;
pub use coordinator::*;
pub use local::*;
pub use producer::*;
pub use relay::*;
pub use remote::*;
pub use session::*;
pub use web::*;
