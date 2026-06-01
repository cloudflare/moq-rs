// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pluggable authorization hook for the MoQ relay.
//!
//! This crate defines the [`AuthHook`] trait and supporting types for
//! intra-scope authorization in `moq-relay-ietf`. Concrete implementations
//! (C4M, PrivacyPass) live in separate crates.

mod hook;
pub mod hooks;
mod types;

#[cfg(test)]
mod tests;

pub use hook::AuthHook;
pub use hooks::*;
pub use types::*;
