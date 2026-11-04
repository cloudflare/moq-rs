// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod client;
pub use client::Client;

pub mod quic;
pub mod tls;

/// Dependencies whose types appear in this crate's public API.
pub mod reexports {
    pub use anyhow;
    pub use moq_transport;
    pub use rustls;
    pub use url;
}
