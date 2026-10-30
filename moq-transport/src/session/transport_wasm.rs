// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Existing browser transport adapter.
//!
//! The retained browser backend does not implement `web-transport-trait`.
//! Native transport injection lives in the sibling platform implementation;
//! browser sessions continue to use their existing WebTransport API wrapper.

pub use web_transport::*;
