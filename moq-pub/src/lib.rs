// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Live CMAF publishing over MoQT draft-18.
//!
//! The draft-16 FETCH history API (`FetchHistory`, `serve_fetches`, and
//! `Media::new_with_fetch`) and the `reexports` module have been removed.
//! Use [`Media::new`] for live publishing; this crate no longer archives media
//! or serves historical FETCH requests.

mod media;
pub use media::*;
