// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

mod fetch;
mod media;
pub use fetch::*;
pub use media::*;

/// Dependencies exposed by this crate's public API.
///
/// ```
/// use moq_pub::reexports::moq_transport;
///
/// let location = moq_transport::coding::Location::new(0, 0);
/// assert_eq!(location.group_id, 0);
/// ```
pub mod reexports {
    pub use moq_transport;
}
