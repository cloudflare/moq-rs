// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

/// Setup Option Types per draft-ietf-moq-transport-18 §15.4.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum ParameterType {
    Path = 0x1,
    AuthorizationToken = 0x3,
    MaxAuthTokenCacheSize = 0x4,
    Authority = 0x5,
    MOQTImplementation = 0x7,
}

impl From<ParameterType> for u64 {
    fn from(value: ParameterType) -> Self {
        value as u64
    }
}
