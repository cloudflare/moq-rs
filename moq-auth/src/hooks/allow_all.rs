// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;

use crate::AuthHook;

/// Default hook that allows all operations unconditionally.
///
/// A relay built without an explicit `AuthHook` gets this behavior.
pub struct AllowAllAuthHook;

#[async_trait]
impl AuthHook for AllowAllAuthHook {}
