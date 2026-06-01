// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;

use crate::{AuthBlob, AuthDecision, AuthHook, DenyReason, RequestContext, SessionContext};

/// Hook that denies all operations. Useful for testing that the hook
/// is wired up correctly at all invocation points.
pub struct DenyAllAuthHook;

#[async_trait]
impl AuthHook for DenyAllAuthHook {
    async fn on_setup(
        &self,
        _ctx: &SessionContext,
        _tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(AuthDecision::deny(DenyReason::TokenMissing))
    }

    async fn on_request(
        &self,
        _ctx: &RequestContext<'_>,
        _tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(AuthDecision::deny(DenyReason::TokenMissing))
    }
}
