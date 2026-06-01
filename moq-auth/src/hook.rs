// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;

use crate::{AuthBlob, AuthDecision, RequestContext, SessionContext};

/// Pluggable authorization hook for the MoQ relay.
///
/// Called at session establishment and before each authorization-relevant
/// request. Implementations validate tokens and return allow/deny verdicts.
///
/// Default implementations return `Ok(AuthDecision::allow())` — a relay
/// without an explicit hook behaves identically to one with no auth.
#[async_trait]
pub trait AuthHook: Send + Sync {
    /// Called once per session at SETUP time, after AUTHORIZATION TOKEN
    /// parameters have been decoded and any aliases resolved.
    ///
    /// Returning a deny verdict causes the relay to terminate the session.
    async fn on_setup(
        &self,
        _ctx: &SessionContext,
        _tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(AuthDecision::allow())
    }

    /// Called once per request before the relay performs any
    /// authorization-relevant action.
    async fn on_request(
        &self,
        _ctx: &RequestContext<'_>,
        _tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(AuthDecision::allow())
    }

    /// Graceful shutdown hook. Implementations may use this to flush state.
    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
