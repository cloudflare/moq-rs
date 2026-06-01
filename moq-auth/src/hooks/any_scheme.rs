// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;

use crate::{AuthBlob, AuthDecision, AuthHook, DenyReason, RequestContext, SessionContext, Verdict};

/// Accepts a session if any of the inner hooks accepts it.
///
/// Each hook is tried in order; the first Allow verdict wins.
/// If all hooks deny, returns the last deny reason.
///
/// This is the right shape for a relay that accepts multiple auth schemes
/// (e.g., PrivacyPass *or* C4M) on the same port/scope.
pub struct AnySchemeAuthHook {
    hooks: Vec<Arc<dyn AuthHook>>,
}

impl AnySchemeAuthHook {
    pub fn new(hooks: Vec<Arc<dyn AuthHook>>) -> Self {
        Self { hooks }
    }
}

#[async_trait]
impl AuthHook for AnySchemeAuthHook {
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let mut last_deny = AuthDecision::deny(DenyReason::TokenMissing);
        for hook in &self.hooks {
            let decision = hook.on_setup(ctx, tokens).await?;
            match &decision.verdict {
                Verdict::Allow => return Ok(decision),
                Verdict::Deny(_) => last_deny = decision,
            }
        }
        Ok(last_deny)
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let mut last_deny = AuthDecision::deny(DenyReason::TokenMissing);
        for hook in &self.hooks {
            let decision = hook.on_request(ctx, tokens).await?;
            match &decision.verdict {
                Verdict::Allow => return Ok(decision),
                Verdict::Deny(_) => last_deny = decision,
            }
        }
        Ok(last_deny)
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.shutdown().await?;
        }
        Ok(())
    }
}
