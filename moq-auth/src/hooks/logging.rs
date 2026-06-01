// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;

use crate::{AuthBlob, AuthDecision, AuthHook, RequestContext, SessionContext};

/// Composable observability wrapper that logs every hook invocation
/// while delegating the actual decision to the inner hook.
pub struct LoggingAuthHook<H> {
    inner: H,
}

impl<H> LoggingAuthHook<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<H: AuthHook> AuthHook for LoggingAuthHook<H> {
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let result: anyhow::Result<AuthDecision> = self.inner.on_setup(ctx, tokens).await;
        match &result {
            Ok(decision) => {
                tracing::debug!(
                    session_id = ctx.session_id,
                    token_count = tokens.len(),
                    allowed = decision.is_allowed(),
                    principal = decision.principal.as_deref(),
                    "auth on_setup"
                );
            }
            Err(e) => {
                tracing::error!(
                    session_id = ctx.session_id,
                    error = %e,
                    "auth on_setup error"
                );
            }
        }
        result
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let result: anyhow::Result<AuthDecision> = self.inner.on_request(ctx, tokens).await;
        match &result {
            Ok(decision) => {
                tracing::debug!(
                    session_id = ctx.session.session_id,
                    operation = ?ctx.operation,
                    allowed = decision.is_allowed(),
                    "auth on_request"
                );
            }
            Err(e) => {
                tracing::error!(
                    session_id = ctx.session.session_id,
                    operation = ?ctx.operation,
                    error = %e,
                    "auth on_request error"
                );
            }
        }
        result
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        self.inner.shutdown().await
    }
}
