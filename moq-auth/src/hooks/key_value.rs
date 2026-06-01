// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;
use bytes::Bytes;

use crate::{AuthBlob, AuthDecision, AuthHook, DenyReason, RequestContext, SessionContext};

/// Accepts any token whose Token Type is 0 (negotiated out-of-band) and
/// whose Token Value matches a configured shared secret.
///
/// Uses constant-time comparison to prevent timing attacks.
/// Useful for early end-to-end integration testing without standing up
/// issuer infrastructure.
pub struct KeyValueAuthHook {
    expected_secret: Bytes,
}

impl KeyValueAuthHook {
    pub fn new(secret: impl Into<Bytes>) -> Self {
        Self {
            expected_secret: secret.into(),
        }
    }

    fn validate(&self, tokens: &[AuthBlob]) -> AuthDecision {
        for token in tokens {
            if token.token_type == 0 && constant_time_eq(&token.token_value, &self.expected_secret)
            {
                return AuthDecision::allow();
            }
        }
        AuthDecision::deny(DenyReason::TokenInvalid)
    }
}

#[async_trait]
impl AuthHook for KeyValueAuthHook {
    async fn on_setup(
        &self,
        _ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(self.validate(tokens))
    }

    async fn on_request(
        &self,
        _ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        Ok(self.validate(tokens))
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(constant_time_eq(b"", b""));
    }
}
