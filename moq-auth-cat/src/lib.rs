// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! C4M (CAT for MoQ) authentication hook for the MoQ relay.
//!
//! Implements [draft-ietf-moq-c4m](https://datatracker.ietf.org/doc/draft-ietf-moq-c4m/)
//! using the [`cat-token`](https://crates.io/crates/cat-token) crate.

mod config;
mod error;
mod mapping;

#[cfg(test)]
mod tests;

pub use config::C4MConfig;
pub use cat_token::Es256Algorithm;

use std::sync::Arc;

use async_trait::async_trait;
use cat_token::{
    CatTokenValidator, CryptographicAlgorithm, MoqtAction, MoqtAuthRequest, MoqtValidator,
    decode_token_bytes,
};
use moq_auth::{AuthBlob, AuthDecision, AuthHook, DenyReason, RequestContext, SessionContext};

use crate::error::map_cat_error;
use crate::mapping::map_operation;

pub use cat_token::C4M_TOKEN_TYPE;

/// C4M authentication hook implementing the CAT for MoQ auth scheme.
///
/// Validates CWT tokens carrying MOQT-specific claims (namespace/track
/// scope matching) using the `cat-token` library.
pub struct C4MAuthHook {
    algorithm: Arc<dyn CryptographicAlgorithm + Send + Sync>,
    token_validator: CatTokenValidator,
    moqt_validator: MoqtValidator,
}

impl C4MAuthHook {
    pub fn new(config: C4MConfig) -> Self {
        Self {
            algorithm: config.algorithm,
            token_validator: config.token_validator,
            moqt_validator: config.moqt_validator,
        }
    }

    fn find_c4m_blob<'a>(&self, tokens: &'a [AuthBlob]) -> Option<&'a AuthBlob> {
        tokens.iter().find(|t| t.token_type == C4M_TOKEN_TYPE)
    }

    fn validate_and_authorize(
        &self,
        blob: &AuthBlob,
        action: MoqtAction,
        namespace: Vec<Vec<u8>>,
        track: Vec<u8>,
    ) -> Result<AuthDecision, DenyReason> {
        // Step 1: Decode token from raw bytes and verify cryptographic signature.
        let token = decode_token_bytes(&blob.token_value, self.algorithm.as_ref())
            .map_err(map_cat_error)?;

        // Step 2: Validate standard CWT claims (exp, nbf, iss, aud).
        self.token_validator
            .validate(&token)
            .map_err(map_cat_error)?;

        // Step 3: Validate MOQT-specific claims.
        self.moqt_validator
            .validate_moqt_claims(&token)
            .map_err(map_cat_error)?;

        // Step 4: Authorize the specific action against the token's scopes.
        let request = MoqtAuthRequest::new(action, namespace, track);
        let result = self.moqt_validator.authorize(&token, &request);

        if result.authorized {
            let principal = token.informational.sub.clone();
            Ok(AuthDecision::allow().with_principal(principal))
        } else {
            Err(DenyReason::ScopeMismatch)
        }
    }
}

#[async_trait]
impl AuthHook for C4MAuthHook {
    async fn on_setup(
        &self,
        _ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let Some(blob) = self.find_c4m_blob(tokens) else {
            return Ok(AuthDecision::deny(DenyReason::TokenMissing));
        };

        match self.validate_and_authorize(blob, MoqtAction::ClientSetup, vec![], vec![]) {
            Ok(decision) => Ok(decision),
            Err(reason) => Ok(AuthDecision::deny(reason)),
        }
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> anyhow::Result<AuthDecision> {
        let Some(blob) = self.find_c4m_blob(tokens) else {
            return Ok(AuthDecision::deny(DenyReason::TokenMissing));
        };

        let (action, namespace, track) = map_operation(&ctx.operation);

        match self.validate_and_authorize(blob, action, namespace, track) {
            Ok(decision) => Ok(decision),
            Err(reason) => Ok(AuthDecision::deny(reason)),
        }
    }
}
