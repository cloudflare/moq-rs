// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use cat_token::CatError;
use moq_auth::DenyReason;

pub(crate) fn map_cat_error(err: CatError) -> DenyReason {
    match err {
        CatError::TokenExpired => DenyReason::TokenExpired,
        CatError::TokenNotYetValid => DenyReason::TokenExpired,
        CatError::InvalidIssuer => DenyReason::IssuerUnknown,
        CatError::InvalidAudience => DenyReason::TokenInvalid,
        CatError::SignatureVerificationFailed => DenyReason::TokenInvalid,
        CatError::MoqtActionNotAuthorized(_) => DenyReason::ScopeMismatch,
        CatError::InvalidTokenFormat => DenyReason::TokenMalformed,
        CatError::InvalidCbor(_) => DenyReason::TokenMalformed,
        CatError::InvalidBase64(_) => DenyReason::TokenMalformed,
        CatError::MissingRequiredClaim(_) => DenyReason::TokenMalformed,
        CatError::ReplayAttackDetected => DenyReason::TokenReplayed,
        CatError::UnsupportedAlgorithm(_) => DenyReason::TokenInvalid,
        CatError::AlgorithmMismatch { .. } => DenyReason::TokenInvalid,
        _ => DenyReason::Other {
            message: format!("{err}"),
        },
    }
}
