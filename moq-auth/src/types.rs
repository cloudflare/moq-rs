// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::SocketAddr;

use bytes::Bytes;
use moq_transport::coding::TrackNamespace;

/// Information about the QUIC/MoQT session. Stable for the session's lifetime.
#[derive(Debug, Clone)]
pub struct SessionContext {
    /// Relay-assigned session handle. Stable across the session lifetime.
    pub session_id: u64,

    /// The connection path from the WebTransport URL or the PATH setup parameter.
    pub connection_path: Option<String>,

    /// Peer socket address.
    pub peer: SocketAddr,
}

/// Information about a specific request. Built fresh per call.
#[derive(Debug)]
pub struct RequestContext<'a> {
    pub session: &'a SessionContext,
    pub operation: AuthzOperation<'a>,
    pub request_id: Option<u64>,
}

/// The operation being authorized.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AuthzOperation<'a> {
    PublishNamespace { namespace: &'a TrackNamespace },
    PublishNamespaceDone { namespace: &'a TrackNamespace },
    Publish { namespace: &'a TrackNamespace, track: &'a [u8] },
    Subscribe { namespace: &'a TrackNamespace, track: &'a [u8] },
    SubscribeNamespace { prefix: &'a TrackNamespace },
    Fetch { namespace: &'a TrackNamespace, track: &'a [u8] },
    TrackStatus { namespace: &'a TrackNamespace, track: &'a [u8] },
    RequestUpdate { request_id: u64 },
}

/// A fully-resolved AUTHORIZATION TOKEN parameter.
///
/// Alias bookkeeping is handled by the relay's transport layer; the hook
/// always sees Type+Value pairs, never raw alias directives.
#[derive(Debug, Clone)]
pub struct AuthBlob {
    /// Token type from the IANA "MOQT Auth Token Type" registry.
    pub token_type: u64,
    /// Raw token value bytes.
    pub token_value: Bytes,
}

/// The hook's decision.
#[derive(Debug, Clone)]
pub struct AuthDecision {
    pub verdict: Verdict,
    /// Optional opaque principal identifier for logging and metrics.
    pub principal: Option<String>,
}

impl AuthDecision {
    pub fn allow() -> Self {
        Self {
            verdict: Verdict::Allow,
            principal: None,
        }
    }

    pub fn deny(reason: DenyReason) -> Self {
        Self {
            verdict: Verdict::Deny(reason),
            principal: None,
        }
    }

    pub fn with_principal(mut self, principal: impl Into<Option<String>>) -> Self {
        self.principal = principal.into();
        self
    }

    pub fn is_allowed(&self) -> bool {
        matches!(self.verdict, Verdict::Allow)
    }
}

#[derive(Debug, Clone)]
pub enum Verdict {
    Allow,
    Deny(DenyReason),
}

/// Abstract deny reasons. The relay maps these to wire codes appropriate
/// for the auth scheme in use.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DenyReason {
    #[error("token missing")]
    TokenMissing,
    #[error("token invalid")]
    TokenInvalid,
    #[error("token expired")]
    TokenExpired,
    #[error("token replayed")]
    TokenReplayed,
    #[error("token malformed")]
    TokenMalformed,
    #[error("scope mismatch")]
    ScopeMismatch,
    #[error("issuer unknown")]
    IssuerUnknown,
    #[error("{message}")]
    Other { message: String },
}
