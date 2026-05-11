// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REQUEST_ERROR message (draft-ietf-moq-transport-16 §9.8).
//!
//! Sent in response to any request (SUBSCRIBE, FETCH, PUBLISH,
//! SUBSCRIBE_NAMESPACE, PUBLISH_NAMESPACE, TRACK_STATUS, REQUEST_UPDATE).
//! Replaces the per-request error messages from earlier drafts.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

/// Draft-16 §13.4.2 REQUEST_ERROR codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum RequestErrorCode {
    InternalError = 0x0,
    Unauthorized = 0x1,
    Timeout = 0x2,
    NotSupported = 0x3,
    MalformedAuthToken = 0x4,
    ExpiredAuthToken = 0x5,
    DoesNotExist = 0x10,
    InvalidRange = 0x11,
    MalformedTrack = 0x12,
    DuplicateSubscription = 0x19,
    Uninterested = 0x20,
    PrefixOverlap = 0x30,
    InvalidJoiningRequestId = 0x32,
}

impl From<RequestErrorCode> for u64 {
    fn from(c: RequestErrorCode) -> u64 {
        c as u64
    }
}

/// Sent to reject any request.
///
/// `retry_interval`: minimum time (ms) before the request SHOULD be sent
/// again, plus one.  A value of 0 means the request MUST NOT be retried;
/// a value of 1 means it can be retried immediately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestError {
    /// The Request ID of the message this is rejecting.
    pub id: u64,

    /// Error code identifying the reason for rejection.
    pub error_code: u64,

    /// Minimum retry delay in milliseconds plus one, or 0 for no retry.
    pub retry_interval: u64,

    /// Human-readable reason phrase (UTF-8, max 1024 bytes).
    pub reason: ReasonPhrase,
}

impl RequestError {
    /// Convenience constructor from a [`RequestErrorCode`].
    pub fn new(id: u64, code: RequestErrorCode, retry_interval: u64, reason: &str) -> Self {
        Self {
            id,
            error_code: code as u64,
            retry_interval,
            reason: ReasonPhrase(reason.to_string()),
        }
    }

    /// Return `true` if this error code indicates the request should not be retried.
    pub fn is_fatal(&self) -> bool {
        self.retry_interval == 0
    }
}

impl Decode for RequestError {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let error_code = u64::decode(r)?;
        let retry_interval = u64::decode(r)?;
        let reason = ReasonPhrase::decode(r)?;
        Ok(Self {
            id,
            error_code,
            retry_interval,
            reason,
        })
    }
}

impl Encode for RequestError {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.error_code.encode(w)?;
        self.retry_interval.encode(w)?;
        self.reason.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let msg = RequestError {
            id: 42,
            error_code: RequestErrorCode::DoesNotExist as u64,
            retry_interval: 0,
            reason: ReasonPhrase("track not found".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestError::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_with_retry() {
        let mut buf = BytesMut::new();
        let msg = RequestError::new(10, RequestErrorCode::Timeout, 5001, "upstream timeout");
        msg.encode(&mut buf).unwrap();
        let decoded = RequestError::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, 10);
        assert_eq!(decoded.error_code, RequestErrorCode::Timeout as u64);
        assert_eq!(decoded.retry_interval, 5001);
        assert!(!decoded.is_fatal());
    }

    #[test]
    fn is_fatal_when_retry_interval_zero() {
        let msg = RequestError {
            id: 1,
            error_code: 0,
            retry_interval: 0,
            reason: ReasonPhrase(String::new()),
        };
        assert!(msg.is_fatal());
    }
}
