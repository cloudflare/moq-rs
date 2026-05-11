// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::{Publisher, SessionError};
use crate::coding::{KeyValuePairs, ReasonPhrase};
use crate::message;
use crate::message::RequestOk;
use crate::serve;

/// Draft-16 §9.19: TRACK_STATUS parameters (key types).
mod param_keys {
    /// LARGEST_OBJECT parameter type (§9.2.2.7) — bytes-typed (odd key).
    pub const LARGEST_OBJECT: u64 = 0x9;
}

pub struct TrackStatusRequested {
    publisher: Publisher,
    pub request_msg: message::TrackStatus,
}

impl TrackStatusRequested {
    pub fn new(publisher: Publisher, request_msg: message::TrackStatus) -> Self {
        Self {
            publisher,
            request_msg,
        }
    }

    /// Reject the TRACK_STATUS request with REQUEST_ERROR (draft-16 §9.8).
    pub fn respond_error(
        &mut self,
        error_code: u64,
        error_message: &str,
    ) -> Result<(), SessionError> {
        self.publisher.send_message(message::RequestError {
            id: self.request_msg.id,
            error_code,
            retry_interval: 0,
            reason: ReasonPhrase(error_message.to_string()),
        });
        Ok(())
    }

    /// Accept the TRACK_STATUS request with REQUEST_OK (draft-16 §9.7).
    ///
    /// The response includes LARGEST_OBJECT when objects have been published.
    /// No Track Alias is included — draft-16 §9.19 does not use one for
    /// TRACK_STATUS responses.
    pub fn respond_ok(mut self, track: &serve::TrackReader) -> Result<(), SessionError> {
        let mut params = KeyValuePairs::default();

        if let Some(largest) = track.largest_location() {
            // Encode the Location as two consecutive varints (group_id, object_id)
            // and store as the LARGEST_OBJECT bytes parameter (odd key → bytes).
            let mut encoded = bytes::BytesMut::new();
            use crate::coding::Encode as _;
            largest.encode(&mut encoded).map_err(|_| SessionError::Internal)?;
            params.set_bytesvalue(param_keys::LARGEST_OBJECT, encoded.to_vec());
        }

        self.publisher.send_message(RequestOk {
            id: self.request_msg.id,
            params,
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::RequestErrorCode;

    #[test]
    fn respond_error_not_found_code() {
        // Verify that the DoesNotExist code is used for missing tracks.
        assert_eq!(RequestErrorCode::DoesNotExist as u64, 0x10);
    }

    #[test]
    fn param_keys_match_draft_16() {
        // LARGEST_OBJECT is §9.2.2.7 key 0x9 (odd → bytes value).
        assert_eq!(param_keys::LARGEST_OBJECT, 0x9);
        assert_eq!(param_keys::LARGEST_OBJECT % 2, 1, "must be odd (bytes key)");
    }
}
