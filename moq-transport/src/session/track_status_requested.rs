// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::{Publisher, SessionError};
use crate::coding::{KeyValuePairs, ReasonPhrase};
use crate::message;
use crate::message::RequestOk;
use crate::serve;

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
        self.publisher.send_request_error(
            "track_status",
            message::RequestError {
                id: self.request_msg.id,
                error_code,
                retry_interval: 0,
                reason: ReasonPhrase::new(error_message),
            },
        );
        Ok(())
    }

    /// Accept TRACK_STATUS with the track's properties and LARGEST_OBJECT when
    /// objects have been published. No Track Alias is included (draft-18 Section 10.14).
    pub fn respond_ok(mut self, track: &serve::TrackReader) -> Result<(), SessionError> {
        let mut params = KeyValuePairs::default();

        if let Some(largest) = track.largest_location() {
            params
                .set_largest_object(largest)
                .map_err(|_| SessionError::Internal)?;
        }

        self.publisher.send_request_ok(
            "track_status",
            RequestOk {
                id: self.request_msg.id,
                params,
                track_extensions: track.track_extensions(),
            },
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{Decode, Encode, TrackNamespace};
    use crate::session::{test_support, RequestId};
    use crate::watch::Queue;

    #[tokio::test]
    async fn respond_ok_sends_track_extensions_snapshot() {
        let transport = test_support::loopback_session().await;
        let (outgoing, mut responses) = Queue::default().split();
        let (tasks, _task_recv) = tokio::sync::mpsc::unbounded_channel();
        let publisher = Publisher::new(outgoing, transport, None, RequestId::new(1, 0), tasks);
        let namespace = TrackNamespace::from_utf8_path("test");
        let request = TrackStatusRequested::new(
            publisher.clone(),
            message::TrackStatus {
                id: 42,
                track_namespace: namespace.clone(),
                track_name: "track".into(),
                params: KeyValuePairs::default(),
            },
        );
        let (mut writer, reader) = serve::Track::new(namespace, "track").produce();
        let mut input = test_support::TRACK_PROPERTY_BYTES;
        writer.set_track_extensions(message::TrackExtensions::decode(&mut input).unwrap());
        request.respond_ok(&reader).unwrap();
        writer.set_track_extensions(message::TrackExtensions::new());

        let Some(message::Message::RequestOk(ok)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), responses.pop())
                .await
                .unwrap()
        else {
            panic!("expected TRACK_STATUS_OK");
        };
        assert_eq!(ok.id, 42);
        let mut forwarded = Vec::new();
        ok.track_extensions.encode(&mut forwarded).unwrap();
        assert_eq!(forwarded, test_support::TRACK_PROPERTY_BYTES);
    }
}
