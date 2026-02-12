use super::{Publisher, SessionError};
use crate::coding::ReasonPhrase;
use crate::message;
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

    pub fn respond_error(
        &mut self,
        error_code: u64,
        error_message: &str,
    ) -> Result<(), SessionError> {
        let status_error = message::RequestError {
            id: self.request_msg.id,
            error_code,
            reason_phrase: ReasonPhrase(error_message.to_string()),
        };
        self.publisher.send_message(status_error);
        Ok(())
    }

    pub fn respond_ok(mut self, track: &serve::TrackReader) -> Result<(), SessionError> {
        // Send TrackStatusOk
        self.publisher.send_message(message::TrackStatusOk {
            id: self.request_msg.id,
            track_alias: self.request_msg.id, // TODO SLG does a track alias make sense in track_status response?  Using track_status request id for now
            expires: 0,                       // TODO SLG
            group_order: message::GroupOrder::Ascending, // TODO: resolve correct value from publisher / subscriber prefs
            content_exists: track.largest_location().is_some(),
            largest_location: track.largest_location(),
            params: Default::default(),
        });

        Ok(())
    }
}
