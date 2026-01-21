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
            retry_interval: 0,
            reason_phrase: ReasonPhrase(error_message.to_string()),
        };
        self.publisher.send_message(status_error);
        Ok(())
    }

    pub fn respond_ok(mut self, track: &serve::TrackReader) -> Result<(), SessionError> {
        use crate::coding::{Encode, KeyValuePairs};
        use crate::data::ParameterType;

        let mut params = KeyValuePairs::new();

        if let Some(largest) = track.largest_location() {
            let mut buf = Vec::new();
            largest.encode(&mut buf).ok();
            params.set_bytesvalue(ParameterType::LargestObject.into(), buf);
        }

        self.publisher.send_message(message::RequestOk {
            id: self.request_msg.id,
            params,
        });

        Ok(())
    }
}
