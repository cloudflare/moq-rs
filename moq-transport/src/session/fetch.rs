// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::{Arc, Mutex};

use crate::{
    coding::{ReasonPhrase, VarInt},
    data::{FetchRecord, FetchRecordDecoder, FetchRecordObject},
    message::{self, FetchOk},
    serve::ServeError,
    watch::State,
};

use super::{FetchValidator, Reader, Subscriber};

/// Exact REQUEST_ERROR metadata for a rejected FETCH.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRejection {
    error_code: u64,
    retry_interval: u64,
    reason: ReasonPhrase,
}

impl FetchRejection {
    /// Construct a FETCH rejection using the exact wire retry interval.
    pub fn new(
        code: message::RequestErrorCode,
        retry_interval: u64,
        reason: impl Into<String>,
    ) -> Result<Self, ServeError> {
        VarInt::try_from(retry_interval).map_err(|_| ServeError::Size)?;
        let reason = reason.into();
        if reason.len() > ReasonPhrase::MAX_LEN {
            return Err(ServeError::Size);
        }
        Ok(Self {
            error_code: code as u64,
            retry_interval,
            reason: ReasonPhrase(reason),
        })
    }

    pub fn error_code(&self) -> u64 {
        self.error_code
    }

    pub fn retry_interval(&self) -> u64 {
        self.retry_interval
    }

    pub fn reason(&self) -> &ReasonPhrase {
        &self.reason
    }

    pub(crate) fn from_message(error: &message::RequestError) -> Self {
        Self {
            error_code: error.error_code,
            retry_interval: error.retry_interval,
            reason: error.reason.clone(),
        }
    }

    pub(crate) fn into_message(self, id: u64) -> message::RequestError {
        message::RequestError {
            id,
            error_code: self.error_code,
            retry_interval: self.retry_interval,
            reason: self.reason,
        }
    }
}

struct FetchState {
    reader: Option<Reader>,
    ok: Option<FetchOk>,
    request_error: Option<message::RequestError>,
    closed: Result<(), ServeError>,
    stream_received: bool,
    cancel_sent: bool,
}

impl Default for FetchState {
    fn default() -> Self {
        Self {
            reader: None,
            ok: None,
            request_error: None,
            closed: Ok(()),
            stream_received: false,
            cancel_sent: false,
        }
    }
}

/// An outbound Standalone or Joining FETCH.
#[must_use = "dropping a FETCH sends FETCH_CANCEL"]
pub struct Fetch {
    subscriber: Subscriber,
    state: State<FetchState>,
    reader: Option<Reader>,
    decoder: FetchRecordDecoder,
    payload_remaining: u64,
    body_mode: FetchBodyMode,
    validator: Arc<Mutex<FetchValidator>>,
    session_lifetime: State<()>,
    stream_done: bool,
    id: u64,
    pub request: message::Fetch,
}

#[derive(Default)]
enum FetchBodyMode {
    #[default]
    Undecided,
    Raw,
    Records,
}

pub(crate) struct FetchRecv {
    state: State<FetchState>,
    validator: Arc<Mutex<FetchValidator>>,
}

impl Fetch {
    pub(super) fn new(
        subscriber: Subscriber,
        request: message::Fetch,
        range: Option<(crate::coding::Location, crate::coding::Location)>,
        session_lifetime: State<()>,
    ) -> (Self, FetchRecv) {
        let id = request.id;
        let (send, recv) = State::default().split();
        let validator = Arc::new(Mutex::new(FetchValidator::new(
            range,
            request.params.group_order().ok().flatten(),
        )));
        (
            Self {
                subscriber,
                state: send,
                reader: None,
                decoder: FetchRecordDecoder::default(),
                payload_remaining: 0,
                body_mode: FetchBodyMode::default(),
                validator: validator.clone(),
                session_lifetime,
                stream_done: false,
                id,
                request,
            },
            FetchRecv {
                state: recv,
                validator,
            },
        )
    }

    pub async fn ok(&self) -> Result<FetchOk, ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if let Some(ok) = &state.ok {
                    return Ok(ok.clone());
                }
                state.modified()
            };
            match notify {
                Some(notify) => {
                    tokio::select! {
                        _ = session_closed(self.session_lifetime.clone()) => return Err(ServeError::Cancel),
                        _ = notify => {},
                    }
                }
                None => return Err(ServeError::Done),
            }
        }
    }

    pub fn request_error(&self) -> Option<message::RequestError> {
        self.state.lock().request_error.clone()
    }

    pub fn rejection(&self) -> Option<FetchRejection> {
        self.state
            .lock()
            .request_error
            .as_ref()
            .map(FetchRejection::from_message)
    }

    /// Decode the next semantic record from the FETCH stream.
    ///
    /// Object payload bytes must be fully consumed with
    /// [`Self::read_payload_chunk`] before requesting another record.
    pub async fn next(&mut self) -> Result<Option<FetchRecord>, super::SessionError> {
        if self.payload_remaining != 0 {
            return Err(ServeError::Size.into());
        }
        match self.body_mode {
            FetchBodyMode::Undecided => self.body_mode = FetchBodyMode::Records,
            FetchBodyMode::Records => {}
            FetchBodyMode::Raw => return Err(ServeError::Mode.into()),
        }
        self.ensure_reader().await?;
        let state = self.state.clone();
        let reader = self.reader.as_mut().ok_or(ServeError::Done)?;
        let record = tokio::select! {
            _ = session_closed(self.session_lifetime.clone()) => return Err(ServeError::Cancel.into()),
            result = reader.decode_fetch(&mut self.decoder) => match result {
                Ok(record) => record,
                Err(error) => {
                    // Draft-16 section 10.4.4.1 requires a first FETCH Object
                    // that references prior fields to close the session with
                    // PROTOCOL_VIOLATION, so serialization failures are fatal.
                    if matches!(error, super::SessionError::Decode(_) | super::SessionError::WrongSize) {
                        self.subscriber.report_fatal(error.clone());
                    }
                    return Err(error);
                }
            },
            err = wait_closed(state) => return Err(err.into()),
        };
        let Some(record) = record else {
            self.stream_done = true;
            return Ok(None);
        };
        let validation = self
            .validator
            .lock()
            .map_err(|_| super::SessionError::Internal)?
            .validate_record(&record);
        if let Err(error) = validation {
            self.cancel_once();
            return Err(error);
        }
        if let FetchRecord::Object(FetchRecordObject { payload_length, .. }) = &record {
            self.payload_remaining = *payload_length;
        }
        Ok(Some(record))
    }

    /// Read at most `max` payload bytes for the current Object.
    pub async fn read_payload_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, super::SessionError> {
        if self.payload_remaining == 0 {
            return Ok(None);
        }
        if max == 0 {
            return Err(ServeError::Size.into());
        }
        if !matches!(self.body_mode, FetchBodyMode::Records) {
            return Err(ServeError::Mode.into());
        }
        self.ensure_reader().await?;
        let limit = usize::try_from(self.payload_remaining.min(max as u64))
            .map_err(|_| ServeError::Size)?;
        let state = self.state.clone();
        let reader = self.reader.as_mut().ok_or(ServeError::Done)?;
        let chunk = tokio::select! {
            _ = session_closed(self.session_lifetime.clone()) => return Err(ServeError::Cancel.into()),
            result = reader.read_chunk(limit) => result?,
            err = wait_closed(state) => return Err(err.into()),
        };
        let Some(chunk) = chunk else {
            let error = super::SessionError::WrongSize;
            self.subscriber.report_fatal(error.clone());
            return Err(error);
        };
        self.payload_remaining -= chunk.len() as u64;
        Ok(Some(chunk))
    }

    pub(super) async fn read_stream_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, super::SessionError> {
        match self.body_mode {
            FetchBodyMode::Undecided => self.body_mode = FetchBodyMode::Raw,
            FetchBodyMode::Raw => {}
            FetchBodyMode::Records => return Err(ServeError::Mode.into()),
        }
        self.ensure_reader().await?;
        let state = self.state.clone();
        let reader = self.reader.as_mut().ok_or(ServeError::Done)?;
        let chunk = tokio::select! {
            _ = session_closed(self.session_lifetime.clone()) => return Err(ServeError::Cancel.into()),
            result = reader.read_chunk(max) => result?,
            err = wait_closed(state) => return Err(err.into()),
        };
        if chunk.is_none() {
            self.stream_done = true;
        }
        Ok(chunk)
    }

    async fn ensure_reader(&mut self) -> Result<(), ServeError> {
        while self.reader.is_none() {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if state.reader.is_some() {
                    self.reader = state.into_mut().and_then(|mut state| state.reader.take());
                    continue;
                }
                state.modified().ok_or(ServeError::Done)?
            };
            tokio::select! {
                _ = session_closed(self.session_lifetime.clone()) => return Err(ServeError::Cancel),
                _ = notify => {},
            }
        }
        Ok(())
    }

    fn cancel_once(&mut self) {
        let send = self.state.lock_mut().is_some_and(|mut state| {
            let send = !state.cancel_sent;
            state.cancel_sent = true;
            send
        });
        if send {
            self.subscriber
                .send_message(message::FetchCancel { id: self.id });
        }
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        let send_cancel = self.state.lock_mut().is_some_and(|mut state| {
            let send = !(self.stream_done && state.ok.is_some())
                && state.closed.is_ok()
                && !state.cancel_sent;
            state.cancel_sent |= send;
            send
        });
        if send_cancel {
            self.subscriber
                .send_message(message::FetchCancel { id: self.id });
        }
        self.subscriber.remove_fetch(self.id);
    }
}

impl FetchRecv {
    pub fn recv_ok(&mut self, ok: &FetchOk) -> Result<(), super::SessionError> {
        self.validator
            .lock()
            .map_err(|_| super::SessionError::Internal)?
            .validate_ok(ok.end_location, &ok.track_extensions)?;
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.ok.is_some() || state.request_error.is_some() {
            return Err(super::SessionError::ProtocolViolation(
                "received multiple terminal FETCH responses".to_string(),
            ));
        }
        state.ok = Some(ok.clone());
        Ok(())
    }

    pub fn recv_error(&mut self, error: &message::RequestError) -> Result<(), super::SessionError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(());
        };
        if state.ok.is_some() || state.request_error.is_some() {
            return Err(super::SessionError::ProtocolViolation(
                "received multiple terminal FETCH responses".to_string(),
            ));
        }
        state.request_error = Some(error.clone());
        state.closed = Err(ServeError::Closed(error.error_code));
        Ok(())
    }

    pub fn recv_timeout(&mut self, err: ServeError) -> Result<bool, ServeError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(false);
        };
        state.closed = Err(err);
        let send_cancel = !state.cancel_sent;
        state.cancel_sent = true;
        Ok(send_cancel)
    }

    pub fn recv_stream(&mut self, reader: Reader) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.stream_received {
            return Err(ServeError::Duplicate);
        }
        state.stream_received = true;
        state.reader = Some(reader);
        Ok(())
    }
}

async fn wait_closed(state: State<FetchState>) -> ServeError {
    loop {
        let notify = {
            let state = state.lock();
            if let Err(err) = &state.closed {
                return err.clone();
            }
            state.modified()
        };
        match notify {
            Some(notify) => notify.await,
            None => return ServeError::Done,
        }
    }
}

async fn session_closed(state: State<()>) {
    loop {
        let state = state.lock();
        match state.modified() {
            Some(changed) => changed.await,
            None => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use crate::{
        coding::{KeyValuePairs, Location, TrackNamespace},
        message::{Message, StandaloneFetch},
        session::{PendingRequest, PendingRequests, RequestId, SessionId},
        watch::Queue,
    };

    use super::*;

    #[test]
    fn rejection_metadata_is_bounded_before_use() {
        assert!(FetchRejection::new(
            message::RequestErrorCode::DoesNotExist,
            VarInt::MAX.into_inner() + 1,
            "retry",
        )
        .is_err());
        assert!(FetchRejection::new(
            message::RequestErrorCode::DoesNotExist,
            0,
            "x".repeat(ReasonPhrase::MAX_LEN + 1),
        )
        .is_err());
    }

    #[tokio::test]
    async fn session_shutdown_wakes_fetch_waiting_for_stream() {
        let subscriber = Subscriber::new(
            Queue::default(),
            Queue::default(),
            None,
            RequestId::new(0, 100, 100, 0),
            PendingRequests::default(),
            SessionId::generate(),
        );
        let request = message::Fetch {
            id: 0,
            fetch_type: message::FetchType::Standalone,
            standalone_fetch: Some(StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test"),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: KeyValuePairs::default(),
        };
        let (lifetime, owner) = State::<()>::default().split();
        let (mut fetch, _recv) = Fetch::new(
            subscriber,
            request,
            Some((Location::new(0, 0), Location::new(1, 0))),
            lifetime,
        );

        drop(owner);

        assert!(matches!(
            fetch.next().await,
            Err(crate::session::SessionError::Serve(ServeError::Cancel))
        ));
    }

    #[test]
    fn request_error_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            validator: Arc::new(Mutex::new(FetchValidator::new(None, None))),
        };
        drop(fetch);

        let error =
            message::RequestError::new(0, message::RequestErrorCode::InternalError, 0, "failed");
        assert!(recv.recv_error(&error).is_ok());
    }

    #[test]
    fn timeout_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            validator: Arc::new(Mutex::new(FetchValidator::new(None, None))),
        };
        drop(fetch);

        assert_eq!(recv.recv_timeout(ServeError::Cancel), Ok(false));
    }

    #[tokio::test]
    async fn timeout_racing_fetch_drop_sends_one_cancel() {
        for _ in 0..100 {
            let (outgoing, mut receiver) = Queue::default().split();
            let keepalive = outgoing.clone();
            let mut subscriber = Subscriber::new(
                outgoing,
                Queue::default(),
                None,
                RequestId::new(0, 100, 100, 0),
                PendingRequests::default(),
                SessionId::generate(),
            );
            let fetch = subscriber
                .fetch(
                    StandaloneFetch {
                        track_namespace: TrackNamespace::from_utf8_path("test"),
                        track_name: "video".into(),
                        start_location: Location::new(0, 0),
                        end_location: Location::new(1, 0),
                    },
                    KeyValuePairs::default(),
                )
                .unwrap();
            let Message::Fetch(request) = receiver.pop().await.unwrap() else {
                panic!("expected FETCH");
            };
            let barrier = Arc::new(Barrier::new(3));
            let drop_barrier = barrier.clone();
            let dropper = std::thread::spawn(move || {
                drop_barrier.wait();
                drop(fetch);
            });
            let timeout_barrier = barrier.clone();
            let timeout = std::thread::spawn(move || {
                timeout_barrier.wait();
                subscriber
                    .recv_request_timeout(request.id, PendingRequest::Fetch)
                    .unwrap();
            });
            barrier.wait();
            dropper.join().unwrap();
            timeout.join().unwrap();

            let cancels = receiver
                .close()
                .into_iter()
                .filter(|message| matches!(message, Message::FetchCancel(_)))
                .count();
            assert_eq!(cancels, 1);
            drop(keepalive);
        }
    }
}
