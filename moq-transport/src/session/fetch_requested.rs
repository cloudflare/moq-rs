// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Publisher side of FETCH (draft-18 §10.12, §10.13, §11.4.4).
//!
//! A peer's FETCH arrives as the first message on a bidi request stream. The
//! session registers the stream in the bidi response map and surfaces a
//! [`FetchRequested`] to the application, which either rejects it or answers
//! with FETCH_OK plus a unidirectional FETCH data stream.
//!
//! Draft-18 differs from draft-16 here in three ways that shape this module:
//!
//! * FETCH_OK and REQUEST_ERROR travel on the request stream, not the control
//!   stream, and omit the Request ID (§10). They are pushed onto the outgoing
//!   queue, which routes them to the request stream by ID.
//! * There is no FETCH_CANCEL (§A.2, "remove cancel messages"). A requester
//!   cancels with STOP_SENDING / RESET_STREAM on the request stream (§5.2),
//!   which the session turns into [`FetchRequestedRecv::cancel`], or by
//!   STOP_SENDING the data stream, which surfaces as a write error. The
//!   request stream therefore stays open until the response is finished or
//!   reset, not just until FETCH_OK.
//! * FETCH objects are delta encoded against the group order (§11.4.4.1), so
//!   the writer's encoder is created with the request's GROUP_ORDER.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    coding::{KeyValuePairs, Location},
    data::{DataStreamResetCode, FetchHeader, FetchRecord, FetchRecordEncoder, StreamHeaderType},
    message::{self, GroupOrder, Message, RequestErrorCode, TrackExtensions},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{Fetch, FetchValidator, SessionError, Writer};

/// Chunk size for copying an upstream FETCH body in [`FetchRequested::proxy`].
const COPY_CHUNK_SIZE: usize = 64 * 1024;

enum ProxyOutcome {
    Done(Result<message::FetchOk, SessionError>),
    Cancelled(ServeError),
    TimedOut,
}

#[derive(Debug)]
struct FetchRequestedState {
    closed: Result<(), ServeError>,
    /// FETCH_OK or REQUEST_ERROR has been queued; exactly one is allowed (§5.2).
    responded: bool,
}

impl Default for FetchRequestedState {
    fn default() -> Self {
        Self {
            closed: Ok(()),
            responded: false,
        }
    }
}

/// FETCH_OK fields supplied by the application. The Request ID is owned by
/// the transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchOkInfo {
    /// Whether the response reaches the final Object in the Track.
    pub end_of_track: bool,
    /// End Location in FETCH request encoding: last Object plus one, or
    /// `{group, 0}` for a whole group (§10.13).
    pub end_location: Location,
    /// FETCH_OK parameters.
    pub params: KeyValuePairs,
    /// Track Properties accompanying FETCH_OK.
    pub track_extensions: TrackExtensions,
}

/// An inbound FETCH awaiting an application decision.
///
/// Dropping it without responding sends REQUEST_ERROR(INTERNAL_ERROR), so the
/// requester always receives exactly one response (§5.2).
#[must_use = "respond to, reject, or proxy the FETCH request"]
pub struct FetchRequested {
    webtransport: web_transport::Session,
    outgoing: Queue<Message>,
    active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    state: State<FetchRequestedState>,
    id: u64,
    /// The FETCH as received.
    pub request: message::Fetch,
}

/// Session-side half, held in the publisher's active-FETCH map so request
/// stream teardown can cancel the request.
pub(crate) struct FetchRequestedRecv {
    state: State<FetchRequestedState>,
}

impl FetchRequestedRecv {
    /// The requester cancelled: it reset or stopped the request stream (§5.2).
    ///
    /// Returns whether the request was still live. Before a response this
    /// suppresses the REQUEST_ERROR a dropped [`FetchRequested`] would send;
    /// after FETCH_OK it makes the response writer stop and reset the data
    /// stream with CANCELLED.
    pub fn cancel(&mut self) -> bool {
        let Some(mut state) = self.state.lock_mut() else {
            return false;
        };
        if state.closed.is_err() {
            return false;
        }
        state.closed = Err(ServeError::Cancel);
        true
    }

    /// Resolves once the application half is gone: the response was finished
    /// or reset, the request was rejected, or the [`FetchRequested`] dropped.
    ///
    /// The request-stream handler waits for this before it FINs, so the
    /// request stream (and its cancellation path) lives as long as the
    /// response is being served.
    pub fn served(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let state = self.state.clone();
        async move {
            loop {
                let notify = state.lock().modified();
                match notify {
                    Some(notify) => notify.await,
                    None => return,
                }
            }
        }
    }
}

impl FetchRequested {
    pub(super) fn new(
        webtransport: web_transport::Session,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
        request: message::Fetch,
    ) -> (Self, FetchRequestedRecv) {
        let (send, recv) = State::default().split();
        (
            Self {
                webtransport,
                outgoing,
                active,
                state: send,
                id: request.id,
                request,
            },
            FetchRequestedRecv { state: recv },
        )
    }

    /// The Request ID of this FETCH.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The effective group order of the response (§10.2.8 defaults to Ascending).
    pub fn group_order(&self) -> Option<GroupOrder> {
        // Validated on receipt, so an invalid value cannot reach here.
        self.request.params.group_order().ok().flatten()
    }

    /// Resolve the request to the standalone range it covers.
    ///
    /// Standalone FETCH resolves immediately. Joining FETCH is not served yet:
    /// it is answered with REQUEST_ERROR(NOT_SUPPORTED) and `None` is returned.
    pub async fn resolve(&self) -> Result<Option<message::StandaloneFetch>, SessionError> {
        match self.request.fetch_type {
            message::FetchType::Standalone => self
                .request
                .standalone_fetch
                .clone()
                .map(Some)
                .ok_or(SessionError::Internal),
            message::FetchType::RelativeJoining | message::FetchType::AbsoluteJoining => {
                self.send_resolution_error(RequestErrorCode::NotSupported, "joining fetch");
                Ok(None)
            }
        }
    }

    /// Wait until the request is cancelled or the session closes.
    ///
    /// Returns the terminal error. This never resolves for a request that is
    /// still wanted, so callers race it against their own work.
    pub async fn closed(&self) -> ServeError {
        tokio::select! {
            err = wait_state_closed(&self.state) => err,
            _ = self.webtransport.closed() => ServeError::Cancel,
        }
    }

    fn is_closed(&self) -> bool {
        self.state.lock().closed.is_err()
    }

    /// Reject the FETCH with REQUEST_ERROR.
    pub fn reject(self, code: RequestErrorCode, reason: &str) -> Result<(), ServeError> {
        self.claim_response()?;
        self.send(message::RequestError::new(self.id, code, 0, reason).into())
    }

    /// Reject with an exact upstream REQUEST_ERROR, rewritten to this request.
    pub fn reject_with(self, mut error: message::RequestError) -> Result<(), ServeError> {
        self.claim_response()?;
        error.id = self.id;
        self.send(error.into())
    }

    /// Send FETCH_OK and open the data stream for the response body.
    ///
    /// The range the writer accepts is the resolved request range, bounded
    /// further by `info.end_location`.
    pub async fn respond(self, info: FetchOkInfo) -> Result<FetchWriter, SessionError> {
        let range = self.standalone_range()?;
        let mut validator = FetchValidator::new(Some(range), self.group_order());
        validator.validate_ok(info.end_location, &info.track_extensions)?;
        let stream = self.open_stream().await?;
        self.commit_ok(message::FetchOk {
            id: self.id,
            end_of_track: info.end_of_track,
            end_location: info.end_location,
            params: info.params,
            track_extensions: info.track_extensions,
        })?;
        let encoder = FetchRecordEncoder::new(self.group_order());
        Ok(FetchWriter {
            stream,
            encoder,
            validator,
            payload_remaining: 0,
            request: self,
        })
    }

    /// Answer by copying an upstream FETCH response.
    ///
    /// The upstream body is relayed byte for byte behind a fresh FETCH_HEADER,
    /// which is valid because the upstream request carries the same GROUP_ORDER
    /// (the delta encoding depends on it, §11.4.4.1). FETCH_OK is sent once the
    /// upstream's FETCH_OK is known and the body has been copied; an upstream
    /// REQUEST_ERROR is forwarded, and any already-written body is reset
    /// (§10.12.3 lets a relay deliver objects before learning the result).
    pub async fn proxy(self, mut upstream: Fetch, timeout: Duration) -> Result<(), SessionError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut stream = None;
        let outcome = {
            let copy = self.proxy_body(&mut upstream, &mut stream);
            tokio::pin!(copy);
            tokio::select! {
                biased;
                err = self.closed() => ProxyOutcome::Cancelled(err),
                _ = tokio::time::sleep_until(deadline) => ProxyOutcome::TimedOut,
                result = &mut copy => ProxyOutcome::Done(result),
            }
        };

        match outcome {
            ProxyOutcome::Done(Ok(ok)) => {
                let mut stream = stream.ok_or(SessionError::Internal)?;
                // FETCH_OK first, then FIN: a subscriber that sees FIN before
                // FETCH_OK cannot yet tell where the response ends.
                self.commit_ok(message::FetchOk {
                    id: self.id,
                    params: KeyValuePairs::default(),
                    ..ok
                })?;
                stream.finish()?;
                Ok(())
            }
            ProxyOutcome::Cancelled(err) => {
                if let Some(stream) = stream.as_mut() {
                    stream.reset = DataStreamResetCode::Cancelled;
                }
                Err(err.into())
            }
            ProxyOutcome::TimedOut => {
                if let Some(stream) = stream.as_mut() {
                    stream.reset = DataStreamResetCode::DeliveryTimeout;
                }
                self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                Err(ServeError::Timeout.into())
            }
            ProxyOutcome::Done(Err(err)) => {
                // Any opened stream keeps its INTERNAL_ERROR reset code: the
                // body copy failed partway, and the requester must not take
                // the truncated body as complete.
                if let Some(error) = upstream.rejection() {
                    self.reject_with(error)?;
                } else {
                    self.reject(RequestErrorCode::InternalError, "fetch proxy failed")?;
                }
                Err(err)
            }
        }
    }

    async fn proxy_body(
        &self,
        upstream: &mut Fetch,
        stream: &mut Option<FetchStream>,
    ) -> Result<message::FetchOk, SessionError> {
        // Wait for the upstream decision before opening anything downstream,
        // so an upstream rejection is forwarded without an orphan data stream.
        let ok = upstream.ok().await?;
        let out = stream.insert(self.open_stream().await?);
        while let Some(chunk) = upstream.read_stream_chunk(COPY_CHUNK_SIZE).await? {
            out.writer.write(&chunk).await?;
        }
        Ok(ok)
    }

    fn standalone_range(&self) -> Result<(Location, Location), SessionError> {
        let standalone = self
            .request
            .standalone_fetch
            .as_ref()
            .ok_or(SessionError::Serve(ServeError::Mode))?;
        Ok((standalone.start_location, standalone.end_location))
    }

    async fn open_stream(&self) -> Result<FetchStream, SessionError> {
        if self.is_closed() {
            return Err(ServeError::Cancel.into());
        }
        let mut stream = FetchStream {
            writer: Writer::new(self.webtransport.open_uni().await?),
            reset: DataStreamResetCode::InternalError,
            finished: false,
        };
        stream
            .writer
            .encode(&FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id: self.id,
            })
            .await?;
        Ok(stream)
    }

    fn commit_ok(&self, ok: message::FetchOk) -> Result<(), SessionError> {
        self.claim_response()?;
        self.send(ok.into())?;
        Ok(())
    }

    fn claim_response(&self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;
        if state.responded {
            return Err(ServeError::Duplicate);
        }
        // The session half may already be gone (session teardown); still
        // record locally that a response was claimed.
        let mut state = state.into_mut_closed();
        state.responded = true;
        Ok(())
    }

    fn send(&self, msg: Message) -> Result<(), ServeError> {
        self.outgoing
            .clone()
            .push(msg)
            .map_err(|_| ServeError::Cancel)
    }

    fn send_resolution_error(&self, code: RequestErrorCode, reason: &str) {
        if self.claim_response().is_ok() {
            let _ = self.send(message::RequestError::new(self.id, code, 0, reason).into());
        }
    }
}

impl Drop for FetchRequested {
    fn drop(&mut self) {
        if !self.is_closed() && self.claim_response().is_ok() {
            let _ = self.send(
                message::RequestError::new(
                    self.id,
                    RequestErrorCode::InternalError,
                    0,
                    "fetch request dropped",
                )
                .into(),
            );
        }
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.id);
        }
    }
}

async fn wait_state_closed(state: &State<FetchRequestedState>) -> ServeError {
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
            // The session half is gone; `closed()` relies on the transport's
            // own close signal from here on.
            None => std::future::pending::<()>().await,
        }
    }
}

/// One FETCH data stream; resets on drop unless finished.
struct FetchStream {
    writer: Writer,
    reset: DataStreamResetCode,
    finished: bool,
}

impl FetchStream {
    fn finish(&mut self) -> Result<(), SessionError> {
        self.writer.finish()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for FetchStream {
    fn drop(&mut self) {
        if !self.finished {
            self.writer.reset(self.reset.into());
        }
    }
}

/// Writes the body of a successful FETCH response.
///
/// Records are validated against the requested range, FETCH_OK's End
/// Location and the group order before they reach the wire. Dropping the
/// writer before [`Self::finish`] resets the data stream.
#[must_use = "finish the FETCH response stream"]
pub struct FetchWriter {
    stream: FetchStream,
    encoder: FetchRecordEncoder,
    validator: FetchValidator,
    payload_remaining: u64,
    request: FetchRequested,
}

impl FetchWriter {
    /// Write one record. An Object's payload must follow via
    /// [`Self::write_payload`] before the next record.
    pub async fn write_record(&mut self, record: &FetchRecord) -> Result<(), SessionError> {
        if self.payload_remaining != 0 {
            return Err(ServeError::Size.into());
        }
        if let Err(err) = self.validator.validate_record(record) {
            self.stream.reset = DataStreamResetCode::MalformedTrack;
            return Err(err);
        }
        let mut encoded = bytes::BytesMut::new();
        self.encoder.encode(record, &mut encoded)?;
        self.write(&encoded).await?;
        if let FetchRecord::Object(object) = record {
            self.payload_remaining = object.payload_length;
        }
        Ok(())
    }

    /// Write payload bytes for the current Object.
    pub async fn write_payload(&mut self, payload: &[u8]) -> Result<(), SessionError> {
        if payload.len() as u64 > self.payload_remaining {
            return Err(ServeError::Size.into());
        }
        self.write(payload).await?;
        self.payload_remaining -= payload.len() as u64;
        Ok(())
    }

    /// Finish the data stream with FIN (§10.12.3).
    pub async fn finish(mut self) -> Result<(), SessionError> {
        if self.payload_remaining != 0 {
            return Err(ServeError::Size.into());
        }
        self.stream.finish()
    }

    /// Reset the data stream with an explicit code, e.g. UNKNOWN_OBJECT_STATUS.
    pub fn reset(mut self, code: DataStreamResetCode) {
        self.stream.reset = code;
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), SessionError> {
        let write = self.stream.writer.write(bytes);
        let result = tokio::select! {
            biased;
            err = self.request.closed() => Err(err),
            result = write => return result,
        };
        // §5.2: a cancelled FETCH has its data stream reset.
        self.stream.reset = DataStreamResetCode::Cancelled;
        result.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;

    fn standalone(id: u64) -> message::Fetch {
        message::Fetch {
            id,
            fetch_type: message::FetchType::Standalone,
            standalone_fetch: Some(message::StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test"),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(2, 0),
            }),
            joining_fetch: None,
            params: KeyValuePairs::default(),
        }
    }

    async fn requested(
        id: u64,
    ) -> (
        FetchRequested,
        FetchRequestedRecv,
        Sent,
        Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    ) {
        let session = super::super::test_support::loopback_session().await;
        let (outgoing, outgoing_rx) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let (request, recv) =
            FetchRequested::new(session, outgoing, active.clone(), standalone(id));
        (request, recv, Sent(outgoing_rx, keepalive), active)
    }

    /// The outgoing queue, kept open after the request drops its half.
    struct Sent(Queue<Message>, #[allow(dead_code)] Queue<Message>);

    impl Sent {
        fn close(self) -> Vec<Message> {
            self.0.close()
        }
    }

    #[tokio::test]
    async fn drop_without_response_sends_internal_error() {
        let (request, _recv, rx, _) = requested(3).await;
        drop(request);
        let messages = rx.close();
        let [Message::RequestError(err)] = messages.as_slice() else {
            panic!("expected one REQUEST_ERROR, got {messages:?}");
        };
        assert_eq!(err.id, 3);
        assert_eq!(err.error_code, RequestErrorCode::InternalError as u64);
    }

    #[tokio::test]
    async fn reject_sends_exactly_one_response() {
        let (request, _recv, rx, _) = requested(5).await;
        request
            .reject(RequestErrorCode::DoesNotExist, "missing")
            .unwrap();
        let messages = rx.close();
        assert_eq!(messages.len(), 1);
        assert!(matches!(
            &messages[0],
            Message::RequestError(e) if e.error_code == RequestErrorCode::DoesNotExist as u64
        ));
    }

    #[tokio::test]
    async fn cancel_before_response_closes_and_suppresses_drop_error() {
        let (request, mut recv, rx, _) = requested(7).await;
        assert!(recv.cancel());
        assert_eq!(request.closed().await, ServeError::Cancel);
        drop(request);
        assert!(
            rx.close().is_empty(),
            "a cancelled request gets no response"
        );
    }

    /// §5.2: STOP_SENDING on the request stream cancels even after FETCH_OK.
    #[tokio::test]
    async fn cancel_after_response_still_cancels() {
        let (request, mut recv, _rx, _) = requested(9).await;
        request.claim_response().unwrap();
        assert!(recv.cancel());
        assert!(request.is_closed());
        assert!(!recv.cancel(), "a second cancel is a no-op");
    }

    #[tokio::test]
    async fn served_resolves_when_the_request_drops() {
        let (request, recv, _rx, _) = requested(15).await;
        let served = recv.served();
        tokio::pin!(served);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut served)
                .await
                .is_err(),
            "still being served"
        );
        drop(request);
        tokio::time::timeout(Duration::from_secs(2), served)
            .await
            .expect("served after the request dropped");
    }

    #[tokio::test]
    async fn drop_removes_active_entry() {
        let (request, recv, _rx, active) = requested(11).await;
        active.lock().unwrap().insert(11, recv);
        drop(request);
        assert!(active.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn joining_resolution_is_not_supported_yet() {
        let session = super::super::test_support::loopback_session().await;
        let (outgoing, rx) = Queue::default().split();
        let rx = Sent(rx, outgoing.clone());
        let mut msg = standalone(13);
        msg.fetch_type = message::FetchType::RelativeJoining;
        msg.standalone_fetch = None;
        msg.joining_fetch = Some(message::JoiningFetch {
            joining_request_id: 1,
            joining_start: 0,
        });
        let (request, _recv) = FetchRequested::new(session, outgoing, Default::default(), msg);
        assert_eq!(request.resolve().await.unwrap(), None);
        drop(request);
        let messages = rx.close();
        assert_eq!(messages.len(), 1, "one response only, even after drop");
        assert!(matches!(
            &messages[0],
            Message::RequestError(e) if e.error_code == RequestErrorCode::NotSupported as u64
        ));
    }
}
