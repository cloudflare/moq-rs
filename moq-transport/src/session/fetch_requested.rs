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
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use crate::{
    coding::{KeyValuePairs, Location},
    data::{DataStreamResetCode, FetchHeader, FetchRecord, FetchRecordEncoder, StreamHeaderType},
    message::{self, GroupOrder, Message, RequestErrorCode, TrackExtensions},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{
    joining_fetch_end_location, Fetch, FetchValidator, JoiningError, JoiningSnapshot,
    JoiningTarget, SessionError, Writer,
};

/// Chunk size for relaying upstream FETCH payloads in [`FetchRequested::proxy`].
const COPY_CHUNK_SIZE: usize = 64 * 1024;

enum ProxyOutcome {
    Done(Result<(), SessionError>),
    Cancelled(ServeError),
    TimedOut,
}

enum ProxyStep {
    Response(Result<message::FetchOk, ServeError>),
    Record(Option<FetchRecord>),
    Payload(Option<bytes::Bytes>),
}

/// The downstream side of [`FetchRequested::proxy`].
struct ProxyState {
    stream: Option<FetchStream>,
    encoder: FetchRecordEncoder,
    validator: FetchValidator,
    payload_remaining: u64,
    ok_sent: bool,
}

impl ProxyState {
    fn new(range: (Location, Location), order: Option<GroupOrder>) -> Self {
        Self {
            stream: None,
            encoder: FetchRecordEncoder::new(order),
            validator: FetchValidator::new(Some(range), order),
            payload_remaining: 0,
            ok_sent: false,
        }
    }
}

/// Whether a relay failure came from a malformed upstream response, as
/// opposed to a transport or local failure.
fn is_malformed_response(err: &SessionError) -> bool {
    matches!(
        err,
        SessionError::ProtocolViolation(_)
            | SessionError::WrongSize
            | SessionError::Serve(ServeError::Size)
    )
}

#[derive(Debug)]
struct FetchRequestedState {
    closed: Result<(), ServeError>,
    /// FETCH_OK or REQUEST_ERROR has been queued; exactly one is allowed (§5.2).
    responded: bool,
    /// The response data stream ended with FIN.
    completed: bool,
}

impl Default for FetchRequestedState {
    fn default() -> Self {
        Self {
            closed: Ok(()),
            responded: false,
            completed: false,
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
    /// The subscription a Joining FETCH references, found when it arrived.
    joining: Option<JoiningTarget>,
    /// The range [`Self::resolve`] settled on.
    resolved: OnceLock<message::StandaloneFetch>,
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
    /// Yields whether the data stream ended with FIN.
    ///
    /// The request-stream handler waits for this before it ends the request
    /// stream, so the stream (and its cancellation path) lives as long as the
    /// response is being served. A response that did not complete gets the
    /// request stream reset rather than FINned: resetting the data stream may
    /// discard its FETCH_HEADER, leaving the requester unable to tell which
    /// request it belonged to.
    pub fn served(&self) -> impl std::future::Future<Output = bool> + Send + 'static {
        let state = self.state.clone();
        async move {
            loop {
                let notify = {
                    let state = state.lock();
                    let completed = state.completed;
                    match state.modified() {
                        Some(notify) => notify,
                        None => return completed,
                    }
                };
                notify.await;
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
        joining: Option<JoiningTarget>,
    ) -> (Self, FetchRequestedRecv) {
        let (send, recv) = State::default().split();
        (
            Self {
                webtransport,
                outgoing,
                active,
                state: send,
                id: request.id,
                joining,
                resolved: OnceLock::new(),
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
    /// Standalone FETCH resolves immediately. A Joining FETCH waits until its
    /// subscription is accepted (§10.12.2: the publisher buffers it), so the
    /// caller bounds this with its own timeout. If the request cannot be
    /// served, REQUEST_ERROR is sent and `None` is returned:
    ///
    /// * INVALID_JOINING_REQUEST_ID when the subscription is unknown (a
    ///   SUBSCRIBE not yet read is waited for briefly) or ended first;
    /// * INVALID_RANGE when it has Forward State 0, had no Objects when it was
    ///   accepted, or the computed start is past the Joining Location.
    pub async fn resolve(&self) -> Result<Option<message::StandaloneFetch>, SessionError> {
        if let Some(resolved) = self.resolved.get() {
            return Ok(Some(resolved.clone()));
        }
        let resolved = match self.request.fetch_type {
            message::FetchType::Standalone => self
                .request
                .standalone_fetch
                .clone()
                .ok_or(SessionError::Internal)?,
            message::FetchType::RelativeJoining | message::FetchType::AbsoluteJoining => {
                let target = self.joining.as_ref().ok_or(SessionError::Internal)?;
                let Some(joining) = target.handle().await else {
                    self.send_resolution_error(
                        RequestErrorCode::InvalidJoiningRequestId,
                        "no such subscription",
                    );
                    return Ok(None);
                };
                let fields = self
                    .request
                    .joining_fetch
                    .as_ref()
                    .ok_or(SessionError::Internal)?;
                let snapshot = match joining.snapshot().await {
                    Ok(snapshot) => snapshot,
                    Err(JoiningError::NotJoinable) => {
                        self.send_resolution_error(
                            RequestErrorCode::InvalidJoiningRequestId,
                            "joined subscription is not active",
                        );
                        return Ok(None);
                    }
                    Err(JoiningError::NotForwarding) => {
                        self.send_resolution_error(
                            RequestErrorCode::InvalidRange,
                            "joined subscription has Forward State 0",
                        );
                        return Ok(None);
                    }
                };
                match resolve_joining_range(
                    &snapshot,
                    self.request.fetch_type,
                    fields.joining_start,
                ) {
                    Some(range) => range,
                    None => {
                        self.send_resolution_error(
                            RequestErrorCode::InvalidRange,
                            "invalid joining FETCH range",
                        );
                        return Ok(None);
                    }
                }
            }
        };
        Ok(Some(self.resolved.get_or_init(|| resolved).clone()))
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

    /// Record that the data stream ended with FIN.
    fn mark_completed(&self) {
        self.state.lock().into_mut_closed().completed = true;
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

    /// Answer by relaying an upstream FETCH response.
    ///
    /// FETCH_OK is forwarded as soon as the upstream sends it, while the body
    /// is relayed concurrently (§10.12.3 allows either order), so neither
    /// side waits on the other and bodies larger than a stream window flow.
    /// Upstream records are decoded and validated on this hop, where this
    /// endpoint is the subscriber (§11.4.4.1), then re-encoded downstream;
    /// payloads are streamed in chunks.
    ///
    /// An upstream REQUEST_ERROR that arrives before FETCH_OK is forwarded,
    /// and any body already written is reset. A failure after FETCH_OK
    /// cannot send a second response, so it resets the data stream instead:
    /// MALFORMED_TRACK for a malformed upstream body, DELIVERY_TIMEOUT on
    /// timeout, INTERNAL_ERROR otherwise. A malformed upstream body is never
    /// session-fatal downstream.
    pub async fn proxy(self, mut upstream: Fetch, timeout: Duration) -> Result<(), SessionError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut relay = ProxyState::new(self.standalone_range()?, self.group_order());
        let outcome = {
            let copy = self.proxy_body(&mut upstream, &mut relay);
            tokio::pin!(copy);
            tokio::select! {
                biased;
                err = self.closed() => ProxyOutcome::Cancelled(err),
                _ = tokio::time::sleep_until(deadline) => ProxyOutcome::TimedOut,
                result = &mut copy => ProxyOutcome::Done(result),
            }
        };

        match outcome {
            ProxyOutcome::Done(Ok(())) => {
                let mut stream = relay.stream.ok_or(SessionError::Internal)?;
                stream.finish()?;
                self.mark_completed();
                Ok(())
            }
            ProxyOutcome::Cancelled(err) => {
                if let Some(stream) = relay.stream.as_mut() {
                    stream.reset = DataStreamResetCode::Cancelled;
                }
                Err(err.into())
            }
            ProxyOutcome::TimedOut => {
                if relay.ok_sent {
                    self.reset_after_ok(&mut relay, DataStreamResetCode::DeliveryTimeout)
                        .await;
                } else {
                    if let Some(stream) = relay.stream.as_mut() {
                        stream.reset = DataStreamResetCode::DeliveryTimeout;
                    }
                    self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                }
                Err(ServeError::Timeout.into())
            }
            ProxyOutcome::Done(Err(err)) => {
                let code = if upstream.properties_error() == Some(RequestErrorCode::MalformedTrack)
                    || is_malformed_response(&err)
                {
                    DataStreamResetCode::MalformedTrack
                } else if upstream.properties_error().is_some() {
                    DataStreamResetCode::Cancelled
                } else {
                    DataStreamResetCode::InternalError
                };
                if relay.ok_sent {
                    self.reset_after_ok(&mut relay, code).await;
                } else {
                    if let Some(stream) = relay.stream.as_mut() {
                        stream.reset = code;
                    }
                    if let Some(code) = upstream.properties_error() {
                        self.reject(code, "unsupported or malformed upstream track properties")?;
                    } else if let Some(error) = upstream.rejection() {
                        self.reject_with(error)?;
                    } else {
                        self.reject(RequestErrorCode::InternalError, "fetch proxy failed")?;
                    }
                }
                Err(err)
            }
        }
    }

    /// After FETCH_OK the requester expects a data stream; make sure there
    /// is one so it learns the response failed rather than waiting for it.
    async fn reset_after_ok(&self, relay: &mut ProxyState, code: DataStreamResetCode) {
        if relay.stream.is_none() {
            relay.stream = self.open_stream().await.ok();
        }
        if let Some(stream) = relay.stream.as_mut() {
            stream.reset = code;
        }
    }

    async fn proxy_body(
        &self,
        upstream: &mut Fetch,
        relay: &mut ProxyState,
    ) -> Result<(), SessionError> {
        let response = upstream.response();
        tokio::pin!(response);
        let mut responded = false;
        let mut body_done = false;
        while !(responded && body_done) {
            // Only upstream reads race FETCH_OK; downstream writes below run
            // to completion so a frame is never cut short.
            let step = {
                let read = async {
                    if relay.payload_remaining > 0 {
                        upstream
                            .read_payload_chunk(COPY_CHUNK_SIZE)
                            .await
                            .map(ProxyStep::Payload)
                    } else {
                        upstream.next().await.map(ProxyStep::Record)
                    }
                };
                tokio::select! {
                    biased;
                    response = &mut response, if !responded => ProxyStep::Response(response),
                    step = read, if !body_done => step?,
                }
            };
            match step {
                ProxyStep::Response(Ok(ok)) => {
                    responded = true;
                    relay
                        .validator
                        .validate_ok(ok.end_location, &ok.track_extensions)?;
                    self.commit_ok(message::FetchOk {
                        id: self.id,
                        params: KeyValuePairs::default(),
                        ..ok
                    })?;
                    relay.ok_sent = true;
                    if relay.stream.is_none() {
                        relay.stream = Some(self.open_stream().await?);
                    }
                }
                ProxyStep::Response(Err(err)) => {
                    // A REQUEST_ERROR (forwarded by the caller) or a request
                    // stream that ended without a response.
                    return Err(err.into());
                }
                ProxyStep::Record(Some(record)) => {
                    relay.validator.validate_record(&record)?;
                    if relay.stream.is_none() {
                        relay.stream = Some(self.open_stream().await?);
                    }
                    let mut encoded = bytes::BytesMut::new();
                    relay.encoder.encode(&record, &mut encoded)?;
                    let out = relay.stream.as_mut().ok_or(SessionError::Internal)?;
                    out.writer.write(&encoded).await?;
                    if let FetchRecord::Object(object) = &record {
                        relay.payload_remaining = object.payload_length;
                    }
                }
                ProxyStep::Record(None) => body_done = true,
                ProxyStep::Payload(Some(chunk)) => {
                    relay.payload_remaining -= chunk.len() as u64;
                    let out = relay.stream.as_mut().ok_or(SessionError::Internal)?;
                    out.writer.write(&chunk).await?;
                }
                // `read_payload_chunk` only returns `None` with nothing left.
                ProxyStep::Payload(None) => relay.payload_remaining = 0,
            }
        }
        Ok(())
    }

    /// The range being answered: the standalone request, or the range a
    /// Joining FETCH resolved to. Responding before resolving a Joining FETCH
    /// is a caller error.
    fn standalone_range(&self) -> Result<(Location, Location), SessionError> {
        let standalone = match self.request.fetch_type {
            message::FetchType::Standalone => self.request.standalone_fetch.as_ref(),
            _ => self.resolved.get(),
        }
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

/// The standalone range a Joining FETCH covers (§10.12.2.1), or `None` if it
/// is an INVALID_RANGE: no Objects when the subscription was accepted, a
/// relative start reaching before group 0, or a start past the Joining
/// Location.
fn resolve_joining_range(
    snapshot: &JoiningSnapshot,
    fetch_type: message::FetchType,
    joining_start: u64,
) -> Option<message::StandaloneFetch> {
    let joining = snapshot.joining_location?;
    let start_group = match fetch_type {
        message::FetchType::RelativeJoining => joining.group_id.checked_sub(joining_start)?,
        message::FetchType::AbsoluteJoining => joining_start,
        message::FetchType::Standalone => return None,
    };
    if start_group > joining.group_id {
        return None;
    }
    Some(message::StandaloneFetch {
        track_namespace: snapshot.track_namespace.clone(),
        track_name: snapshot.track_name.clone(),
        start_location: Location::new(start_group, 0),
        end_location: joining_fetch_end_location(joining),
    })
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
        self.stream.finish()?;
        self.request.mark_completed();
        Ok(())
    }

    /// Write bytes past the validator and encoder, to simulate a broken peer.
    #[cfg(test)]
    pub(crate) async fn write_raw(&mut self, bytes: &[u8]) -> Result<(), SessionError> {
        self.write(bytes).await
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
            FetchRequested::new(session, outgoing, active.clone(), standalone(id), None);
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

    fn snapshot(joining: Option<Location>) -> JoiningSnapshot {
        JoiningSnapshot {
            track_namespace: TrackNamespace::from_utf8_path("test"),
            track_name: "video".into(),
            joining_location: joining,
        }
    }

    fn range(start: Location, end: Location) -> Option<(Location, Location)> {
        Some((start, end))
    }

    fn resolved(
        joining: Option<Location>,
        fetch_type: message::FetchType,
        joining_start: u64,
    ) -> Option<(Location, Location)> {
        resolve_joining_range(&snapshot(joining), fetch_type, joining_start)
            .map(|r| (r.start_location, r.end_location))
    }

    /// §10.12.2.1: end is `{JL.group, JL.object + 1}`; start is group-aligned.
    #[test]
    fn joining_range_is_contiguous_with_the_subscription() {
        use message::FetchType::{AbsoluteJoining, RelativeJoining};
        let jl = Some(Location::new(7, 11));
        assert_eq!(
            resolved(jl, RelativeJoining, 3),
            range(Location::new(4, 0), Location::new(7, 12))
        );
        assert_eq!(
            resolved(jl, RelativeJoining, 0),
            range(Location::new(7, 0), Location::new(7, 12))
        );
        assert_eq!(
            resolved(jl, AbsoluteJoining, 2),
            range(Location::new(2, 0), Location::new(7, 12))
        );
        assert_eq!(
            resolved(jl, AbsoluteJoining, 7),
            range(Location::new(7, 0), Location::new(7, 12))
        );
    }

    #[test]
    fn joining_range_rejects_what_cannot_be_served() {
        use message::FetchType::{AbsoluteJoining, RelativeJoining, Standalone};
        let jl = Some(Location::new(2, 3));
        // No Objects published when the subscription was accepted.
        assert_eq!(resolved(None, RelativeJoining, 0), None);
        // Relative start before group 0.
        assert_eq!(resolved(jl, RelativeJoining, 3), None);
        // Absolute start past the Joining Location.
        assert_eq!(resolved(jl, AbsoluteJoining, 3), None);
        assert_eq!(resolved(jl, Standalone, 0), None);
    }

    /// Object ID 2^64-1 has no `+1`; the whole-group encoding covers it.
    #[test]
    fn joining_range_at_max_object_uses_whole_group_end() {
        let jl = Some(Location::new(2, u64::MAX));
        assert_eq!(
            resolved(jl, message::FetchType::RelativeJoining, 0),
            range(Location::new(2, 0), Location::new(2, 0))
        );
    }
}
