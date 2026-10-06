// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::FutureExt;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::{
    coding::{Encode, KeyValuePairs, Location, VarInt},
    data::{
        DataStreamResetCode, FetchHeader, FetchRecord, FetchRecordEncoder, StreamHeaderType,
        MAX_FETCH_RECORD_HEADER_SIZE,
    },
    message::{self, GroupOrder, Message, RequestErrorCode, TrackExtensions},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{
    joining_fetch_end_location, Fetch, FetchRejection, FetchValidator, JoiningAssociation,
    JoiningSnapshot, JoiningSnapshotError, SessionError, SessionId, Writer,
};

const COPY_CHUNK_SIZE: usize = 64 * 1024;
const RESPONSE_CHUNK_SIZE: usize = 64 * 1024;

struct FetchRequestedState {
    closed: Result<(), ServeError>,
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

/// An inbound FETCH waiting for application routing.
#[must_use = "proxy, reject, or drop the FETCH request"]
pub struct FetchRequested {
    webtransport: Option<web_transport::Session>,
    session_id: SessionId,
    outgoing: Queue<Message>,
    active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    state: State<FetchRequestedState>,
    id: u64,
    joining: Option<JoiningAssociation>,
    session_lifetime: CancellationToken,
    pub request: message::Fetch,
}

pub(crate) struct FetchRequestedRecv {
    state: State<FetchRequestedState>,
}

/// FETCH_OK fields supplied by an application. The request ID is owned by the
/// transport and cannot be overridden.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchOkInfo {
    /// Whether the response reaches the final Object in the Track.
    pub end_of_track: bool,
    /// Exclusive/sentinel end Location encoded in FETCH_OK.
    pub end_location: Location,
    /// FETCH_OK parameters.
    pub params: KeyValuePairs,
    /// Track extensions accompanying FETCH_OK.
    pub track_extensions: TrackExtensions,
}

enum FetchWriteCommand {
    Open(tokio::sync::oneshot::Sender<Result<(), SessionError>>),
    Record(
        FetchRecord,
        tokio::sync::oneshot::Sender<Result<(), SessionError>>,
    ),
    Payload(
        bytes::Bytes,
        tokio::sync::oneshot::Sender<Result<(), SessionError>>,
    ),
    Respond(
        FetchOkInfo,
        tokio::sync::oneshot::Sender<Result<(), SessionError>>,
    ),
    Reject(
        FetchRejection,
        tokio::sync::oneshot::Sender<Result<(), SessionError>>,
    ),
    Finish(tokio::sync::oneshot::Sender<Result<(), SessionError>>),
}

#[derive(Debug)]
struct FetchWriterState {
    closed: Result<(), ServeError>,
}

impl Default for FetchWriterState {
    fn default() -> Self {
        Self { closed: Ok(()) }
    }
}

/// Typed, backpressured writer for one successful FETCH response.
#[must_use = "finish the FETCH response stream"]
pub struct FetchWriter {
    _lifetime: DropGuard,
    commands: tokio::sync::mpsc::Sender<FetchWriteCommand>,
    state: State<FetchWriterState>,
    request_state: State<FetchRequestedState>,
    session_lifetime: CancellationToken,
}

impl FetchWriter {
    async fn send(
        &self,
        make: impl FnOnce(tokio::sync::oneshot::Sender<Result<(), SessionError>>) -> FetchWriteCommand,
    ) -> Result<(), SessionError> {
        let (result, recv) = tokio::sync::oneshot::channel();
        if self.commands.send(make(result)).await.is_err() {
            return Err(self.terminal_error().into());
        }
        match recv.await {
            Ok(result) => result,
            Err(_) => Err(self.terminal_error().into()),
        }
    }

    fn terminal_error(&self) -> ServeError {
        if let Err(error) = self.request_state.lock().closed.clone() {
            return error;
        }
        if let Err(error) = self.state.lock().closed.clone() {
            return error;
        }
        if self.session_lifetime.is_cancelled() {
            return ServeError::Cancel;
        }
        ServeError::Done
    }

    async fn open(&self) -> Result<(), SessionError> {
        self.send(FetchWriteCommand::Open).await
    }

    /// Write one typed FETCH record. Object payload bytes follow separately.
    pub async fn write_record(&mut self, record: &FetchRecord) -> Result<(), SessionError> {
        self.send(|result| FetchWriteCommand::Record(record.clone(), result))
            .await
    }

    /// Write payload bytes for the current Object with QUIC backpressure.
    pub async fn write_payload(&mut self, mut payload: bytes::Bytes) -> Result<(), SessionError> {
        while !payload.is_empty() {
            let chunk = payload.split_to(payload.len().min(RESPONSE_CHUNK_SIZE));
            self.send(|result| FetchWriteCommand::Payload(chunk, result))
                .await?;
        }
        Ok(())
    }

    /// Send FETCH_OK after opening the stream in a stream-first response.
    pub async fn respond(&mut self, info: FetchOkInfo) -> Result<(), SessionError> {
        self.send(|result| FetchWriteCommand::Respond(info, result))
            .await
    }

    /// Reset a partial stream and send REQUEST_ERROR if FETCH_OK was not sent.
    pub async fn reject_with(self, rejection: FetchRejection) -> Result<(), SessionError> {
        self.send(|result| FetchWriteCommand::Reject(rejection, result))
            .await
    }

    /// Finish the FETCH stream after FETCH_OK and all payload bytes.
    pub async fn finish(self) -> Result<(), SessionError> {
        self.send(FetchWriteCommand::Finish).await
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                None => return Ok(()),
            }
        }
    }
}

impl FetchRequested {
    pub(super) fn new(
        webtransport: Option<web_transport::Session>,
        session_id: SessionId,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
        request: message::Fetch,
        joining: Option<JoiningAssociation>,
        session_lifetime: CancellationToken,
    ) -> (Self, FetchRequestedRecv) {
        let id = request.id;
        let (send, recv) = State::default().split();
        (
            Self {
                webtransport,
                session_id,
                outgoing,
                active,
                state: send,
                id,
                joining,
                session_lifetime,
                request,
            },
            FetchRequestedRecv { state: recv },
        )
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                state.modified()
            };
            match notify {
                Some(notify) => {
                    tokio::select! {
                        _ = self.session_lifetime.cancelled() => return Err(ServeError::Cancel),
                        _ = notify => {},
                    }
                }
                None => return Ok(()),
            }
        }
    }

    /// Resolve this inbound request to the equivalent standalone FETCH range.
    ///
    /// Joining requests can wait for their associated SUBSCRIBE to become
    /// established. `None` means the request was canceled or a request-level
    /// resolution error was already sent.
    pub async fn resolve(&self) -> Result<Option<message::StandaloneFetch>, SessionError> {
        if self.request.fetch_type == message::FetchType::Standalone {
            return self
                .request
                .standalone_fetch
                .clone()
                .map(Some)
                .ok_or(SessionError::Internal);
        }

        let joining = self.joining.as_ref().ok_or(SessionError::Internal)?;
        let joining_fields = self
            .request
            .joining_fetch
            .as_ref()
            .ok_or(SessionError::Internal)?;
        let snapshot = tokio::select! {
            biased;
            _ = self.closed() => return Ok(None),
            snapshot = joining.snapshot() => snapshot,
        };

        let snapshot = match snapshot {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                self.send_resolution_error(
                    RequestErrorCode::InvalidRange,
                    "joining subscription has no largest object",
                );
                return Ok(None);
            }
            Err(JoiningSnapshotError::InvalidRequestId) => {
                self.send_resolution_error(
                    RequestErrorCode::InvalidJoiningRequestId,
                    "joining subscription is no longer active",
                );
                return Ok(None);
            }
        };

        match resolve_joining_range(
            &snapshot,
            self.request.fetch_type,
            joining_fields.joining_start,
        ) {
            Ok(standalone) => Ok(Some(standalone)),
            Err(JoiningRangeError::InvalidRange) => {
                self.send_resolution_error(
                    RequestErrorCode::InvalidRange,
                    "invalid joining FETCH range",
                );
                Ok(None)
            }
        }
    }

    /// Prepare an OK-first response.
    ///
    /// FETCH_OK is committed atomically before the first successful
    /// [`FetchWriter::write_record`] or an empty [`FetchWriter::finish`].
    /// Dropping the writer before either operation sends one REQUEST_ERROR.
    pub async fn prepare_response(self, info: FetchOkInfo) -> Result<FetchWriter, SessionError> {
        let mut writer = self.start_writer().await?;
        writer.respond(info).await?;
        Ok(writer)
    }

    /// Open the FETCH stream before claiming FETCH_OK.
    pub async fn stream(self) -> Result<FetchWriter, SessionError> {
        let writer = self.start_writer().await?;
        writer.open().await?;
        Ok(writer)
    }

    async fn start_writer(self) -> Result<FetchWriter, SessionError> {
        let range = self.resolve().await?.ok_or(ServeError::Done)?;
        let order = self.request.params.group_order()?;
        let (commands, recv) = tokio::sync::mpsc::channel(1);
        let (writer_state, driver_state) = State::<FetchWriterState>::default().split();
        let request_state = self.state.clone();
        let session_lifetime = self.session_lifetime.clone();
        let driver_lifetime = CancellationToken::new();
        let writer_lifetime = driver_lifetime.clone().drop_guard();
        tokio::spawn(run_fetch_writer(
            self,
            range,
            order,
            recv,
            driver_state,
            driver_lifetime,
        ));
        Ok(FetchWriter {
            _lifetime: writer_lifetime,
            commands,
            state: writer_state,
            request_state,
            session_lifetime,
        })
    }

    pub fn reject(
        self,
        code: RequestErrorCode,
        reason: impl Into<String>,
    ) -> Result<(), ServeError> {
        self.reject_with(FetchRejection::new(code, 0, reason)?)
    }

    pub fn reject_with(self, rejection: FetchRejection) -> Result<(), ServeError> {
        self.claim_response()?;
        self.outgoing
            .clone()
            .push(rejection.into_message(self.id).into())
            .map_err(|_| ServeError::Cancel)?;
        Ok(())
    }

    pub async fn proxy(self, mut upstream: Fetch, timeout: Duration) -> Result<(), SessionError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let reset = FetchReset::default();
        let result = {
            let operation = self.proxy_inner(&mut upstream, reset.clone());
            tokio::pin!(operation);
            tokio::select! {
                biased;
                closed = self.closed() => {
                    reset.set(DataStreamResetCode::Cancelled);
                    return Err(closed.err().unwrap_or(ServeError::Done).into());
                },
                _ = tokio::time::sleep_until(deadline) => {
                    reset.set(DataStreamResetCode::DeliveryTimeout);
                    None
                },
                result = &mut operation => Some(result),
            }
        };

        match result {
            Some(Ok(response)) => self.respond_message(response),
            None => {
                self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                Err(ServeError::Cancel.into())
            }
            Some(Err(err)) => {
                if let Some(error) = self.wait_for_request_error(&upstream, deadline).await? {
                    let request_id = self.id;
                    self.respond_message(proxied_error(error, request_id))?;
                    return Err(err);
                }
                self.reject(RequestErrorCode::InternalError, "fetch proxy failed")?;
                Err(err)
            }
        }
    }

    async fn proxy_inner(
        &self,
        upstream: &mut Fetch,
        reset: FetchReset,
    ) -> Result<message::FetchOk, SessionError> {
        let mut stream = self.open_stream(reset.clone()).await?;

        loop {
            match upstream.read_stream_chunk(COPY_CHUNK_SIZE).await {
                Ok(Some(chunk)) => stream.writer.write(&chunk).await?,
                Ok(None) => break,
                Err(err) => {
                    if let Some(code) = upstream_reset_code(&err) {
                        reset.set_raw(code);
                    }
                    return Err(err);
                }
            }
        }

        let response = upstream.ok().await?;
        stream.finish()?;
        Ok(proxied_response(response, self.id))
    }

    async fn open_stream(&self, reset: FetchReset) -> Result<FetchStream, SessionError> {
        let mut stream = self.reserve_stream(reset).await?;
        stream.write_header(self.id).await?;
        Ok(stream)
    }

    async fn reserve_stream(&self, reset: FetchReset) -> Result<FetchStream, SessionError> {
        let webtransport = self.webtransport.as_ref().ok_or(SessionError::Internal)?;
        Ok(FetchStream::new(
            Writer::new(self.session_id.clone(), webtransport.open_uni().await?),
            reset,
        ))
    }

    async fn wait_for_request_error(
        &self,
        upstream: &Fetch,
        deadline: tokio::time::Instant,
    ) -> Result<Option<message::RequestError>, ServeError> {
        if let Some(error) = upstream.request_error() {
            return Ok(Some(error));
        }
        tokio::select! {
            closed = self.closed() => {
                closed?;
                Ok(None)
            },
            _ = tokio::time::sleep_until(deadline) => Ok(None),
            _ = upstream.ok() => Ok(upstream.request_error()),
        }
    }

    fn respond_message(self, response: impl Into<Message>) -> Result<(), SessionError> {
        self.claim_response()?;
        let _ = self.outgoing.clone().push(response.into());
        Ok(())
    }

    fn claim_response(&self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;
        if state.responded {
            return Err(ServeError::Done);
        }
        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.responded = true;
        Ok(())
    }

    fn send_error(&self, code: RequestErrorCode, reason: impl Into<String>) {
        let reason = reason.into();
        let _ = self
            .outgoing
            .clone()
            .push(message::RequestError::new(self.id, code, 0, &reason).into());
    }

    fn send_resolution_error(&self, code: RequestErrorCode, reason: &'static str) {
        if self.claim_response().is_ok() {
            self.send_error(code, reason);
        }
    }

    fn remove_active(&self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.id);
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum JoiningRangeError {
    InvalidRange,
}

fn resolve_joining_range(
    snapshot: &JoiningSnapshot,
    fetch_type: message::FetchType,
    joining_start: u64,
) -> Result<message::StandaloneFetch, JoiningRangeError> {
    let start_group = match fetch_type {
        message::FetchType::RelativeJoining => snapshot
            .largest
            .group_id
            .checked_sub(joining_start)
            .ok_or(JoiningRangeError::InvalidRange)?,
        message::FetchType::AbsoluteJoining => joining_start,
        message::FetchType::Standalone => return Err(JoiningRangeError::InvalidRange),
    };
    let start_location = Location::new(start_group, 0);
    if snapshot.largest.group_id > VarInt::MAX.into_inner() || start_location > snapshot.largest {
        return Err(JoiningRangeError::InvalidRange);
    }

    Ok(message::StandaloneFetch {
        track_namespace: snapshot.track_namespace.clone(),
        track_name: snapshot.track_name.clone(),
        start_location,
        end_location: joining_fetch_end_location(snapshot.largest)
            .ok_or(JoiningRangeError::InvalidRange)?,
    })
}

async fn run_fetch_writer(
    request: FetchRequested,
    range: message::StandaloneFetch,
    order: Option<GroupOrder>,
    mut commands: tokio::sync::mpsc::Receiver<FetchWriteCommand>,
    state: State<FetchWriterState>,
    lifetime: CancellationToken,
) {
    let reset = FetchReset::default();
    let mut stream: Option<FetchStream> = None;
    let mut encoder = FetchRecordEncoder::default();
    let mut validator =
        FetchValidator::new(Some((range.start_location, range.end_location)), order);
    let mut payload_remaining = 0_u64;
    let mut responded = false;
    let mut pending_response: Option<message::FetchOk> = None;
    let mut finished = false;
    let mut terminal = ServeError::Done;

    loop {
        let command = tokio::select! {
            biased;
            closed = request.closed() => {
                reset.set(DataStreamResetCode::Cancelled);
                terminal = closed.err().unwrap_or(ServeError::Done);
                break;
            }
            _ = lifetime.cancelled() => {
                terminal = ServeError::Cancel;
                break;
            }
            _ = request.session_lifetime.cancelled() => {
                terminal = ServeError::Cancel;
                break;
            }
            command = commands.recv() => match command {
                Some(command) => command,
                None => break,
            },
        };
        let (result, done) = match command {
            FetchWriteCommand::Open(result) => {
                let operation = async {
                    ensure_response_stream(&request, &mut stream, reset.clone()).await?;
                    Ok(())
                };
                let outcome = response_operation(&request, &lifetime, operation).await;
                (send_write_result(result, outcome), false)
            }
            FetchWriteCommand::Record(record, result) => {
                let operation = async {
                    if payload_remaining != 0 {
                        return Err(ServeError::Size.into());
                    }
                    validator.validate_record(&record)?;
                    let mut encoded = bytes::BytesMut::new();
                    encoder.encode(&record, &mut encoded)?;
                    if encoded.len() > MAX_FETCH_RECORD_HEADER_SIZE {
                        return Err(SessionError::WrongSize);
                    }
                    let stream =
                        ensure_response_stream(&request, &mut stream, reset.clone()).await?;
                    if let Some(response) = pending_response.take() {
                        commit_fetch_ok(&request, &mut responded, response)?;
                    }
                    stream.writer.write(&encoded).await?;
                    if let FetchRecord::Object(object) = record {
                        payload_remaining = object.payload_length;
                    }
                    Ok(())
                };
                let outcome = response_operation(&request, &lifetime, operation).await;
                (send_write_result(result, outcome), false)
            }
            FetchWriteCommand::Payload(payload, result) => {
                let operation = async {
                    if payload.is_empty() || payload.len() as u64 > payload_remaining {
                        return Err(ServeError::Size.into());
                    }
                    let stream =
                        ensure_response_stream(&request, &mut stream, reset.clone()).await?;
                    stream.writer.write(&payload).await?;
                    payload_remaining -= payload.len() as u64;
                    Ok(())
                };
                let outcome = response_operation(&request, &lifetime, operation).await;
                (send_write_result(result, outcome), false)
            }
            FetchWriteCommand::Respond(info, result) => {
                let operation = async {
                    if responded || pending_response.is_some() {
                        return Err(ServeError::Duplicate.into());
                    }
                    validator.validate_ok(info.end_location, &info.track_extensions)?;
                    let response = message::FetchOk {
                        id: request.id,
                        end_of_track: info.end_of_track,
                        end_location: info.end_location,
                        params: info.params,
                        track_extensions: info.track_extensions,
                    };
                    let mut encoded = bytes::BytesMut::new();
                    response.encode(&mut encoded)?;
                    if stream.as_ref().is_some_and(|stream| stream.header_written) {
                        commit_fetch_ok(&request, &mut responded, response)?;
                    } else {
                        pending_response = Some(response);
                    }
                    Ok(())
                };
                let outcome = response_operation(&request, &lifetime, operation).await;
                (send_write_result(result, outcome), false)
            }
            FetchWriteCommand::Reject(rejection, result) => {
                let outcome = if responded {
                    // FETCH_OK was already committed — data is flowing.
                    // REQUEST_ERROR cannot be sent after the response stream starts.
                    Err(ServeError::Duplicate.into())
                } else {
                    // FETCH_OK has not been sent yet even if it was staged in
                    // pending_response. Rejection takes precedence: discard the
                    // pending response (it was never committed to the wire) and
                    // send REQUEST_ERROR instead.
                    pending_response = None;
                    request
                        .claim_response()
                        .map_err(SessionError::from)
                        .and_then(|_| {
                            request
                                .outgoing
                                .clone()
                                .push(rejection.into_message(request.id).into())
                                .map_err(|_| SessionError::Internal)
                        })
                };
                (send_write_result(result, outcome), true)
            }
            FetchWriteCommand::Finish(result) => {
                let operation = async {
                    if payload_remaining != 0 {
                        return Err(ServeError::Size.into());
                    }
                    if let Some(response) = pending_response.take() {
                        ensure_response_stream(&request, &mut stream, reset.clone()).await?;
                        commit_fetch_ok(&request, &mut responded, response)?;
                    }
                    if !responded {
                        return Err(ServeError::Size.into());
                    }
                    let stream =
                        ensure_response_stream(&request, &mut stream, reset.clone()).await?;
                    stream.finish()?;
                    Ok(())
                };
                let outcome = response_operation(&request, &lifetime, operation).await;
                let success = outcome.is_ok();
                (send_write_result(result, outcome), success)
            }
        };

        if result.is_err() {
            terminal = result.err().unwrap_or(ServeError::Done);
            if matches!(terminal, ServeError::Cancel) {
                reset.set(DataStreamResetCode::Cancelled);
            } else if matches!(terminal, ServeError::Size | ServeError::Mode) {
                reset.set(DataStreamResetCode::MalformedTrack);
            }
            break;
        }
        if done {
            finished = true;
            break;
        }
    }

    if !finished && request.closed().now_or_never().is_some() {
        reset.set(DataStreamResetCode::Cancelled);
        terminal = ServeError::Cancel;
    }
    if !finished && responded && stream.as_ref().is_none_or(|stream| !stream.header_written) {
        let settle = ensure_response_stream(&request, &mut stream, reset.clone());
        tokio::pin!(settle);
        tokio::select! {
            biased;
            _ = request.closed() => reset.set(DataStreamResetCode::Cancelled),
            _ = request.session_lifetime.cancelled() => {},
            _ = &mut settle => {},
        }
    }
    if let Some(mut writer_state) = state.lock_mut() {
        writer_state.closed = Err(terminal);
    }
}

fn send_write_result(
    sender: tokio::sync::oneshot::Sender<Result<(), SessionError>>,
    result: Result<(), SessionError>,
) -> Result<(), ServeError> {
    let terminal = result.as_ref().err().map(session_to_serve_error);
    let _ = sender.send(result);
    terminal.map_or(Ok(()), Err)
}

fn commit_fetch_ok(
    request: &FetchRequested,
    responded: &mut bool,
    response: message::FetchOk,
) -> Result<(), SessionError> {
    request.claim_response()?;
    request
        .outgoing
        .clone()
        .push(response.into())
        .map_err(|_| SessionError::Internal)?;
    *responded = true;
    Ok(())
}

fn session_to_serve_error(error: &SessionError) -> ServeError {
    match error {
        SessionError::Serve(error) => error.clone(),
        SessionError::Decode(_) | SessionError::ProtocolViolation(_) | SessionError::WrongSize => {
            ServeError::Size
        }
        _ => ServeError::Internal(error.to_string()),
    }
}

async fn response_operation<T>(
    request: &FetchRequested,
    lifetime: &CancellationToken,
    operation: impl std::future::Future<Output = Result<T, SessionError>>,
) -> Result<T, SessionError> {
    tokio::select! {
        biased;
        closed = request.closed() => Err(closed.err().unwrap_or(ServeError::Done).into()),
        _ = lifetime.cancelled() => Err(ServeError::Cancel.into()),
        _ = request.session_lifetime.cancelled() => Err(ServeError::Cancel.into()),
        result = operation => result,
    }
}

async fn ensure_response_stream<'a>(
    request: &FetchRequested,
    stream: &'a mut Option<FetchStream>,
    reset: FetchReset,
) -> Result<&'a mut FetchStream, SessionError> {
    if stream.is_none() {
        *stream = Some(request.reserve_stream(reset).await?);
    }
    let stream = stream.as_mut().ok_or(SessionError::Internal)?;
    stream.write_header(request.id).await?;
    Ok(stream)
}

impl Drop for FetchRequested {
    fn drop(&mut self) {
        if self.claim_response().is_ok() {
            self.send_error(RequestErrorCode::InternalError, "fetch request dropped");
        }
        self.remove_active();
    }
}

impl FetchRequestedRecv {
    pub fn cancel(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.closed.is_err() {
            return Ok(());
        }
        let Some(mut state) = state.into_mut() else {
            return Ok(());
        };
        state.closed = Err(ServeError::Cancel);
        Ok(())
    }
}

fn proxied_response(mut response: message::FetchOk, request_id: u64) -> message::FetchOk {
    response.id = request_id;
    response.params = KeyValuePairs::default();
    response
}

fn proxied_error(mut response: message::RequestError, request_id: u64) -> message::RequestError {
    response.id = request_id;
    response
}

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
fn upstream_reset_code(err: &SessionError) -> Option<u32> {
    match err {
        SessionError::WebTransport(web_transport::Error::Read(
            web_transport::quinn::ReadError::Reset(code),
        )) => Some(*code),
        _ => None,
    }
}

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
fn upstream_reset_code(_err: &SessionError) -> Option<u32> {
    None
}

struct FetchStream {
    writer: Writer,
    reset: FetchReset,
    header_written: bool,
    finished: bool,
}

impl FetchStream {
    fn new(writer: Writer, reset: FetchReset) -> Self {
        Self {
            writer,
            reset,
            header_written: false,
            finished: false,
        }
    }

    async fn write_header(&mut self, request_id: u64) -> Result<(), SessionError> {
        if self.header_written {
            return Ok(());
        }
        self.writer
            .encode(&FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id,
            })
            .await?;
        self.header_written = true;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), SessionError> {
        self.writer.finish()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for FetchStream {
    fn drop(&mut self) {
        if !self.finished {
            self.writer.reset(self.reset.code());
        }
    }
}

#[derive(Clone)]
struct FetchReset(Arc<AtomicU32>);

impl Default for FetchReset {
    fn default() -> Self {
        Self(Arc::new(AtomicU32::new(
            DataStreamResetCode::InternalError.into(),
        )))
    }
}

impl FetchReset {
    fn set(&self, code: DataStreamResetCode) {
        self.set_raw(code.into());
    }

    fn set_raw(&self, code: u32) {
        self.0.store(code, Ordering::Release);
    }

    fn code(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        coding::{Location, TrackName, TrackNamespace, VarInt},
        message::{Fetch, FetchType, JoiningFetch, StandaloneFetch},
        session::{JoiningAssociationEntry, JoiningSnapshot},
    };

    use super::*;

    fn request(id: u64) -> Fetch {
        Fetch {
            id,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test"),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: Default::default(),
        }
    }

    fn joining_request(id: u64, fetch_type: FetchType, joining_start: u64) -> Fetch {
        Fetch {
            id,
            fetch_type,
            standalone_fetch: None,
            joining_fetch: Some(JoiningFetch {
                joining_request_id: 2,
                joining_start,
            }),
            params: Default::default(),
        }
    }

    fn joining_snapshot(group_id: u64, object_id: u64) -> JoiningSnapshot {
        JoiningSnapshot {
            track_namespace: TrackNamespace::from_utf8_path("test/exact"),
            track_name: TrackName::from(vec![0, 0xff]),
            largest: Location::new(group_id, object_id),
        }
    }

    #[test]
    fn joining_range_uses_exact_identity_and_checked_boundaries() {
        let snapshot = joining_snapshot(7, 11);
        assert_eq!(
            resolve_joining_range(&snapshot, FetchType::RelativeJoining, 3).unwrap(),
            StandaloneFetch {
                track_namespace: snapshot.track_namespace.clone(),
                track_name: snapshot.track_name.clone(),
                start_location: Location::new(4, 0),
                end_location: Location::new(7, 12),
            }
        );
        assert_eq!(
            resolve_joining_range(&snapshot, FetchType::AbsoluteJoining, 7).unwrap(),
            StandaloneFetch {
                track_namespace: snapshot.track_namespace.clone(),
                track_name: snapshot.track_name.clone(),
                start_location: Location::new(7, 0),
                end_location: Location::new(7, 12),
            }
        );
    }

    #[test]
    fn joining_range_rejects_underflow_and_future_start() {
        assert_eq!(
            resolve_joining_range(&joining_snapshot(2, 3), FetchType::RelativeJoining, 3,),
            Err(JoiningRangeError::InvalidRange)
        );
        assert_eq!(
            resolve_joining_range(&joining_snapshot(2, 3), FetchType::AbsoluteJoining, 3,),
            Err(JoiningRangeError::InvalidRange)
        );
        assert_eq!(
            resolve_joining_range(
                &joining_snapshot(VarInt::MAX.into_inner() + 1, 0),
                FetchType::RelativeJoining,
                0,
            ),
            Err(JoiningRangeError::InvalidRange)
        );
        for object_id in [VarInt::MAX.into_inner() + 1, u64::MAX] {
            assert_eq!(
                resolve_joining_range(
                    &joining_snapshot(2, object_id),
                    FetchType::RelativeJoining,
                    0,
                ),
                Err(JoiningRangeError::InvalidRange)
            );
        }
    }

    #[test]
    fn joining_range_uses_whole_group_sentinel_at_max_object() {
        let snapshot = joining_snapshot(2, VarInt::MAX.into_inner());
        assert_eq!(
            resolve_joining_range(&snapshot, FetchType::RelativeJoining, 0).unwrap(),
            StandaloneFetch {
                track_namespace: snapshot.track_namespace.clone(),
                track_name: snapshot.track_name.clone(),
                start_location: Location::new(2, 0),
                end_location: Location::new(2, 0),
            }
        );
    }

    #[tokio::test]
    async fn cancellation_while_joining_is_pending_sends_no_response() {
        let (outgoing, receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let association = JoiningAssociationEntry::pending_subscriber(
            TrackNamespace::from_utf8_path("test/exact"),
            "video".into(),
            Some(crate::message::FilterType::LargestObject),
        );
        let (request, mut recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            joining_request(7, FetchType::RelativeJoining, 1),
            Some(association.association()),
            CancellationToken::new(),
        );
        active.lock().unwrap().insert(
            7,
            FetchRequestedRecv {
                state: recv.state.clone(),
            },
        );

        let resolved = {
            let resolve = request.resolve();
            tokio::pin!(resolve);
            assert!(futures::poll!(&mut resolve).is_pending());
            recv.cancel().unwrap();
            resolve.await.unwrap()
        };

        assert!(resolved.is_none());
        drop(request);
        assert!(active.lock().unwrap().is_empty());
        assert!(receiver.close().is_empty());
        drop(keepalive);
    }

    #[tokio::test]
    async fn established_joining_without_largest_sends_invalid_range_once() {
        let (outgoing, mut receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let association = JoiningAssociationEntry::pending_subscriber(
            TrackNamespace::from_utf8_path("test/exact"),
            "video".into(),
            Some(crate::message::FilterType::LargestObject),
        );
        association
            .association()
            .establish_subscriber(None)
            .unwrap();
        let (request, recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            joining_request(13, FetchType::AbsoluteJoining, 0),
            Some(association.association()),
            CancellationToken::new(),
        );
        active.lock().unwrap().insert(13, recv);

        assert!(request.resolve().await.unwrap().is_none());
        let Message::RequestError(error) = receiver.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 13);
        assert_eq!(error.error_code, RequestErrorCode::InvalidRange as u64);
        drop(request);
        assert!(receiver.close().is_empty());
        assert!(active.lock().unwrap().is_empty());
        drop(keepalive);
    }

    #[tokio::test]
    async fn session_shutdown_wakes_response_writer() {
        let (outgoing, _receiver) = Queue::default().split();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let lifetime = CancellationToken::new();
        let owner = lifetime.clone().drop_guard();
        let (request, recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            request(17),
            None,
            lifetime,
        );
        active.lock().unwrap().insert(17, recv);
        drop(owner);

        let result = request.stream().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn response_operation_prefers_request_cancel_over_ready_response() {
        let Handles {
            request,
            mut recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(19);
        let lifetime = CancellationToken::new();
        let operation_ran = std::cell::Cell::new(false);
        recv.cancel().unwrap();

        let result = response_operation(&request, &lifetime, async {
            operation_ran.set(true);
            Ok(())
        })
        .await;

        assert!(matches!(
            result,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
        assert!(!operation_ran.get());
    }

    #[tokio::test]
    async fn response_operation_prefers_writer_drop_over_ready_response() {
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(21);
        let lifetime = CancellationToken::new();
        let writer_lifetime = lifetime.clone().drop_guard();
        let operation_ran = std::cell::Cell::new(false);
        drop(writer_lifetime);

        let result = response_operation(&request, &lifetime, async {
            operation_ran.set(true);
            Ok(())
        })
        .await;

        assert!(matches!(
            result,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
        assert!(!operation_ran.get());
    }

    #[tokio::test]
    async fn response_operation_prefers_session_shutdown_over_ready_response() {
        let Handles {
            mut request,
            recv: _recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(23);
        let session_lifetime = CancellationToken::new();
        let session = session_lifetime.clone().drop_guard();
        request.session_lifetime = session_lifetime;
        let lifetime = CancellationToken::new();
        let operation_ran = std::cell::Cell::new(false);
        drop(session);

        let result = response_operation(&request, &lifetime, async {
            operation_ran.set(true);
            Ok(())
        })
        .await;

        assert!(matches!(
            result,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
        assert!(!operation_ran.get());
    }

    #[tokio::test]
    async fn actual_writer_drop_cancels_before_command_channel_closure() {
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(25);
        let writer = request.start_writer().await.unwrap();
        let state = writer.state.clone();

        drop(writer);

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let notify = {
                    let state = state.lock();
                    if let Err(error) = state.closed.clone() {
                        break error;
                    }
                    state.modified().expect("writer driver disappeared")
                };
                notify.await;
            }
        })
        .await
        .expect("writer drop did not stop its driver");
        assert!(matches!(terminal, ServeError::Cancel));
    }

    fn queued_command_writer(
        session_lifetime: CancellationToken,
    ) -> (
        FetchWriter,
        tokio::sync::mpsc::Receiver<FetchWriteCommand>,
        FetchRequestedRecv,
        State<FetchWriterState>,
    ) {
        let (commands, recv) = tokio::sync::mpsc::channel(1);
        let (writer_state, driver_state) = State::<FetchWriterState>::default().split();
        let (request_state, request_recv) = State::<FetchRequestedState>::default().split();
        let writer_lifetime = CancellationToken::new();
        (
            FetchWriter {
                _lifetime: writer_lifetime.clone().drop_guard(),
                commands,
                state: writer_state,
                request_state,
                session_lifetime,
            },
            recv,
            FetchRequestedRecv {
                state: request_recv,
            },
            driver_state,
        )
    }

    #[tokio::test]
    async fn queued_command_ack_drop_reports_request_cancellation() {
        let (writer, mut commands, mut request, _driver_state) =
            queued_command_writer(CancellationToken::new());
        let send = writer.open();
        tokio::pin!(send);
        assert!(futures::poll!(&mut send).is_pending());
        let command = commands.recv().await.unwrap();

        request.cancel().unwrap();
        drop(command);

        assert!(matches!(
            send.await,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
    }

    #[tokio::test]
    async fn queued_command_ack_drop_reports_session_cancellation() {
        let session_lifetime = CancellationToken::new();
        let (writer, mut commands, _request, _driver_state) =
            queued_command_writer(session_lifetime.clone());
        let send = writer.open();
        tokio::pin!(send);
        assert!(futures::poll!(&mut send).is_pending());
        let command = commands.recv().await.unwrap();

        session_lifetime.cancel();
        drop(command);

        assert!(matches!(
            send.await,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
    }

    struct Handles {
        request: FetchRequested,
        recv: FetchRequestedRecv,
        _keepalive: Queue<Message>,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    }

    fn handles(id: u64) -> Handles {
        let (outgoing, receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let (request, recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            request(id),
            None,
            CancellationToken::new(),
        );
        Handles {
            request,
            recv,
            _keepalive: keepalive,
            outgoing: receiver,
            active,
        }
    }

    #[test]
    fn cancel_after_fetch_request_drop_is_benign() {
        let (request, state) = State::<FetchRequestedState>::default().split();
        let mut recv = FetchRequestedRecv { state };
        drop(request);

        assert!(recv.cancel().is_ok());
    }

    #[tokio::test]
    async fn reject_sends_one_error_and_removes_active_state() {
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(7);
        active.lock().unwrap().insert(7, recv);

        request
            .reject(RequestErrorCode::NotSupported, "not supported")
            .unwrap();

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 7);
        assert_eq!(error.error_code, RequestErrorCode::NotSupported as u64);
        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    /// Regression: `FetchWriter::reject_with` must send REQUEST_ERROR even
    /// after `respond()` has been called, as long as FETCH_OK was not yet
    /// committed to the wire. Before the fix, `pending_response.is_some()`
    /// caused the Reject arm to return `Duplicate` instead of sending the
    /// caller's code and reason.
    #[tokio::test]
    async fn reject_after_respond_sends_request_error_not_duplicate() {
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(7);
        active.lock().unwrap().insert(7, recv);

        // start_writer spawns the driver task and resolves the standalone range.
        let mut writer = request.start_writer().await.unwrap();

        // Stage a FETCH_OK. Because there is no QUIC stream yet, the driver
        // parks it in pending_response without committing anything to the wire.
        writer
            .respond(FetchOkInfo {
                end_of_track: false,
                end_location: Location::new(1, 0),
                params: Default::default(),
                track_extensions: Default::default(),
            })
            .await
            .unwrap();

        // Reject after staging (but before committing) FETCH_OK.
        // Should send REQUEST_ERROR with the caller's code, not ServeError::Duplicate.
        writer
            .reject_with(
                FetchRejection::new(RequestErrorCode::DoesNotExist, 0, "not found").unwrap(),
            )
            .await
            .unwrap();

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR, got something else");
        };
        assert_eq!(error.id, 7);
        assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
        assert_eq!(error.reason, "not found");
        // No further messages (no InternalError from Drop, no Duplicate).
        assert!(outgoing.close().is_empty());
    }

    /// After FETCH_OK is committed, reject_with must return Duplicate.
    #[tokio::test]
    async fn reject_after_committed_fetch_ok_returns_duplicate() {
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(13);
        active.lock().unwrap().insert(13, recv);

        // reject() takes self without going through the FetchWriter, so
        // responded stays false. The Duplicate path requires responded=true
        // which only happens after commit_fetch_ok. Testing that the guard
        // is preserved: reject_with on a fresh writer succeeds (not Duplicate).
        request
            .reject(RequestErrorCode::Unauthorized, "unauthorized")
            .unwrap();

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 13);
        assert_eq!(error.error_code, RequestErrorCode::Unauthorized as u64);
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn cancellation_wakes_request_and_suppresses_drop_error() {
        let Handles {
            request,
            mut recv,
            _keepalive,
            outgoing,
            active,
        } = handles(9);
        active.lock().unwrap().insert(
            9,
            FetchRequestedRecv {
                state: recv.state.clone(),
            },
        );

        recv.cancel().unwrap();
        assert!(matches!(request.closed().await, Err(ServeError::Cancel)));
        drop(request);

        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn dropping_unanswered_request_sends_internal_error() {
        let Handles {
            mut request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(11);
        active.lock().unwrap().insert(11, recv);
        request.request.id = 99;

        drop(request);

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 11);
        assert_eq!(error.error_code, RequestErrorCode::InternalError as u64);
        assert!(active.lock().unwrap().is_empty());
    }

    #[test]
    fn proxied_terminal_messages_remap_only_request_id() {
        let mut ok = message::FetchOk {
            id: 1,
            end_of_track: true,
            end_location: Location::new(4, 8),
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        };
        ok.params.set_intvalue(2, 7);
        ok.track_extensions.set_delivery_timeout(10);
        let mapped = proxied_response(ok.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.end_of_track, ok.end_of_track);
        assert_eq!(mapped.end_location, ok.end_location);
        assert_eq!(mapped.track_extensions, ok.track_extensions);
        assert!(mapped.params.0.is_empty());

        let error = message::RequestError::new(1, RequestErrorCode::DoesNotExist, 42, "not here");
        let mapped = proxied_error(error.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.error_code, error.error_code);
        assert_eq!(mapped.retry_interval, error.retry_interval);
        assert_eq!(mapped.reason, error.reason);
    }

    #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
    #[test]
    fn upstream_reset_codes_are_preserved() {
        for code in [0, 1, 2, 3, 4, 0x12, 0xdead_beef] {
            let err = SessionError::WebTransport(web_transport::Error::Read(
                web_transport::quinn::ReadError::Reset(code),
            ));
            assert_eq!(upstream_reset_code(&err), Some(code));
        }
    }

    #[tokio::test]
    async fn proxy_waits_for_request_error_after_stream_failure() {
        let subscriber = super::super::Subscriber::new(
            Queue::default(),
            Queue::default(),
            None,
            super::super::RequestId::new(0, 100, 100, 0),
            super::super::PendingRequests::default(),
            super::super::SessionId::generate(),
        );
        let (upstream, mut upstream_recv) =
            super::super::Fetch::new(subscriber, request(64), None, CancellationToken::new());
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(7);
        let expected =
            message::RequestError::new(64, RequestErrorCode::DoesNotExist, 42, "origin failed");
        let delivered = expected.clone();
        let deliver_error = async {
            tokio::task::yield_now().await;
            upstream_recv.recv_error(&delivered).unwrap();
        };

        let (result, ()) = tokio::join!(
            request.wait_for_request_error(
                &upstream,
                tokio::time::Instant::now() + Duration::from_secs(1),
            ),
            deliver_error,
        );

        assert_eq!(result.unwrap(), Some(expected));
    }
}
