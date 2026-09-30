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

use crate::{
    coding::{KeyValuePairs, Location, VarInt},
    data::{DataStreamResetCode, FetchHeader, StreamHeaderType},
    message::{self, Message, RequestErrorCode},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{
    Fetch, JoiningAssociation, JoiningSnapshot, JoiningSnapshotError, SessionError, SessionId,
    Writer,
};

const COPY_CHUNK_SIZE: usize = 64 * 1024;

struct FetchRequestedState {
    closed: Result<(), ServeError>,
}

impl Default for FetchRequestedState {
    fn default() -> Self {
        Self { closed: Ok(()) }
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
    pub request: message::Fetch,
}

pub(crate) struct FetchRequestedRecv {
    state: State<FetchRequestedState>,
}

impl FetchRequested {
    pub(super) fn new(
        webtransport: Option<web_transport::Session>,
        session_id: SessionId,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
        request: message::Fetch,
        joining: Option<JoiningAssociation>,
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
                Some(notify) => notify.await,
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

    pub fn reject(
        self,
        code: RequestErrorCode,
        reason: impl Into<String>,
    ) -> Result<(), ServeError> {
        self.claim_response()?;
        self.send_error(code, reason);
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
            Some(Ok(response)) => self.respond(response),
            None => {
                self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                Err(ServeError::Cancel.into())
            }
            Some(Err(err)) => {
                if let Some(error) = self.wait_for_request_error(&upstream, deadline).await? {
                    let request_id = self.id;
                    self.respond(proxied_error(error, request_id))?;
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
        let webtransport = self.webtransport.as_ref().ok_or(SessionError::Internal)?;
        let mut stream = FetchStream::new(
            Writer::new(self.session_id.clone(), webtransport.open_uni().await?),
            reset.clone(),
        );
        stream
            .writer
            .encode(&FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id: self.id,
            })
            .await?;

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

    fn respond(self, response: impl Into<Message>) -> Result<(), SessionError> {
        self.claim_response()?;
        let _ = self.outgoing.clone().push(response.into());
        Ok(())
    }

    fn claim_response(&self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;
        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(ServeError::Done);
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

    let end_object = snapshot
        .largest
        .object_id
        .checked_add(1)
        .filter(|object_id| *object_id <= VarInt::MAX.into_inner())
        .ok_or(JoiningRangeError::InvalidRange)?;

    Ok(message::StandaloneFetch {
        track_namespace: snapshot.track_namespace.clone(),
        track_name: snapshot.track_name.clone(),
        start_location,
        end_location: Location::new(snapshot.largest.group_id, end_object),
    })
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

pub(super) fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, VarInt::MAX.into_inner())
    } else {
        Location::new(end.group_id, end.object_id - 1)
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
    finished: bool,
}

impl FetchStream {
    fn new(writer: Writer, reset: FetchReset) -> Self {
        Self {
            writer,
            reset,
            finished: false,
        }
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
    fn joining_range_rejects_underflow_future_start_and_object_overflow() {
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
                &joining_snapshot(2, VarInt::MAX.into_inner()),
                FetchType::RelativeJoining,
                0,
            ),
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
        let (upstream, mut upstream_recv) = super::super::Fetch::new(subscriber, request(64));
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
