// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Requester side of FETCH (draft-18 §10.12, §10.13, §11.4.4).
//!
//! [`Subscriber::fetch`](super::Subscriber::fetch) opens a request stream,
//! sends FETCH and FINs its send half (draft-18 has no FETCH follow-ups that
//! this endpoint sends). FETCH_OK or REQUEST_ERROR arrives on the same stream
//! and is read by a future polled by `Session::run`. Objects arrive on a
//! separate unidirectional stream, routed here by Request ID.
//!
//! Draft-18 removed FETCH_CANCEL (§A.2). Dropping an unfinished [`Fetch`]
//! cancels it the draft-18 way (§5.2): STOP_SENDING on the request stream, and
//! STOP_SENDING on the data stream if it is open (dropping the data reader).

use std::sync::{Arc, Mutex};

use crate::{
    coding::Location,
    data::{FetchRecord, FetchRecordDecoder, FetchRecordObject},
    message::{self, FetchOk},
    serve::ServeError,
    watch::State,
};

use super::{FetchStreamReg, FetchValidator, Reader, SessionError};

/// Stream reset / STOP_SENDING code used when the requester cancels.
const CANCELLED: u32 = crate::data::DataStreamResetCode::Cancelled as u32;
/// STOP_SENDING code used when the response is malformed (§2.4.2).
const MALFORMED: u32 = crate::data::DataStreamResetCode::MalformedTrack as u32;

#[derive(Debug, Default)]
pub(super) struct FetchState {
    ok: Option<FetchOk>,
    error: Option<message::RequestError>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum BodyMode {
    #[default]
    Undecided,
    /// Typed records via [`Fetch::next`].
    Records,
    /// Raw bytes after the FETCH_HEADER, for relays that copy the body.
    Raw,
}

/// An outbound FETCH.
///
/// Drive `Session::run` concurrently: the response is read by a future it
/// polls.
#[must_use = "dropping an unfinished FETCH cancels it"]
pub struct Fetch {
    id: u64,
    /// The FETCH as sent.
    pub request: message::Fetch,
    webtransport: web_transport::Session,
    state: State<FetchState>,
    validator: Arc<Mutex<FetchValidator>>,
    registration: FetchStreamReg,
    cancel: Option<tokio::sync::oneshot::Sender<u32>>,
    reader: Option<Reader>,
    decoder: FetchRecordDecoder,
    payload_remaining: u64,
    mode: BodyMode,
    stream_done: bool,
}

/// The half of a [`Fetch`] the request-stream reader drives.
pub(super) struct FetchRecv {
    state: State<FetchState>,
    validator: Arc<Mutex<FetchValidator>>,
}

impl Fetch {
    pub(super) fn new(
        request: message::Fetch,
        range: Option<(Location, Location)>,
        webtransport: web_transport::Session,
        registration: FetchStreamReg,
        cancel: tokio::sync::oneshot::Sender<u32>,
    ) -> (Self, FetchRecv) {
        let order = request.params.group_order().ok().flatten();
        let (send, recv) = State::default().split();
        let validator = Arc::new(Mutex::new(FetchValidator::new(range, order)));
        (
            Self {
                id: request.id,
                request,
                webtransport,
                state: send,
                validator: validator.clone(),
                registration,
                cancel: Some(cancel),
                reader: None,
                decoder: FetchRecordDecoder::new(order),
                payload_remaining: 0,
                mode: BodyMode::Undecided,
                stream_done: false,
            },
            FetchRecv {
                state: recv,
                validator,
            },
        )
    }

    /// The Request ID of this FETCH.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Wait for FETCH_OK.
    ///
    /// A REQUEST_ERROR resolves to `Err(ServeError::Closed(code))`; the exact
    /// message is then available from [`Self::rejection`].
    pub async fn ok(&self) -> Result<FetchOk, ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                if let Some(ok) = &state.ok {
                    return Ok(ok.clone());
                }
                if let Some(error) = &state.error {
                    return Err(ServeError::Closed(error.error_code));
                }
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                // The request stream ended without a response.
                None => return Err(ServeError::Cancel),
            }
        }
    }

    /// The REQUEST_ERROR that rejected this FETCH, if any.
    pub fn rejection(&self) -> Option<message::RequestError> {
        self.state.lock().error.clone()
    }

    /// Decode the next record from the response stream.
    ///
    /// The Object payload must be fully consumed with
    /// [`Self::read_payload_chunk`] before the next call. `Ok(None)` means the
    /// stream ended with FIN.
    pub async fn next(&mut self) -> Result<Option<FetchRecord>, SessionError> {
        if self.payload_remaining != 0 {
            return Err(ServeError::Size.into());
        }
        self.enter_mode(BodyMode::Records)?;
        if self.stream_done {
            return Ok(None);
        }
        self.ensure_reader().await?;
        let reader = self.reader.as_mut().ok_or(SessionError::Internal)?;
        let record = match reader.decode_fetch(&mut self.decoder).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                self.stream_done = true;
                return Ok(None);
            }
            Err(SessionError::Decode(err)) => {
                // §11.4.4: undefined flags, references to a missing prior
                // Object and ID wrap-around are PROTOCOL_VIOLATIONs.
                let err = SessionError::ProtocolViolation(format!("malformed FETCH object: {err}"));
                self.webtransport
                    .close(err.code() as u32, "malformed FETCH object");
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let validated = self
            .validator
            .lock()
            .map_err(|_| SessionError::Internal)?
            .validate_record(&record);
        if let Err(err) = validated {
            self.stop(MALFORMED);
            return Err(err);
        }
        if let FetchRecord::Object(FetchRecordObject { payload_length, .. }) = &record {
            self.payload_remaining = *payload_length;
        }
        Ok(Some(record))
    }

    /// Read at most `max` payload bytes of the current Object.
    ///
    /// Returns `Ok(None)` once the payload is exhausted.
    pub async fn read_payload_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, SessionError> {
        if self.payload_remaining == 0 {
            return Ok(None);
        }
        if max == 0 || self.mode != BodyMode::Records {
            return Err(ServeError::Mode.into());
        }
        let limit = usize::try_from(self.payload_remaining)
            .unwrap_or(usize::MAX)
            .min(max);
        let reader = self.reader.as_mut().ok_or(SessionError::Internal)?;
        match reader.read_chunk(limit).await? {
            Some(chunk) => {
                self.payload_remaining -= chunk.len() as u64;
                Ok(Some(chunk))
            }
            // FIN inside an Object payload.
            None => Err(SessionError::WrongSize),
        }
    }

    /// Read raw response bytes after the FETCH_HEADER, without decoding.
    ///
    /// For relays that forward the body unchanged; it cannot be mixed with
    /// [`Self::next`]. `Ok(None)` means FIN.
    pub(super) async fn read_stream_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, SessionError> {
        self.enter_mode(BodyMode::Raw)?;
        if self.stream_done {
            return Ok(None);
        }
        self.ensure_reader().await?;
        let reader = self.reader.as_mut().ok_or(SessionError::Internal)?;
        let chunk = reader.read_chunk(max).await?;
        if chunk.is_none() {
            self.stream_done = true;
        }
        Ok(chunk)
    }

    fn enter_mode(&mut self, mode: BodyMode) -> Result<(), ServeError> {
        match self.mode {
            BodyMode::Undecided => {
                self.mode = mode;
                Ok(())
            }
            current if current == mode => Ok(()),
            _ => Err(ServeError::Mode),
        }
    }

    /// Wait for the data stream, or for the request to fail first.
    async fn ensure_reader(&mut self) -> Result<(), SessionError> {
        if self.reader.is_some() {
            return Ok(());
        }
        let failed = wait_rejected(&self.state);
        let reader = tokio::select! {
            biased;
            reader = self.registration.await_stream() => {
                reader.map_err(|_| SessionError::Serve(ServeError::Cancel))?
            }
            err = failed => return Err(err.into()),
            _ = self.webtransport.closed() => return Err(ServeError::Cancel.into()),
        };
        self.reader = Some(reader);
        Ok(())
    }

    fn is_complete(&self) -> bool {
        let state = self.state.lock();
        state.error.is_some() || (state.ok.is_some() && self.stream_done)
    }

    fn stop(&mut self, code: u32) {
        if let Some(reader) = self.reader.as_mut() {
            reader.stop(code);
        }
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(code);
        }
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        if !self.is_complete() {
            self.stop(CANCELLED);
        }
    }
}

/// Resolves once a REQUEST_ERROR is recorded, or the request stream ends with
/// no FETCH_OK. Pending while FETCH_OK is known, since the data stream may
/// still be on its way.
async fn wait_rejected(state: &State<FetchState>) -> ServeError {
    loop {
        // The guard must not live across an await: it would make every
        // future that polls this one `!Send`.
        let notify = {
            let state = state.lock();
            if let Some(error) = &state.error {
                return ServeError::Closed(error.error_code);
            }
            if state.ok.is_some() {
                None
            } else {
                Some(state.modified())
            }
        };
        let Some(notify) = notify else {
            return std::future::pending().await;
        };
        match notify {
            Some(notify) => notify.await,
            None => return ServeError::Cancel,
        }
    }
}

impl FetchRecv {
    /// Record FETCH_OK. An invalid FETCH_OK is session-fatal (§10.13).
    pub fn recv_ok(&mut self, ok: FetchOk) -> Result<(), SessionError> {
        self.validator
            .lock()
            .map_err(|_| SessionError::Internal)?
            .validate_ok(ok.end_location, &ok.track_extensions)?;
        let Some(mut state) = self.state.lock_mut() else {
            // The application dropped the FETCH already.
            return Ok(());
        };
        state.ok = Some(ok);
        Ok(())
    }

    /// Record REQUEST_ERROR.
    pub fn recv_error(&mut self, error: message::RequestError) {
        if let Some(mut state) = self.state.lock_mut() {
            state.error = Some(error);
        }
    }

    /// Whether the application still holds the [`Fetch`].
    #[cfg(test)]
    pub fn is_live(&self) -> bool {
        self.state.lock_mut().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        coding::{KeyValuePairs, TrackNamespace},
        data::ExtensionHeaders,
        message::{GroupOrder, RequestErrorCode, StandaloneFetch},
        session::{FetchOkInfo, Publisher, Session, Subscriber, Transport},
    };
    use std::time::Duration;

    const T: Duration = Duration::from_secs(5);

    /// Relays spawn these futures onto multi-threaded runtimes.
    #[allow(dead_code)]
    fn fetch_futures_are_send(mut f: Fetch, r: crate::session::FetchRequested, mut s: Subscriber) {
        fn send<T: Send>(_: T) {}
        send(f.ok());
        send(f.next());
        send(f.read_payload_chunk(1));
        send(s.fetch(
            request(Location::new(0, 0), Location::new(1, 0)),
            KeyValuePairs::default(),
        ));
        send(r.resolve());
        send(r.closed());
        send(r.proxy(f, T));
    }

    /// Two connected sessions, both running. Client fetches, server publishes.
    async fn pair() -> (Subscriber, Publisher) {
        let (client_t, server_t) = super::super::test_support::loopback_session_pair().await;
        let (client, server) = tokio::join!(
            Session::connect(client_t, None, Transport::WebTransport),
            Session::accept(server_t, None, Transport::WebTransport),
        );
        let (client, _client_pub, client_sub) = client.unwrap();
        let (server, server_pub, _server_sub) = server.unwrap();
        let server_pub = server_pub.unwrap();
        server_pub.accept_fetch();
        tokio::spawn(client.run());
        tokio::spawn(server.run());
        (client_sub, server_pub)
    }

    /// A publisher that never opted in to FETCH answers NOT_SUPPORTED, as it
    /// did before FETCH serving existed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_is_not_supported_until_the_publisher_accepts_it() {
        let (client_t, server_t) = super::super::test_support::loopback_session_pair().await;
        let (client, server) = tokio::join!(
            Session::connect(client_t, None, Transport::WebTransport),
            Session::accept(server_t, None, Transport::WebTransport),
        );
        let (client, _client_pub, client_sub) = client.unwrap();
        let (server, _server_pub, _server_sub) = server.unwrap();
        tokio::spawn(client.run());
        tokio::spawn(server.run());
        let mut sub = client_sub;
        let fetch = sub
            .fetch(
                request(Location::new(0, 0), Location::new(1, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(T, fetch.ok()).await.unwrap(),
            Err(ServeError::Closed(RequestErrorCode::NotSupported as u64))
        );
    }

    fn request(start: Location, end: Location) -> StandaloneFetch {
        StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path("test/fetch"),
            track_name: "video".into(),
            start_location: start,
            end_location: end,
        }
    }

    fn obj(group_id: u64, object_id: u64, len: u64) -> FetchRecord {
        FetchRecord::Object(FetchRecordObject {
            group_id,
            subgroup_id: Some(0),
            object_id,
            publisher_priority: 1,
            extension_headers: ExtensionHeaders::default(),
            payload_length: len,
        })
    }

    fn ok_info(end: Location) -> FetchOkInfo {
        FetchOkInfo {
            end_of_track: false,
            end_location: end,
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        }
    }

    async fn read_all(fetch: &mut Fetch) -> Vec<(FetchRecord, Vec<u8>)> {
        let mut out = Vec::new();
        while let Some(record) = tokio::time::timeout(T, fetch.next())
            .await
            .unwrap()
            .unwrap()
        {
            let mut payload = Vec::new();
            while let Some(chunk) = fetch.read_payload_chunk(3).await.unwrap() {
                payload.extend_from_slice(&chunk);
            }
            out.push((record, payload));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standalone_fetch_end_to_end() {
        let (mut sub, mut publ) = pair().await;
        let mut fetch = sub
            .fetch(
                request(Location::new(1, 0), Location::new(3, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();

        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(requested.id(), fetch.id());
        let standalone = requested.resolve().await.unwrap().unwrap();
        assert_eq!(standalone.start_location, Location::new(1, 0));

        let mut writer = requested
            .respond(ok_info(Location::new(2, 3)))
            .await
            .unwrap();
        for (record, payload) in [
            (obj(1, 0, 5), &b"hello"[..]),
            (obj(1, 1, 0), &b""[..]),
            (
                FetchRecord::NotExist {
                    end: Location::new(1, 9),
                },
                &b""[..],
            ),
            (obj(2, 2, 4), &b"abcd"[..]),
        ] {
            writer.write_record(&record).await.unwrap();
            if !payload.is_empty() {
                writer.write_payload(payload).await.unwrap();
            }
        }
        writer.finish().await.unwrap();

        let ok = tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        assert_eq!(ok.end_location, Location::new(2, 3));
        let records = read_all(&mut fetch).await;
        assert_eq!(records.len(), 4);
        assert_eq!(records[0], (obj(1, 0, 5), b"hello".to_vec()));
        assert_eq!(
            records[2].0,
            FetchRecord::NotExist {
                end: Location::new(1, 9)
            }
        );
        assert_eq!(records[3], (obj(2, 2, 4), b"abcd".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn descending_fetch_end_to_end() {
        let (mut sub, mut publ) = pair().await;
        let mut params = KeyValuePairs::default();
        params.set_group_order(GroupOrder::Descending);
        let mut fetch = sub
            .fetch(request(Location::new(0, 0), Location::new(5, 0)), params)
            .await
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let mut writer = requested
            .respond(ok_info(Location::new(4, 1)))
            .await
            .unwrap();
        for g in [4, 2, 0] {
            writer.write_record(&obj(g, 0, 0)).await.unwrap();
        }
        // An ascending step inside a descending response is refused by the
        // writer before it reaches the wire.
        assert!(writer.write_record(&obj(1, 0, 0)).await.is_err());
        writer.finish().await.unwrap();

        let groups: Vec<u64> = read_all(&mut fetch)
            .await
            .into_iter()
            .map(|(r, _)| r.location().group_id)
            .collect();
        assert_eq!(groups, vec![4, 2, 0]);
    }

    /// An End Location of `{group, 0}` covers the whole of `group` (§10.12.1),
    /// so any Object in it is in range and the next group is not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn whole_group_end_location_covers_the_group() {
        let (mut sub, mut publ) = pair().await;
        let mut fetch = sub
            .fetch(
                request(Location::new(1, 0), Location::new(2, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let mut writer = requested
            .respond(ok_info(Location::new(2, 0)))
            .await
            .unwrap();
        writer.write_record(&obj(1, 0, 0)).await.unwrap();
        writer.write_record(&obj(2, 0, 0)).await.unwrap();
        writer.write_record(&obj(2, 900, 0)).await.unwrap();
        assert!(
            writer.write_record(&obj(3, 0, 0)).await.is_err(),
            "the group after a whole-group end is out of range"
        );
        writer.finish().await.unwrap();

        tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        let locations: Vec<Location> = read_all(&mut fetch)
            .await
            .into_iter()
            .map(|(r, _)| r.location())
            .collect();
        assert_eq!(
            locations,
            vec![
                Location::new(1, 0),
                Location::new(2, 0),
                Location::new(2, 900)
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_fetch_surfaces_request_error() {
        let (mut sub, mut publ) = pair().await;
        let mut fetch = sub
            .fetch(
                request(Location::new(0, 0), Location::new(1, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        requested
            .reject(RequestErrorCode::DoesNotExist, "no such track")
            .unwrap();
        assert_eq!(
            tokio::time::timeout(T, fetch.ok()).await.unwrap(),
            Err(ServeError::Closed(RequestErrorCode::DoesNotExist as u64))
        );
        assert_eq!(fetch.rejection().unwrap().reason.0, "no such track");
        assert!(fetch.next().await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_range_is_rejected_by_the_session() {
        let (mut sub, _publ) = pair().await;
        // Local check refuses to send an empty range.
        assert!(sub
            .fetch(
                request(Location::new(2, 0), Location::new(1, 5)),
                KeyValuePairs::default()
            )
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_fetch_cancels_the_unanswered_request() {
        let (mut sub, mut publ) = pair().await;
        let fetch = sub
            .fetch(
                request(Location::new(0, 0), Location::new(1, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        drop(fetch);
        assert_eq!(
            tokio::time::timeout(T, requested.closed()).await.unwrap(),
            ServeError::Cancel
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_response_is_header_then_fin() {
        let (mut sub, mut publ) = pair().await;
        let mut fetch = sub
            .fetch(
                request(Location::new(0, 0), Location::new(1, 0)),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let writer = requested
            .respond(ok_info(Location::new(0, 1)))
            .await
            .unwrap();
        writer.finish().await.unwrap();
        tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        assert!(read_all(&mut fetch).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_copies_upstream_response() {
        // upstream: origin publisher <- relay subscriber
        let (mut relay_up, mut origin) = pair().await;
        // downstream: relay publisher <- end subscriber
        let (mut end, mut relay_down) = pair().await;

        let range = request(Location::new(0, 0), Location::new(2, 0));
        let mut fetch = end
            .fetch(range.clone(), KeyValuePairs::default())
            .await
            .unwrap();
        let downstream = tokio::time::timeout(T, relay_down.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let standalone = downstream.resolve().await.unwrap().unwrap();
        let upstream = relay_up
            .fetch(standalone, KeyValuePairs::default())
            .await
            .unwrap();

        let origin_task = tokio::spawn(async move {
            let requested = origin.fetch_requested().await.unwrap();
            let mut writer = requested
                .respond(ok_info(Location::new(1, 2)))
                .await
                .unwrap();
            writer.write_record(&obj(0, 0, 2)).await.unwrap();
            writer.write_payload(b"hi").await.unwrap();
            writer.write_record(&obj(1, 1, 0)).await.unwrap();
            writer.finish().await.unwrap();
            origin
        });
        downstream.proxy(upstream, T).await.unwrap();
        let _origin = origin_task.await.unwrap();

        let ok = tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        assert_eq!(ok.end_location, Location::new(1, 2));
        assert_eq!(fetch.id(), ok.id);
        let records = read_all(&mut fetch).await;
        assert_eq!(
            records,
            vec![(obj(0, 0, 2), b"hi".to_vec()), (obj(1, 1, 0), vec![])]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_forwards_upstream_rejection() {
        let (mut relay_up, mut origin) = pair().await;
        let (mut end, mut relay_down) = pair().await;
        let range = request(Location::new(0, 0), Location::new(2, 0));
        let fetch = end
            .fetch(range.clone(), KeyValuePairs::default())
            .await
            .unwrap();
        let downstream = tokio::time::timeout(T, relay_down.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let upstream = relay_up
            .fetch(range, KeyValuePairs::default())
            .await
            .unwrap();
        let origin_task = tokio::spawn(async move {
            let requested = origin.fetch_requested().await.unwrap();
            requested
                .reject(RequestErrorCode::InvalidRange, "nothing yet")
                .unwrap();
            origin
        });
        assert!(downstream.proxy(upstream, T).await.is_err());
        let _origin = origin_task.await.unwrap();
        assert_eq!(
            tokio::time::timeout(T, fetch.ok()).await.unwrap(),
            Err(ServeError::Closed(RequestErrorCode::InvalidRange as u64))
        );
        assert_eq!(fetch.rejection().unwrap().reason.0, "nothing yet");
    }

    #[test]
    fn rejection_after_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            validator: Arc::new(Mutex::new(FetchValidator::new(None, None))),
        };
        drop(fetch);
        recv.recv_error(message::RequestError::new(
            0,
            message::RequestErrorCode::InternalError,
            0,
            "failed",
        ));
        assert!(!recv.is_live());
    }

    #[test]
    fn fetch_ok_outside_range_is_session_fatal() {
        let (_fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            validator: Arc::new(Mutex::new(FetchValidator::new(
                Some((Location::new(0, 0), Location::new(1, 0))),
                None,
            ))),
        };
        let err = recv
            .recv_ok(FetchOk {
                id: 0,
                end_of_track: false,
                end_location: Location::new(2, 0),
                params: Default::default(),
                track_extensions: Default::default(),
            })
            .unwrap_err();
        assert!(err.is_session_fatal());
    }
}
