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

use super::transport as web_transport;
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
    /// The request stream was reset after FETCH_OK: the response failed.
    aborted: bool,
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
        // §10.12.2.1: an Absolute Joining FETCH starts at `{joining_start, 0}`;
        // its end comes from the publisher, so only the start is checked.
        let min_start = match (&request.fetch_type, &request.joining_fetch) {
            (message::FetchType::AbsoluteJoining, Some(joining)) => {
                Some(Location::new(joining.joining_start, 0))
            }
            _ => None,
        };
        let validator = Arc::new(Mutex::new(
            FetchValidator::new(range, order).with_min_start(min_start),
        ));
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
        if self.stream_done {
            return Ok(None);
        }
        self.ensure_reader().await?;
        use tracing::Instrument;
        let span = self
            .reader
            .as_ref()
            .ok_or(SessionError::Internal)?
            .session_id()
            .span();
        self.next_record().instrument(span).await
    }

    async fn next_record(&mut self) -> Result<Option<FetchRecord>, SessionError> {
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
        if max == 0 {
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

    /// Wait for FETCH_OK without borrowing the handle, so the body can be
    /// read concurrently. `Err(None)` means the request stream ended with
    /// no response.
    pub(super) fn response(
        &self,
    ) -> impl std::future::Future<Output = Result<FetchOk, Option<message::RequestError>>> + Send + 'static
    {
        let state = self.state.clone();
        async move {
            loop {
                let notify = {
                    let state = state.lock();
                    if let Some(ok) = &state.ok {
                        return Ok(ok.clone());
                    }
                    if let Some(error) = &state.error {
                        return Err(Some(error.clone()));
                    }
                    state.modified()
                };
                match notify {
                    Some(notify) => notify.await,
                    None => return Err(None),
                }
            }
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
        state.error.is_some() || state.aborted || (state.ok.is_some() && self.stream_done)
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
            if state.aborted {
                return ServeError::Cancel;
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

    /// The publisher reset the request stream after FETCH_OK: the response
    /// will not complete, and its data stream may never be identifiable.
    pub fn recv_abort(&mut self) {
        if let Some(mut state) = self.state.lock_mut() {
            state.aborted = true;
        }
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

    /// FETCHes beyond the per-session cap get EXCESSIVE_LOAD, and answering
    /// one frees its slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_fetches_beyond_the_cap_are_excessive_load() {
        let (mut sub, mut publ) = pair().await;
        let cap = crate::session::publisher::MAX_INBOUND_FETCHES_PER_SESSION;
        let range = || request(Location::new(0, 0), Location::new(1, 0));
        let mut held = Vec::new();
        let mut pending = Vec::new();
        for _ in 0..cap {
            pending.push(sub.fetch(range(), KeyValuePairs::default()).await.unwrap());
            held.push(
                tokio::time::timeout(T, publ.fetch_requested())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let over = sub.fetch(range(), KeyValuePairs::default()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(T, over.ok()).await.unwrap(),
            Err(ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64))
        );

        // Answering one frees a slot.
        held.pop()
            .unwrap()
            .reject(RequestErrorCode::DoesNotExist, "gone")
            .unwrap();
        let rejected = pending.pop().unwrap();
        tokio::time::timeout(T, rejected.ok())
            .await
            .unwrap()
            .unwrap_err();
        let _again = sub.fetch(range(), KeyValuePairs::default()).await.unwrap();
        let _accepted = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .expect("a freed slot accepts the next FETCH")
            .unwrap();
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

    // ── Joining FETCH ────────────────────────────────────────────────────

    fn track(name: &str) -> (crate::serve::TrackWriter, crate::serve::TrackReader) {
        crate::serve::Track::new(TrackNamespace::from_utf8_path("test/fetch"), name).produce()
    }

    /// Joining FETCH sent while its SUBSCRIBE is still pending is buffered
    /// until SUBSCRIBE_OK, then resolved against the Joining Location.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_waits_for_the_subscription_and_ends_at_its_start() {
        let (mut sub, mut publ) = pair().await;
        let (writer, _reader) = track("video");
        let subscribe = sub.subscribe_start(writer).await.unwrap();
        let subscribe_id = subscribe.info.id;
        let mut fetch = sub
            .joining_fetch(
                crate::message::FetchType::RelativeJoining,
                subscribe_id,
                2,
                KeyValuePairs::default(),
            )
            .await
            .unwrap();

        let subscribed = tokio::time::timeout(T, publ.subscribed())
            .await
            .unwrap()
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();

        // Pending until the subscription is accepted.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), requested.resolve())
                .await
                .is_err(),
            "a Joining FETCH must wait for its subscription"
        );

        let (_track_writer, track_reader) = track("video");
        let serve = tokio::spawn(async move {
            subscribed
                .serve_with_largest_location(track_reader, Some(Location::new(5, 3)))
                .await
        });
        let range = tokio::time::timeout(T, requested.resolve())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Resolution is memoized; responding uses the same range.
        assert_eq!(requested.resolve().await.unwrap().unwrap(), range);
        assert_eq!(
            range.track_namespace,
            TrackNamespace::from_utf8_path("test/fetch")
        );
        assert_eq!(range.start_location, Location::new(3, 0));
        assert_eq!(range.end_location, Location::new(5, 4));
        subscribe.ok().await.unwrap();

        let mut writer = requested
            .respond(ok_info(Location::new(5, 4)))
            .await
            .unwrap();
        writer.write_record(&obj(3, 0, 0)).await.unwrap();
        writer.write_record(&obj(5, 3, 1)).await.unwrap();
        writer.write_payload(b"x").await.unwrap();
        assert!(
            writer.write_record(&obj(5, 4, 0)).await.is_err(),
            "the Joining Location is the last Object in the response"
        );
        writer.finish().await.unwrap();

        let ok = tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        assert_eq!(ok.end_location, Location::new(5, 4));
        let records = read_all(&mut fetch).await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[1], (obj(5, 3, 1), b"x".to_vec()));
        drop(serve);
    }

    async fn joining_error(
        sub: &mut Subscriber,
        fetch_type: crate::message::FetchType,
        subscribe_id: u64,
        joining_start: u64,
    ) -> Option<u64> {
        let fetch = sub
            .joining_fetch(
                fetch_type,
                subscribe_id,
                joining_start,
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        match tokio::time::timeout(T, fetch.ok()).await.unwrap() {
            Err(ServeError::Closed(code)) => Some(code),
            _ => None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_for_an_unknown_subscription_is_rejected() {
        let (mut sub, _publ) = pair().await;
        // Not an ID the client can use (wrong parity).
        assert_eq!(
            joining_error(&mut sub, crate::message::FetchType::AbsoluteJoining, 99, 0).await,
            Some(RequestErrorCode::InvalidJoiningRequestId as u64)
        );
        // Already used, by the FETCH above, so it can never be a SUBSCRIBE.
        assert_eq!(
            joining_error(&mut sub, crate::message::FetchType::AbsoluteJoining, 0, 0).await,
            Some(RequestErrorCode::InvalidJoiningRequestId as u64)
        );
    }

    /// Joining a PUBLISH is not supported yet: its Joining Location is not
    /// recorded, so it gets NOT_SUPPORTED rather than INVALID_JOINING_REQUEST_ID.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_for_a_publish_is_not_supported() {
        let (client_t, server_t) = super::super::test_support::loopback_session_pair().await;
        let (client, server) = tokio::join!(
            Session::connect(client_t, None, Transport::WebTransport),
            Session::accept(server_t, None, Transport::WebTransport),
        );
        let (client, _client_pub, client_sub) = client.unwrap();
        let (server, server_pub, _server_sub) = server.unwrap();
        let mut client_sub = client_sub;
        let mut server_pub = server_pub.unwrap();
        server_pub.accept_fetch();
        tokio::spawn(client.run());
        tokio::spawn(server.run());

        let (_writer, reader) = track("video");
        let publish = tokio::spawn(async move {
            let published = server_pub.publish(reader, KeyValuePairs::default()).await;
            (server_pub, published)
        });
        let mut received = tokio::time::timeout(T, client_sub.publish_received())
            .await
            .unwrap()
            .unwrap();
        // The publisher uses the Request ID as the Track Alias.
        let publish_id = received.track_alias();
        received.accept(true).unwrap();
        let (_server_pub, _published) = tokio::time::timeout(T, publish).await.unwrap().unwrap();

        assert_eq!(
            joining_error(
                &mut client_sub,
                crate::message::FetchType::RelativeJoining,
                publish_id,
                0
            )
            .await,
            Some(RequestErrorCode::NotSupported as u64)
        );
    }

    /// Request streams are unordered, so a Joining FETCH can be read before
    /// the SUBSCRIBE it names. It waits for the SUBSCRIBE instead of being
    /// rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_read_before_its_subscribe_waits_for_it() {
        let (mut sub, mut publ) = pair().await;
        // A fresh client allocates request IDs 0, 2, ...: the FETCH takes 0
        // and names 2, which the SUBSCRIBE sent afterwards will use.
        let mut fetch = sub
            .joining_fetch(
                crate::message::FetchType::AbsoluteJoining,
                2,
                1,
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        assert_eq!(fetch.id(), 0);
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let resolve = tokio::spawn(async move {
            let range = requested.resolve().await;
            (range, requested)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (writer, _reader) = track("video");
        let subscribe = sub.subscribe_start(writer).await.unwrap();
        assert_eq!(subscribe.info.id, 2);
        let subscribed = tokio::time::timeout(T, publ.subscribed())
            .await
            .unwrap()
            .unwrap();
        let (_track_writer, track_reader) = track("video");
        let _serve = tokio::spawn(async move {
            subscribed
                .serve_with_largest_location(track_reader, Some(Location::new(3, 0)))
                .await
        });

        let (range, requested) = tokio::time::timeout(T, resolve).await.unwrap().unwrap();
        let range = range.unwrap().expect("resolved once the SUBSCRIBE arrived");
        assert_eq!(range.start_location, Location::new(1, 0));
        assert_eq!(range.end_location, Location::new(3, 1));
        let writer = requested
            .respond(ok_info(Location::new(3, 1)))
            .await
            .unwrap();
        writer.finish().await.unwrap();
        tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        assert!(read_all(&mut fetch).await.is_empty());
    }

    /// §10.12.2: Forward State 0 and an empty track are both INVALID_RANGE,
    /// as is an absolute start after the Joining Location.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_range_errors() {
        let (mut sub, mut publ) = pair().await;
        // The application resolves each FETCH; resolution sends the error.
        let mut resolver = publ.clone();
        tokio::spawn(async move {
            while let Some(requested) = resolver.fetch_requested().await {
                tokio::spawn(async move {
                    if let Ok(Some(_)) = requested.resolve().await {
                        let _ = requested.reject(RequestErrorCode::InternalError, "unexpected");
                    }
                });
            }
        });

        let serve_track = |publ: &mut Publisher, largest: Option<Location>| {
            let mut publ = publ.clone();
            async move {
                let subscribed = publ.subscribed().await.unwrap();
                let name = subscribed.info.track_name.to_string_lossy().into_owned();
                let (_w, reader) = track(&name);
                tokio::spawn(async move {
                    let _keep = _w;
                    subscribed
                        .serve_with_largest_location(reader, largest)
                        .await
                });
            }
        };

        // Forward State 0.
        let (writer, _r0) = track("paused");
        let mut params = KeyValuePairs::default();
        params.set_forward(false);
        let (paused, ()) = tokio::join!(
            sub.subscribe_open_with_params(writer, params),
            serve_track(&mut publ, Some(Location::new(4, 0))),
        );
        let paused = paused.unwrap();
        assert_eq!(
            joining_error(
                &mut sub,
                crate::message::FetchType::RelativeJoining,
                paused.info.id,
                0
            )
            .await,
            Some(RequestErrorCode::InvalidRange as u64)
        );

        // No Objects published yet.
        let (writer, _r1) = track("empty");
        let (empty, ()) = tokio::join!(sub.subscribe_open(writer), serve_track(&mut publ, None));
        let empty = empty.unwrap();
        assert_eq!(
            joining_error(
                &mut sub,
                crate::message::FetchType::RelativeJoining,
                empty.info.id,
                0
            )
            .await,
            Some(RequestErrorCode::InvalidRange as u64)
        );

        // Absolute start after the Joining Location; relative start before 0.
        let (writer, _r2) = track("live");
        let (live, ()) = tokio::join!(
            sub.subscribe_open(writer),
            serve_track(&mut publ, Some(Location::new(4, 0)))
        );
        let live = live.unwrap();
        assert_eq!(
            joining_error(
                &mut sub,
                crate::message::FetchType::AbsoluteJoining,
                live.info.id,
                5
            )
            .await,
            Some(RequestErrorCode::InvalidRange as u64)
        );
        assert_eq!(
            joining_error(
                &mut sub,
                crate::message::FetchType::RelativeJoining,
                live.info.id,
                5
            )
            .await,
            Some(RequestErrorCode::InvalidRange as u64)
        );
    }

    /// A pending Joining FETCH whose subscription is rejected is answered
    /// with INVALID_JOINING_REQUEST_ID rather than left waiting.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joining_fetch_fails_when_the_pending_subscription_is_rejected() {
        let (mut sub, mut publ) = pair().await;
        let (writer, _reader) = track("video");
        let subscribe = sub.subscribe_start(writer).await.unwrap();
        let fetch = sub
            .joining_fetch(
                crate::message::FetchType::RelativeJoining,
                subscribe.info.id,
                0,
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let subscribed = tokio::time::timeout(T, publ.subscribed())
            .await
            .unwrap()
            .unwrap();
        let requested = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        let resolve = tokio::spawn(async move {
            let result = requested.resolve().await;
            (result, requested)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        subscribed.close(ServeError::not_found_ctx("gone")).unwrap();
        let (result, _requested) = tokio::time::timeout(T, resolve).await.unwrap().unwrap();
        assert_eq!(result.unwrap(), None);
        assert_eq!(
            tokio::time::timeout(T, fetch.ok()).await.unwrap(),
            Err(ServeError::Closed(
                RequestErrorCode::InvalidJoiningRequestId as u64
            ))
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

    /// §5.2: the requester can cancel after FETCH_OK, and the publisher sees it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_fetch_after_fetch_ok_cancels_the_response() {
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
        let mut writer = requested
            .respond(ok_info(Location::new(1, 0)))
            .await
            .unwrap();
        tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
        drop(fetch);

        // Keep writing an endless payload until the cancel lands; it may
        // already have landed before the first record.
        let cancelled = tokio::time::timeout(T, async {
            if writer.write_record(&obj(0, 0, u64::MAX)).await.is_err() {
                return;
            }
            while writer.write_payload(&[0; 1024]).await.is_ok() {}
        })
        .await;
        assert!(cancelled.is_ok(), "the writer must observe the cancel");
    }

    /// A FETCH counts against the cap until its response ends, not just until
    /// FETCH_OK.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fetch_holds_its_cap_slot_until_the_response_ends() {
        let (mut sub, mut publ) = pair().await;
        let cap = crate::session::publisher::MAX_INBOUND_FETCHES_PER_SESSION;
        let range = || request(Location::new(0, 0), Location::new(1, 0));
        let mut fetches = Vec::new();
        let mut writers = Vec::new();
        for _ in 0..cap {
            let fetch = sub.fetch(range(), KeyValuePairs::default()).await.unwrap();
            let requested = tokio::time::timeout(T, publ.fetch_requested())
                .await
                .unwrap()
                .unwrap();
            writers.push(
                requested
                    .respond(ok_info(Location::new(0, 1)))
                    .await
                    .unwrap(),
            );
            tokio::time::timeout(T, fetch.ok()).await.unwrap().unwrap();
            fetches.push(fetch);
        }
        let over = sub.fetch(range(), KeyValuePairs::default()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(T, over.ok()).await.unwrap(),
            Err(ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)),
            "FETCH_OK alone does not free a slot"
        );

        writers.pop().unwrap().finish().await.unwrap();
        let mut done = fetches.pop().unwrap();
        assert!(read_all(&mut done).await.is_empty());
        let _again = sub.fetch(range(), KeyValuePairs::default()).await.unwrap();
        let _accepted = tokio::time::timeout(T, publ.fetch_requested())
            .await
            .expect("a finished response frees its slot")
            .unwrap();
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

    /// A body larger than a QUIC stream window flows through the proxy to a
    /// requester that waits for FETCH_OK before reading.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_streams_a_large_body_to_a_requester_awaiting_fetch_ok() {
        const LEN: usize = 4 * 1024 * 1024;
        let (mut relay_up, mut origin) = pair().await;
        let (mut end, mut relay_down) = pair().await;
        let range = request(Location::new(0, 0), Location::new(1, 0));
        let mut fetch = end
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
            let mut writer = requested
                .respond(ok_info(Location::new(0, 1)))
                .await
                .unwrap();
            writer.write_record(&obj(0, 0, LEN as u64)).await.unwrap();
            for chunk in vec![7u8; LEN].chunks(64 * 1024) {
                writer.write_payload(chunk).await.unwrap();
            }
            writer.finish().await.unwrap();
            origin
        });
        let relay = tokio::spawn(downstream.proxy(upstream, Duration::from_secs(30)));

        tokio::time::timeout(T, fetch.ok())
            .await
            .expect("FETCH_OK must not wait for the body")
            .unwrap();
        let record = tokio::time::timeout(T, fetch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record, Some(obj(0, 0, LEN as u64)));
        let mut received = 0;
        while let Some(chunk) = tokio::time::timeout(T, fetch.read_payload_chunk(1 << 20))
            .await
            .unwrap()
            .unwrap()
        {
            assert!(chunk.iter().all(|b| *b == 7));
            received += chunk.len();
        }
        assert_eq!(received, LEN);
        assert_eq!(
            tokio::time::timeout(T, fetch.next())
                .await
                .unwrap()
                .unwrap(),
            None
        );
        tokio::time::timeout(T, relay)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _origin = origin_task.await.unwrap();
    }

    /// A malformed upstream body resets the downstream data stream; the
    /// requester's session with the relay survives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_resets_downstream_on_a_malformed_upstream_body() {
        let (mut relay_up, mut origin) = pair().await;
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
        let upstream = relay_up
            .fetch(range.clone(), KeyValuePairs::default())
            .await
            .unwrap();
        let origin_task = tokio::spawn(async move {
            let requested = origin.fetch_requested().await.unwrap();
            let mut writer = requested
                .respond(ok_info(Location::new(1, 0)))
                .await
                .unwrap();
            // §11.4.4.1: Serialization Flags 0x00 on the first Object refers
            // to a prior Object that does not exist.
            writer.write_raw(&[0x00, 0x00, 0x00, 0x00]).await.unwrap();
            // Hold the stream open; the relay must react to the bad record.
            tokio::time::sleep(T).await;
            drop(writer);
            origin
        });
        let relay = tokio::spawn(downstream.proxy(upstream, Duration::from_secs(30)));

        let relayed = tokio::time::timeout(T, relay).await.unwrap().unwrap();
        assert!(relayed.is_err());
        // The relay may hit the bad record before or after the upstream
        // FETCH_OK: REQUEST_ERROR, or FETCH_OK and a failed body. A request
        // stream reset can also overtake an undelivered FETCH_OK (Cancel).
        match tokio::time::timeout(T, fetch.ok()).await.unwrap() {
            Ok(_) => assert!(
                tokio::time::timeout(T, fetch.next())
                    .await
                    .unwrap()
                    .is_err(),
                "the requester sees a failed body, not a malformed record"
            ),
            Err(err) => assert!(
                err == ServeError::Cancel
                    || err == ServeError::Closed(RequestErrorCode::InternalError as u64),
                "unexpected {err:?}"
            ),
        }

        // §11.4.4.1: the relay, as subscriber on the upstream hop, closes
        // the session that sent the malformed object.
        tokio::time::timeout(T, async {
            while relay_up
                .fetch(
                    request(Location::new(0, 0), Location::new(1, 0)),
                    KeyValuePairs::default(),
                )
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the upstream session is closed");

        // The requester's session with the relay is still usable.
        let _again = end.fetch(range, KeyValuePairs::default()).await.unwrap();
        let _accepted = tokio::time::timeout(T, relay_down.fetch_requested())
            .await
            .expect("session with the relay survives")
            .unwrap();
        origin_task.abort();
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

    /// §10.12.2.1/§10.13: an Absolute Joining FETCH's requester rejects a
    /// FETCH_OK that ends before `{joining_start, 0}`; a relative join has no
    /// known start.
    #[tokio::test]
    async fn absolute_joining_fetch_checks_its_start() {
        async fn recv_for(fetch_type: message::FetchType) -> (Fetch, FetchRecv) {
            let session = super::super::test_support::loopback_session().await;
            let (cancel, _) = tokio::sync::oneshot::channel();
            let request = message::Fetch {
                id: 0,
                fetch_type,
                standalone_fetch: None,
                joining_fetch: Some(message::JoiningFetch {
                    joining_request_id: 2,
                    joining_start: 4,
                }),
                params: KeyValuePairs::default(),
            };
            Fetch::new(request, None, session, FetchStreamReg::detached(), cancel)
        }
        let early = || FetchOk {
            id: 0,
            end_of_track: false,
            end_location: Location::new(3, 5),
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        };
        let (_f, mut recv) = recv_for(message::FetchType::AbsoluteJoining).await;
        assert!(matches!(
            recv.recv_ok(early()),
            Err(SessionError::ProtocolViolation(_))
        ));
        let (_f, mut recv) = recv_for(message::FetchType::RelativeJoining).await;
        recv.recv_ok(early()).unwrap();
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
