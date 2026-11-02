// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
use super::quinn;
use super::{Error, SendStream, Session};
use crate::session::{Session as MoqSession, SessionError, Transport};
use bytes::Bytes;
use futures::lock::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use url::Url;
use web_transport_trait::RecvStream as _;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum TestError {
    #[error("session closed: {0}")]
    Session(u32),
    #[error("stream ended: {0}")]
    Stream(u32),
    #[error("transport failed")]
    Failed,
}

impl web_transport_trait::Error for TestError {
    fn session_error(&self) -> Option<(u32, String)> {
        match self {
            Self::Session(code) => Some((*code, "test close".into())),
            _ => None,
        }
    }
    fn stream_error(&self) -> Option<u32> {
        match self {
            Self::Stream(code) => Some(*code),
            _ => None,
        }
    }
}

async fn wait_code(rx: &mut watch::Receiver<Option<u32>>) -> u32 {
    loop {
        if let Some(code) = *rx.borrow_and_update() {
            return code;
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

struct MemorySend {
    data: Option<mpsc::Sender<Bytes>>,
    reset: watch::Sender<Option<u32>>,
    stopped: watch::Receiver<Option<u32>>,
}

struct MemoryRecv {
    data: mpsc::Receiver<Bytes>,
    pending: Bytes,
    reset: watch::Receiver<Option<u32>>,
    stopped: watch::Sender<Option<u32>>,
}

struct InvalidSend(usize);

impl web_transport_trait::SendStream for InvalidSend {
    type Error = TestError;

    async fn write(&mut self, _: &[u8]) -> Result<usize, TestError> {
        Ok(self.0)
    }
    fn set_priority(&mut self, _: u8) {}
    fn finish(&mut self) -> Result<(), TestError> {
        Ok(())
    }
    fn reset(&mut self, _: u32) {}
    async fn closed(&mut self) -> Result<(), TestError> {
        Ok(())
    }
}

fn pipe(capacity: usize) -> (MemorySend, MemoryRecv) {
    let (tx, rx) = mpsc::channel(capacity);
    let (reset_tx, reset_rx) = watch::channel(None);
    let (stop_tx, stop_rx) = watch::channel(None);
    (
        MemorySend {
            data: Some(tx),
            reset: reset_tx,
            stopped: stop_rx,
        },
        MemoryRecv {
            data: rx,
            pending: Bytes::new(),
            reset: reset_rx,
            stopped: stop_tx,
        },
    )
}

impl web_transport_trait::SendStream for MemorySend {
    type Error = TestError;

    async fn write(&mut self, data: &[u8]) -> Result<usize, TestError> {
        let tx = self.data.as_ref().ok_or(TestError::Stream(0))?;
        let permit = tokio::select! {
            code = wait_code(&mut self.stopped) => return Err(TestError::Stream(code)),
            permit = tx.reserve() => permit.map_err(|_| TestError::Stream(0))?,
        };
        // Force partial writes so protocol tests cannot assume all-or-nothing IO.
        let size = data.len().min(3);
        permit.send(Bytes::copy_from_slice(&data[..size]));
        Ok(size)
    }

    fn set_priority(&mut self, _: u8) {}
    fn finish(&mut self) -> Result<(), TestError> {
        self.data.take();
        Ok(())
    }
    fn reset(&mut self, code: u32) {
        self.reset.send_replace(Some(code));
        self.data.take();
    }
    async fn closed(&mut self) -> Result<(), TestError> {
        if self.data.is_none() {
            return Ok(());
        }
        Err(TestError::Stream(wait_code(&mut self.stopped).await))
    }
}

impl web_transport_trait::RecvStream for MemoryRecv {
    type Error = TestError;

    async fn read(&mut self, dst: &mut [u8]) -> Result<Option<usize>, TestError> {
        match self.read_chunk(dst.len()).await? {
            Some(chunk) => {
                dst[..chunk.len()].copy_from_slice(&chunk);
                Ok(Some(chunk.len()))
            }
            None => Ok(None),
        }
    }

    async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, TestError> {
        if max == 0 {
            return Ok(Some(Bytes::new()));
        }
        if let Some(code) = *self.reset.borrow() {
            return Err(TestError::Stream(code));
        }
        if self.pending.is_empty() {
            self.pending = tokio::select! {
                code = wait_code(&mut self.reset) => return Err(TestError::Stream(code)),
                chunk = self.data.recv() => match chunk { Some(chunk) => chunk, None => return Ok(None) },
            };
        }
        Ok(Some(self.pending.split_to(max.min(self.pending.len()))))
    }

    fn stop(&mut self, code: u32) {
        self.stopped.send_replace(Some(code));
    }
    async fn closed(&mut self) -> Result<(), TestError> {
        while self.read_chunk(8192).await?.is_some() {}
        Ok(())
    }
}

impl Drop for MemoryRecv {
    fn drop(&mut self) {
        self.stopped.send_if_modified(|code| {
            if code.is_none() {
                *code = Some(0);
                true
            } else {
                false
            }
        });
    }
}

#[derive(Clone)]
struct MemorySession {
    uni: Arc<Mutex<mpsc::UnboundedReceiver<MemoryRecv>>>,
    bidi: Arc<Mutex<mpsc::UnboundedReceiver<(MemorySend, MemoryRecv)>>>,
    datagrams: Arc<Mutex<mpsc::UnboundedReceiver<Bytes>>>,
    peer_uni: mpsc::UnboundedSender<MemoryRecv>,
    peer_bidi: mpsc::UnboundedSender<(MemorySend, MemoryRecv)>,
    peer_datagrams: mpsc::UnboundedSender<Bytes>,
    close: watch::Sender<Option<u32>>,
}

fn pair() -> (MemorySession, MemorySession) {
    let (a_uni_tx, a_uni_rx) = mpsc::unbounded_channel();
    let (b_uni_tx, b_uni_rx) = mpsc::unbounded_channel();
    let (a_bi_tx, a_bi_rx) = mpsc::unbounded_channel();
    let (b_bi_tx, b_bi_rx) = mpsc::unbounded_channel();
    let (a_dg_tx, a_dg_rx) = mpsc::unbounded_channel();
    let (b_dg_tx, b_dg_rx) = mpsc::unbounded_channel();
    let (close, _) = watch::channel(None);
    (
        MemorySession {
            uni: Arc::new(Mutex::new(a_uni_rx)),
            bidi: Arc::new(Mutex::new(a_bi_rx)),
            datagrams: Arc::new(Mutex::new(a_dg_rx)),
            peer_uni: b_uni_tx,
            peer_bidi: b_bi_tx,
            peer_datagrams: b_dg_tx,
            close: close.clone(),
        },
        MemorySession {
            uni: Arc::new(Mutex::new(b_uni_rx)),
            bidi: Arc::new(Mutex::new(b_bi_rx)),
            datagrams: Arc::new(Mutex::new(b_dg_rx)),
            peer_uni: a_uni_tx,
            peer_bidi: a_bi_tx,
            peer_datagrams: a_dg_tx,
            close,
        },
    )
}

impl web_transport_trait::Session for MemorySession {
    type SendStream = MemorySend;
    type RecvStream = MemoryRecv;
    type Error = TestError;

    async fn accept_uni(&self) -> Result<MemoryRecv, TestError> {
        tokio::select! {
            err = self.closed() => Err(err),
            stream = async { self.uni.lock().await.recv().await } => stream.ok_or(TestError::Failed),
        }
    }
    async fn accept_bi(&self) -> Result<(MemorySend, MemoryRecv), TestError> {
        tokio::select! {
            err = self.closed() => Err(err),
            stream = async { self.bidi.lock().await.recv().await } => stream.ok_or(TestError::Failed),
        }
    }
    async fn open_uni(&self) -> Result<MemorySend, TestError> {
        let (send, recv) = pipe(32);
        self.peer_uni.send(recv).map_err(|_| TestError::Failed)?;
        Ok(send)
    }
    async fn open_bi(&self) -> Result<(MemorySend, MemoryRecv), TestError> {
        let (send, peer_recv) = pipe(32);
        let (peer_send, recv) = pipe(32);
        self.peer_bidi
            .send((peer_send, peer_recv))
            .map_err(|_| TestError::Failed)?;
        Ok((send, recv))
    }
    fn send_datagram(&self, data: Bytes) -> Result<(), TestError> {
        self.peer_datagrams
            .send(data)
            .map_err(|_| TestError::Failed)
    }
    async fn recv_datagram(&self) -> Result<Bytes, TestError> {
        tokio::select! {
            err = self.closed() => Err(err),
            data = async { self.datagrams.lock().await.recv().await } => data.ok_or(TestError::Failed),
        }
    }
    fn max_datagram_size(&self) -> usize {
        1200
    }
    fn protocol(&self) -> Option<&str> {
        Some("moqt-18")
    }
    fn close(&self, code: u32, _: &str) {
        self.close.send_replace(Some(code));
    }
    async fn closed(&self) -> TestError {
        TestError::Session(wait_code(&mut self.close.subscribe()).await)
    }
}

#[tokio::test]
async fn independent_backend_drives_moq_setup_and_namespace_cancellation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, server) = pair();
        let url = Url::parse("https://relay.example/tenant").unwrap();
        let client = Session::new(client, url.clone());
        let server = Session::new(server, url);
        let close = client.clone();
        assert_eq!(client.protocol(), Some("moqt-18"));
        let (client, server) = tokio::join!(
            MoqSession::connect(client, None, Transport::WebTransport),
            MoqSession::accept(server, None, Transport::WebTransport),
        );
        let (client, mut publisher, _subscriber) = client.unwrap();
        let (server, _publisher, subscriber) = server.unwrap();
        assert_eq!(client.connection_path(), Some("/tenant"));
        assert_eq!(server.connection_path(), Some("/tenant"));
        let mut subscriber = subscriber.unwrap();
        let client_task = tokio::spawn(client.run());
        let server_task = tokio::spawn(server.run());
        let publication = publisher
            .publish_namespace_start(crate::coding::TrackNamespace::from_utf8_path("live"))
            .await
            .unwrap();
        let mut received = subscriber.published_namespace().await.unwrap();
        received.ok().unwrap();
        publication.ok().await.unwrap();
        drop(publication);
        received.closed().await.unwrap();
        close.close(0, "done");
        assert!(SessionError::WebTransport(close.closed().await).is_graceful_close());
        let _ = client_task.await.unwrap();
        let _ = server_task.await.unwrap();
    })
    .await
    .expect("independent transport test stalled");
}

#[tokio::test]
async fn cancelled_write_does_not_advance_unaccepted_bytes() {
    let (send, mut recv) = pipe(1);
    let mut send = SendStream::new(send);
    let mut data = Bytes::from_static(b"abcdef");
    assert_eq!(send.write_buf(&mut data).await.unwrap(), 3);
    {
        let write = send.write_buf(&mut data);
        tokio::pin!(write);
        assert!(futures::poll!(&mut write).is_pending());
    }
    assert_eq!(&data[..], b"def");
    assert_eq!(recv.read_chunk(3).await.unwrap().unwrap(), b"abc"[..]);
    assert_eq!(send.write_buf(&mut data).await.unwrap(), 3);
    assert!(data.is_empty());
    assert_eq!(recv.read_chunk(3).await.unwrap().unwrap(), b"def"[..]);
    recv.stop(42);
    assert_eq!(send.closed().await.unwrap(), Some(42));
}

#[tokio::test]
async fn invalid_writes_fail_without_consuming_the_buffer() {
    for accepted in [0, 4] {
        let mut send = SendStream::new(InvalidSend(accepted));
        let mut data = Bytes::from_static(b"abc");
        let err = send.write_buf(&mut data).await.unwrap_err();
        assert_eq!(&data[..], b"abc");
        match accepted {
            0 => assert!(matches!(err, Error::WriteZero)),
            _ => assert!(matches!(err, Error::InvalidWriteSize)),
        }
        assert!(SessionError::WebTransport(err).is_session_fatal());
    }
}

#[tokio::test]
async fn cancelled_read_preserves_payload_chunks_and_fin() {
    let (send, recv) = pipe(2);
    let mut send = SendStream::new(send);
    let mut recv = super::RecvStream::new(recv);
    {
        let read = recv.read(2);
        tokio::pin!(read);
        assert!(futures::poll!(&mut read).is_pending());
    }
    let mut data = Bytes::from_static(b"abcdef");
    while !data.is_empty() {
        send.write_buf(&mut data).await.unwrap();
    }
    send.finish().unwrap();
    let mut received = Vec::new();
    while let Some(chunk) = recv.read(2).await.unwrap() {
        assert!(chunk.len() <= 2);
        received.extend_from_slice(&chunk);
    }
    assert_eq!(&received, b"abcdef");
}

#[tokio::test]
async fn independent_backend_carries_raw_setup_metadata_and_fetch_payload() {
    use crate::{
        coding::{KeyValuePairs, Location, TrackNamespace, Value},
        data::{ExtensionHeaders, FetchRecord, FetchRecordObject},
        message::StandaloneFetch,
        session::FetchOkInfo,
        setup::ParameterType,
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, server) = pair();
        let client = Session::new(
            client,
            Url::parse("moqt://relay.example:4443/tenant?key=value").unwrap(),
        );
        let server = Session::new(server, Url::parse("moqt://localhost/").unwrap());
        let close = client.clone();
        let (client, server) = tokio::join!(
            MoqSession::connect(client, None, Transport::RawQuic),
            MoqSession::accept(server, None, Transport::RawQuic),
        );
        let (client, _publisher, mut subscriber) = client.unwrap();
        let (server, publisher, _subscriber) = server.unwrap();
        assert_eq!(client.connection_path(), Some("/tenant"));
        assert_eq!(server.connection_path(), Some("/tenant?key=value"));
        assert_eq!(
            server
                .setup_params()
                .get(ParameterType::Authority.into())
                .unwrap()
                .value,
            Value::BytesValue(b"relay.example:4443".to_vec()),
        );
        assert_eq!(
            server
                .setup_params()
                .get(ParameterType::Path.into())
                .unwrap()
                .value,
            Value::BytesValue(b"/tenant?key=value".to_vec()),
        );
        let mut publisher = publisher.unwrap();
        publisher.accept_fetch();
        let client_task = tokio::spawn(client.run());
        let server_task = tokio::spawn(server.run());

        let end = Location::new(1, 0);
        let mut fetch = subscriber
            .fetch(
                StandaloneFetch {
                    track_namespace: TrackNamespace::from_utf8_path("live"),
                    track_name: "video".into(),
                    start_location: Location::new(0, 0),
                    end_location: end,
                },
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = publisher.fetch_requested().await.unwrap();
        assert_eq!(requested.id(), fetch.id());
        let mut writer = requested
            .respond(FetchOkInfo {
                end_of_track: false,
                end_location: end,
                params: KeyValuePairs::default(),
                track_extensions: Default::default(),
            })
            .await
            .unwrap();
        let payload = b"independent transport payload";
        let record = FetchRecord::Object(FetchRecordObject {
            group_id: 0,
            subgroup_id: Some(0),
            object_id: 0,
            publisher_priority: 128,
            extension_headers: ExtensionHeaders::default(),
            payload_length: payload.len() as u64,
        });
        writer.write_record(&record).await.unwrap();
        writer.write_payload(payload).await.unwrap();
        writer.finish().await.unwrap();
        assert_eq!(fetch.ok().await.unwrap().end_location, end);
        assert_eq!(fetch.next().await.unwrap(), Some(record));
        let mut received = Vec::new();
        while let Some(chunk) = fetch.read_payload_chunk(2).await.unwrap() {
            received.extend_from_slice(&chunk);
        }
        assert_eq!(&received, payload);
        assert!(fetch.next().await.unwrap().is_none());
        close.close(0, "done");
        let _ = client_task.await.unwrap();
        let _ = server_task.await.unwrap();
    })
    .await
    .expect("independent FETCH test stalled");
}

#[tokio::test]
async fn datagrams_and_nonzero_close_survive_type_erasure() {
    let (a, b) = pair();
    let url = Url::parse("moqt://relay.example/scope?key=value").unwrap();
    let a = Session::new(a, url.clone());
    let b = Session::new(b, url.clone());
    assert_eq!(a.url(), &url);
    assert_eq!(a.max_datagram_size(), 1200);
    a.send_datagram(Bytes::from_static(b"payload"))
        .await
        .unwrap();
    assert_eq!(b.recv_datagram().await.unwrap(), b"payload"[..]);
    a.close(7, "test");
    let err = SessionError::WebTransport(b.closed().await);
    assert!(!err.is_graceful_close());
    assert!(err.is_session_fatal());
}

#[test]
fn generic_errors_preserve_stream_and_session_failure_boundaries() {
    for code in [0, 42, 65536] {
        let err = SessionError::WebTransport(Error::new(TestError::Stream(code)));
        assert!(err.is_stream_error());
        assert!(!err.is_session_fatal());
        assert!(!err.is_graceful_close());
    }
    let err = SessionError::WebTransport(Error::new(TestError::Failed));
    assert!(!err.is_stream_error());
    assert!(err.is_session_fatal());
    assert!(!err.is_graceful_close());
}

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
#[test]
fn native_close_and_closed_stream_compatibility() {
    use quinn::quinn::{ApplicationClose, ConnectionError, VarInt};
    for code in [
        0,
        1,
        quinn::proto::error_to_http3(0),
        quinn::proto::error_to_http3(7),
    ] {
        let native =
            quinn::SessionError::from(ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: VarInt::from_u64(code).unwrap(),
                reason: Bytes::from_static(b"close"),
            }));
        let err = SessionError::WebTransport(Error::new(native));
        assert_eq!(
            err.is_graceful_close(),
            code == 0 || code == quinn::proto::error_to_http3(0)
        );
    }
    let local = SessionError::WebTransport(Error::new(quinn::SessionError::ConnectionError(
        ConnectionError::LocallyClosed,
    )));
    assert!(local.is_graceful_close());
    let timeout = SessionError::WebTransport(Error::new(quinn::SessionError::ConnectionError(
        ConnectionError::TimedOut,
    )));
    assert!(!timeout.is_graceful_close());
    assert!(timeout.is_session_fatal());
    for err in [
        Error::new(quinn::ReadError::ClosedStream),
        Error::new(quinn::WriteError::ClosedStream),
    ] {
        assert!(!SessionError::WebTransport(err).is_session_fatal());
    }
    let invalid = SessionError::WebTransport(Error::new(quinn::ReadError::InvalidReset(
        VarInt::from_u32(42),
    )));
    assert!(invalid.is_session_fatal());
}
