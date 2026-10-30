// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use std::time::Duration;
use web_transport::quinn::{self as wt, quinn};

// The object fits in both flow-control windows and the sender's buffer, but a
// delayed path keeps payload buffered after the forwarder has queued FIN.
const PAYLOAD: usize = 512 * 1024;

struct Proxy(
    tokio::task::JoinHandle<()>,
    Arc<std::sync::atomic::AtomicBool>,
);
impl Drop for Proxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn pair(
    raw: bool,
) -> (
    web_transport::Session,
    web_transport::Session,
    quinn::Connection,
    Proxy,
) {
    pair_with_ack_control(raw, false).await
}

async fn pair_with_ack_control(
    raw: bool,
    controlled: bool,
) -> (
    web_transport::Session,
    web_transport::Session,
    quinn::Connection,
    Proxy,
) {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let alpn = if raw {
        b"moqt-18".to_vec()
    } else {
        wt::ALPN.as_bytes().to_vec()
    };
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            cert.key_pair.serialize_der().try_into().unwrap(),
        )
        .unwrap();
    crypto.alpn_protocols = vec![alpn.clone()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto).unwrap(),
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window((2 * PAYLOAD as u32).into());
    if controlled {
        transport.max_concurrent_uni_streams(4096u32.into());
    }
    config.transport_config(Arc::new(transport));
    let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = server.local_addr().unwrap();
    // Delay both directions by 75 ms without limiting flow control: writes can
    // finish locally while congestion control still holds most payload bytes.
    let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let proxy_addr = socket.local_addr().unwrap();
    let hold_acks = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hold_reverse = hold_acks.clone();
    let proxy = Proxy(
        tokio::spawn(async move {
            let mut client_addr = None;
            let mut pending = FuturesUnordered::new();
            let mut buf = vec![0; 65536];
            loop {
                tokio::select! {
                    result = socket.recv_from(&mut buf) => {
                        let (len, from) = result.unwrap();
                        if from == addr && hold_reverse.load(Ordering::SeqCst) {
                            continue;
                        }
                        let to = if from == addr { client_addr.unwrap() } else {
                            client_addr = Some(from); addr
                        };
                        let packet = buf[..len].to_vec();
                        let socket = socket.clone();
                        pending.push(async move {
                            if !controlled {
                                tokio::time::sleep(Duration::from_millis(75)).await;
                            }
                            socket.send_to(&packet, to).await.unwrap();
                        });
                    },
                    _ = pending.next(), if !pending.is_empty() => {},
                }
            }
        }),
        hold_acks,
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![alpn];
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));
    if controlled {
        // Keep congestion/stream credit from hiding the application slot limit
        // while the proxy withholds every ACK after the handshake.
        let mut congestion = quinn::congestion::CubicConfig::default();
        congestion.initial_window(1024 * 1024);
        let mut transport = quinn::TransportConfig::default();
        transport.congestion_controller_factory(Arc::new(congestion));
        client_config.transport_config(Arc::new(transport));
    }
    client.set_default_client_config(client_config);
    let accept = async { server.accept().await.unwrap().await.unwrap() };
    let connect = client.connect(proxy_addr, "localhost").unwrap();
    let (sender, receiver) = tokio::join!(connect, accept);
    let sender = sender.unwrap();
    let observer = receiver.clone();
    let url = url::Url::parse(&format!("https://localhost:{}/", addr.port())).unwrap();
    let (sender, receiver) = if raw {
        (
            wt::Session::raw(sender, url.clone(), wt::proto::ConnectResponse::default()),
            wt::Session::raw(receiver, url, wt::proto::ConnectResponse::default()),
        )
    } else {
        let (sender, receiver) = tokio::join!(wt::Session::connect(sender, url), async {
            wt::Request::accept(receiver)
                .await
                .unwrap()
                .ok()
                .await
                .unwrap()
        });
        (sender.unwrap(), receiver)
    };
    (sender.into(), receiver.into(), observer, proxy)
}

fn forwarder(session: web_transport::Session) -> (ObjectForwarder, ObjectForwarderRecv) {
    let (outgoing, _outgoing_recv) = crate::watch::Queue::default().split();
    let (bidi_task_tx, _bidi_task_rx) = tokio::sync::mpsc::unbounded_channel();
    ObjectForwarder::new(
        Publisher::new(
            outgoing,
            session,
            None,
            crate::session::RequestId::new(0, 1),
            bidi_task_tx,
        ),
        42,
        None,
    )
}

async fn source() -> (serve::SubgroupsWriter, serve::SubgroupsReader) {
    let (writer, reader) = serve::Track::new(
        crate::coding::TrackNamespace::from_utf8_path("test"),
        "track",
    )
    .produce();
    let mut writer = writer.subgroups().unwrap();
    let mut subgroup = writer
        .create(serve::Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 128,
        })
        .unwrap();
    let mut object = subgroup.create_with_id(0, PAYLOAD, None).unwrap();
    object.write(bytes::Bytes::from(vec![7; PAYLOAD])).unwrap();
    drop(object);
    drop(subgroup);
    let TrackReaderMode::Subgroups(reader) = reader.mode().await.unwrap() else {
        panic!("expected subgroups")
    };
    (writer, reader)
}

async fn cancelled_finished_stream(raw: bool, unsubscribe: bool) {
    let (sender, receiver, observer, _proxy) = pair(raw).await;
    let (mut forwarder, mut cancel) = forwarder(sender);
    let (_source, reader) = source().await;
    let serve = forwarder.serve_subgroups(
        reader,
        DeliveryFilter {
            forward: true,
            start_location: None,
            end_group_id: None,
        },
    );
    tokio::pin!(serve);
    let mut stream = tokio::select! {
        stream = receiver.accept_uni() => stream.unwrap(),
        result = &mut serve => panic!("forwarder ended early: {result:?}"),
    };
    // Let the forwarder enqueue the finite subgroup and FIN. The receiver does
    // not read; the delayed path keeps most of the object unacknowledged.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        result = &mut serve => panic!("forwarder ended early: {result:?}"),
    }
    let before = observer.stats().frame_rx.reset_stream;
    let started = tokio::time::Instant::now();
    let code = if unsubscribe {
        u32::from(DataStreamResetCode::Cancelled)
    } else {
        6
    };
    if unsubscribe {
        cancel.recv_unsubscribe().unwrap();
    } else {
        stream.stop(code);
    }
    let receive_reset = async {
        // A locally stopped receive stream no longer exposes a reset code;
        // observe the actual incoming RESET_STREAM frame in that case.
        if unsubscribe {
            assert_eq!(stream.closed().await.unwrap(), Some(code as u8));
        }
        while observer.stats().frame_rx.reset_stream == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(observer.stats().frame_rx.reset_stream - before, 1);
        println!(
            "raw={raw} unsubscribe={unsubscribe} resets=1 cancellation_ms={}",
            started.elapsed().as_millis()
        );
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        if unsubscribe {
            let (result, ()) = tokio::join!(&mut serve, receive_reset);
            assert!(matches!(
                result,
                Err(SessionError::Serve(ServeError::Cancel))
            ));
        } else {
            tokio::select! {
                () = receive_reset => {},
                result = &mut serve => panic!("forwarder ended early: {result:?}"),
            }
        }
    })
    .await
    .expect("finished stream must be reset promptly, without draining its payload");
}

#[tokio::test]
async fn stop_sending_after_finish_raw() {
    cancelled_finished_stream(true, false).await;
}
#[tokio::test]
async fn stop_sending_after_finish_webtransport() {
    cancelled_finished_stream(false, false).await;
}
#[tokio::test]
async fn unsubscribe_after_finish_raw() {
    cancelled_finished_stream(true, true).await;
}
#[tokio::test]
async fn unsubscribe_after_finish_webtransport() {
    cancelled_finished_stream(false, true).await;
}

async fn cancellation_is_scoped_and_ack_releases(raw: bool) {
    let (sender, receiver, observer, _proxy) = pair(raw).await;
    let (mut cancelled, mut cancel) = forwarder(sender.clone());
    let (mut healthy, _healthy_recv) = forwarder(sender);
    let (_cancelled_source, cancelled_reader) = source().await;
    let (healthy_source, healthy_reader) = source().await;
    drop(healthy_source);
    let filter = DeliveryFilter {
        forward: true,
        start_location: None,
        end_group_id: None,
    };
    let cancelled = cancelled.serve_subgroups(cancelled_reader, filter);
    let healthy = healthy.serve_subgroups(healthy_reader, filter);
    // Start the cancelled stream first so the peer can identify it unambiguously.
    tokio::pin!(cancelled);
    let mut stale = tokio::select! {
        stream = receiver.accept_uni() => stream.unwrap(),
        result = &mut cancelled => panic!("ended early: {result:?}"),
    };
    let receive = async {
        cancel.recv_unsubscribe().unwrap();
        assert_eq!(
            stale.closed().await.unwrap(),
            Some(u32::from(DataStreamResetCode::Cancelled) as u8)
        );
        let mut stream = receiver.accept_uni().await.unwrap();
        let mut bytes = 0;
        while let Some(chunk) = stream.read(65536).await.unwrap() {
            bytes += chunk.len();
        }
        assert!(
            bytes > PAYLOAD,
            "healthy subgroup header plus complete payload"
        );
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        let (cancelled, healthy, ()) = tokio::join!(&mut cancelled, healthy, receive);
        assert!(matches!(
            cancelled,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
        healthy.unwrap();
    })
    .await
    .expect("FIN ACK must release the completed forwarding future");
    assert_eq!(observer.stats().frame_rx.reset_stream, 1);
}

#[tokio::test]
async fn cancellation_is_scoped_and_fin_ack_releases_raw() {
    cancellation_is_scoped_and_ack_releases(true).await;
}

#[tokio::test]
async fn cancellation_is_scoped_and_fin_ack_releases_webtransport() {
    cancellation_is_scoped_and_ack_releases(false).await;
}

async fn finished_subgroups_release_active_slots(raw: bool, overflow: bool) {
    let (sender, receiver, observer, proxy) = pair_with_ack_control(raw, true).await;
    // Reverse packets containing ACKs are discarded from this point onward.
    // No data stream has been opened yet, so none can already have its FIN ACKed.
    proxy.1.store(true, Ordering::SeqCst);
    let (mut forwarder, mut cancel) = forwarder(sender);
    let opened = forwarder.stream_count.clone();
    let (writer, reader) = serve::Track::new(
        crate::coding::TrackNamespace::from_utf8_path("test"),
        "short-subgroups",
    )
    .produce();
    let mut writer = writer.subgroups().unwrap();
    let TrackReaderMode::Subgroups(reader) = reader.mode().await.unwrap() else {
        panic!("expected subgroups")
    };
    let count = if overflow {
        MAX_PENDING_SUBGROUP_ACKS + 1
    } else {
        2 * MAX_CONCURRENT_SUBGROUPS
    };
    let produce = async move {
        for subgroup_id in 0..count as u64 {
            let mut subgroup = writer
                .create(serve::Subgroup {
                    group_id: 0,
                    subgroup_id,
                    priority: 128,
                })
                .unwrap();
            let mut object = subgroup.create_with_id(0, 1, None).unwrap();
            object.write(bytes::Bytes::from_static(b"x")).unwrap();
            drop(object);
            drop(subgroup);
            // Also works with the draft-16 latest-only reader: don't overwrite
            // a subgroup before the forwarder has taken its reader.
            while opened.get() <= subgroup_id {
                tokio::task::yield_now().await;
            }
        }
        writer
    };
    let receive = async {
        for _ in 0..count {
            let mut stream = receiver.accept_uni().await.unwrap();
            let mut bytes = 0;
            loop {
                match stream.read(65536).await {
                    Ok(Some(chunk)) => bytes += chunk.len(),
                    Ok(None) => break,
                    Err(_) if overflow => break,
                    Err(err) => panic!("stream failed: {err:?}"),
                }
            }
            if !overflow {
                assert!(bytes > 1, "complete subgroup header and payload");
            }
        }
    };
    let serve = forwarder.serve_subgroups(
        reader,
        DeliveryFilter {
            forward: true,
            start_location: None,
            end_group_id: None,
        },
    );
    tokio::pin!(serve);
    let _writer = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            (writer, ()) = async { tokio::join!(produce, receive) } => {
                if overflow {
                    assert!(matches!((&mut serve).await,
                        Err(SessionError::Serve(ServeError::Closed(code)))
                            if code == message::PublishDoneCode::TooFarBehind as u64));
                }
                Some(writer)
            },
            result = &mut serve => {
                assert!(overflow, "forwarder ended early: {result:?}");
                assert!(matches!(result,
                    Err(SessionError::Serve(ServeError::Closed(code)))
                        if code == message::PublishDoneCode::TooFarBehind as u64));
                None
            },
        }
    })
    .await
    .expect("more than 128 finished, unacknowledged subgroups must start");
    if !overflow {
        assert_eq!(observer.stats().frame_rx.reset_stream, 0);
        cancel.recv_unsubscribe().unwrap();
        assert!(matches!(
            serve.await,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
    }
    // Cancellation still owns and resets every FIN waiter in the separate set.
    tokio::time::timeout(Duration::from_secs(3), async {
        while observer.stats().frame_rx.reset_stream < count as u64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.stats().frame_rx.reset_stream, count as u64);
}

#[tokio::test]
async fn finished_subgroups_release_active_slots_raw() {
    finished_subgroups_release_active_slots(true, false).await;
}

#[tokio::test]
async fn finished_subgroups_release_active_slots_webtransport() {
    finished_subgroups_release_active_slots(false, false).await;
}

#[tokio::test]
async fn pending_ack_budget_overflow_resets_raw() {
    finished_subgroups_release_active_slots(true, true).await;
}

#[tokio::test]
async fn pending_ack_budget_overflow_resets_webtransport() {
    finished_subgroups_release_active_slots(false, true).await;
}
