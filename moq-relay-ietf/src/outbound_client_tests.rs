// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::connect_forward;
use crate::test::test_endpoint;
use moq_native_ietf::{
    client::{ConnectFuture, Connector},
    Client,
};
use moq_transport::session::{SessionConfig, Transport};
use std::{net::SocketAddr, time::Duration};
use url::Url;

#[tokio::test]
async fn opaque_quinn_raw_and_wt_preserve_metadata_and_transfer_bytes() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for (scheme, transport) in [
            ("moqt", Transport::RawQuic),
            ("https", Transport::WebTransport),
        ] {
            let endpoint = test_endpoint();
            let local = endpoint.client.local_addr().unwrap();
            let client = Client::from(endpoint.client);
            let url =
                Url::parse(&format!("{scheme}://localhost:4443/tenant?preserved=yes")).unwrap();
            let mut server = endpoint.server;
            // Direct address changes destination, not SNI, certificate name or URL.
            let (connected, accepted) =
                tokio::join!(client.connect(&url, Some(endpoint.addr)), server.accept());
            let connected = connected.unwrap();
            let (peer, info) = accepted.unwrap();
            assert_eq!(connected.transport, transport);
            assert_eq!(info.transport, transport);
            assert_eq!(connected.session.url(), &url);
            assert_eq!(
                connected.initial_destination_cid.as_deref(),
                Some(info.id.as_str())
            );
            assert_eq!(info.server_name.as_deref(), Some("localhost"));
            assert_eq!(info.remote_address.port(), local.port());
            let payload = b"opaque client real transport bytes";
            let send = async {
                let mut stream = connected.session.open_uni().await.unwrap();
                let mut remaining = payload.as_slice();
                while !remaining.is_empty() {
                    let n = stream.write(remaining).await.unwrap();
                    remaining = &remaining[n..];
                }
                stream.finish().unwrap();
                stream.closed().await.unwrap();
            };
            let recv = async {
                let mut stream = peer.accept_uni().await.unwrap();
                let mut received = Vec::new();
                while let Some(bytes) = stream.read(1024).await.unwrap() {
                    received.extend_from_slice(&bytes);
                }
                assert_eq!(received, payload);
            };
            tokio::join!(send, recv);
            connected.session.close(0, "fixture complete");
        }
    })
    .await
    .expect("raw/WT opaque traffic timed out");
}

#[tokio::test]
async fn opaque_quinn_keeps_original_trust_and_url_name_with_direct_address() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = test_endpoint();
        let unrelated = test_endpoint();
        let wrong_root = Client::from(unrelated.client);
        let client = Client::from(endpoint.client);
        let mut server = endpoint.server;
        let good = Url::parse("https://localhost/tenant").unwrap();
        let wrong_name = Url::parse("https://wrong-name.invalid/tenant").unwrap();
        let attempts = async {
            for (selected, url) in [(&client, &wrong_name), (&wrong_root, &good)] {
                let error = selected
                    .connect(url, Some(endpoint.addr))
                    .await
                    .err()
                    .expect("TLS policy was lost");
                assert!(
                    format!("{error:#}").contains("certificate"),
                    "expected certificate rejection"
                );
            }
            client.connect(&good, Some(endpoint.addr)).await.unwrap()
        };
        let (connected, accepted) = tokio::join!(attempts, server.accept());
        let (peer, _) = accepted.unwrap();
        connected.session.close(0, "fixture complete");
        drop(peer);
    })
    .await
    .expect("TLS policy test timed out");
}

struct Refuse;
impl Connector for Refuse {
    fn connect<'a>(&'a self, _: &'a Url, addr: Option<SocketAddr>) -> ConnectFuture<'a> {
        assert!(addr.is_none(), "announce does not invent a direct address");
        Box::pin(async {
            Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "custom refusal").into())
        })
    }
}

#[tokio::test]
async fn announce_opaque_connect_keeps_connection_and_setup_error_stages() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let url = Url::parse("moqt://localhost/tenant").unwrap();
        let error = connect_forward(&Client::new(Refuse), &url, SessionConfig::default())
            .await
            .err()
            .unwrap();
        assert_eq!(error.to_string(), "failed to establish forward connection");
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        for scheme in ["moqt", "https"] {
            let endpoint = test_endpoint();
            let client = Client::from(endpoint.client);
            let url = Url::parse(&format!(
                "{scheme}://localhost:{}/tenant?not-scope=yes",
                endpoint.addr.port()
            ))
            .unwrap();
            let mut server = endpoint.server;
            let accept = async {
                let (session, info) = server.accept().await.unwrap();
                moq_transport::session::Session::accept(session, None, info.transport)
                    .await
                    .unwrap()
            };
            let (forward, peer) = tokio::join!(
                connect_forward(&client, &url, SessionConfig::default()),
                accept
            );
            let (session, _, _) = forward.unwrap();
            assert_eq!(session.connection_path(), Some("/tenant"));
            assert_eq!(
                peer.0.connection_path(),
                Some(if scheme == "moqt" {
                    "/tenant?not-scope=yes"
                } else {
                    "/tenant"
                })
            );
            drop(session);
            drop(peer);
        }
        let endpoint = test_endpoint();
        let client = Client::from(endpoint.client);
        let url = Url::parse(&format!("moqt://localhost:{}/tenant", endpoint.addr.port())).unwrap();
        let mut server = endpoint.server;
        let refuse_setup = async {
            let (session, _) = server.accept().await.unwrap();
            session.close(2, "setup refused");
        };
        let (result, ()) = tokio::join!(
            connect_forward(&client, &url, SessionConfig::default()),
            refuse_setup
        );
        assert_eq!(
            result.err().unwrap().to_string(),
            "failed to establish forward session"
        );
    })
    .await
    .expect("announce stages timed out");
}
