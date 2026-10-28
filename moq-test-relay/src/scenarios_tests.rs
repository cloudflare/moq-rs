// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use moq_native_ietf::{quic, tls};
use moq_transport::{
    coding::{Decode, Encode, KeyValuePairs, Location, TrackNamespace, TrackNamespacePrefix},
    message::{self, GroupOrder},
    serve::{ServeError, Track},
    session::{Publisher, Session, SessionError, Subscriber},
};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::timeout,
};

use crate::{
    registry,
    run::{self, RunResult, TEST_NAMESPACE, TEST_TRACK},
};

struct Relay {
    client: quic::Client,
    url: url::Url,
    task: JoinHandle<Result<RunResult>>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    async fn start(test: &str, scheme: &str) -> Result<Self> {
        if std::env::var_os("MOQ_TEST_RELAY_TRACE").is_some() {
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .try_init();
        }
        Self::start_with_wait(test, scheme, Duration::from_millis(300)).await
    }

    async fn start_with_wait(
        test: &str,
        scheme: &str,
        post_verdict_wait: Duration,
    ) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
        ])?;
        let der = cert.cert.der().clone();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(vec![der.clone()], key.into())?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der)?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = tls::Config {
            client,
            server: Some(server),
            fingerprints: Vec::new(),
        };
        let mut endpoint = None;
        // These tests share a process and can run concurrently. Binding selects
        // an unused localhost port above the task's reserved-service threshold.
        for port in 16000..16100 {
            if let Ok(config) = quic::Config::new(
                SocketAddr::from(([127, 0, 0, 1], port)),
                std::env::var_os("MOQ_TEST_RELAY_QLOG").map(Into::into),
                tls.clone(),
            ) {
                endpoint = Some(quic::Endpoint::new(config)?);
                break;
            }
        }
        let endpoint = endpoint.context("no free test port in 16000..16100")?;
        let mut server = endpoint.server.context("server missing")?;
        let url = url::Url::parse(&format!(
            "{scheme}://127.0.0.1:{}",
            server.local_addr()?.port()
        ))?;
        let (name, variant) = test
            .split_once('@')
            .map_or((test, None), |(name, variant)| (name, Some(variant)));
        let entry = registry::find(name, variant).context("test entry missing")?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let task = tokio::spawn(async move {
            run::run(
                &mut server,
                entry,
                Duration::from_secs(2),
                post_verdict_wait,
                &mut terminate,
            )
            .await
        });
        Ok(Self {
            client: endpoint.client,
            url,
            task,
        })
    }

    async fn connect(
        &self,
        tasks: &mut JoinSet<Result<(), SessionError>>,
    ) -> Result<(web_transport::Session, Publisher, Subscriber, String)> {
        timeout(Duration::from_secs(2), async {
            let (transport_session, cid, transport) = self.client.connect(&self.url, None).await?;
            let close = transport_session.clone();
            let (session, publisher, subscriber) =
                Session::connect(transport_session, None, transport).await?;
            tasks.spawn(session.run());
            Ok((close, publisher, subscriber, cid))
        })
        .await
        .context("client SETUP timed out")?
    }

    async fn verdict(&mut self) -> Result<RunResult> {
        timeout(Duration::from_secs(5), &mut self.task)
            .await
            .context("relay hung")??
    }
}

async fn read_frame(reader: &mut web_transport::RecvStream) -> Result<(u64, Vec<u8>)> {
    let mut bytes = Vec::new();
    loop {
        let decoded = (|| -> Result<_> {
            let mut payload = bytes.as_slice();
            let kind = u64::decode(&mut payload)?;
            let len = u16::decode(&mut payload)? as usize;
            anyhow::ensure!(payload.len() >= len, "incomplete frame");
            Ok((kind, payload[..len].to_vec()))
        })();
        if let Ok(frame) = decoded {
            return Ok(frame);
        }
        let chunk = reader.read(1).await?.context("response stream ended")?;
        bytes.extend_from_slice(&chunk);
    }
}

async fn publish_namespace(
    session: &web_transport::Session,
    namespace: &str,
) -> Result<(web_transport::SendStream, web_transport::RecvStream)> {
    // Publisher::publish_namespace has no public acceptance future: it keeps
    // waiting for subscriptions after REQUEST_OK. Use the public message codec
    // here so the test really waits for that response, rather than a sleep.
    let request = message::Message::PublishNamespace(message::PublishNamespace {
        id: 0,
        track_namespace: TrackNamespace::from_utf8_path(namespace),
        params: KeyValuePairs::default(),
    });
    let (writer, mut reader) = send_request(session, request).await?;
    let (kind, payload) = timeout(Duration::from_secs(2), read_frame(&mut reader))
        .await
        .context("PUBLISH_NAMESPACE response timed out")??;
    assert_eq!(
        kind,
        message::wire_id::RequestOk,
        "publisher must receive REQUEST_OK before subscriber starts"
    );
    KeyValuePairs::decode_message_params(&mut payload.as_slice())?;
    Ok((writer, reader))
}

async fn write_bytes(writer: &mut web_transport::SendStream, bytes: &[u8]) -> Result<()> {
    let mut sent = 0;
    while sent < bytes.len() {
        sent += writer.write(&bytes[sent..]).await?;
    }
    Ok(())
}

async fn send_request(
    session: &web_transport::Session,
    request: message::Message,
) -> Result<(web_transport::SendStream, web_transport::RecvStream)> {
    let (mut writer, reader) = session.open_bi().await?;
    let mut frame = Vec::new();
    request.encode(&mut frame)?;
    write_bytes(&mut writer, &frame).await?;
    Ok((writer, reader))
}

async fn connect_raw(
    relay: &Relay,
) -> Result<(
    web_transport::Session,
    web_transport::SendStream,
    web_transport::RecvStream,
)> {
    let (session, _, _) = relay.client.connect(&relay.url, None).await?;
    let mut control = session.open_uni().await?;
    let mut setup = Vec::new();
    moq_transport::setup::Setup {
        params: KeyValuePairs::default(),
    }
    .encode(&mut setup)?;
    write_bytes(&mut control, &setup).await?;
    let mut peer_control = session.accept_uni().await?;
    assert_eq!(
        read_frame(&mut peer_control).await?.0,
        moq_transport::setup::SETUP_TYPE
    );
    Ok((session, control, peer_control))
}

async fn cancel_namespace(
    streams: &mut (web_transport::SendStream, web_transport::RecvStream),
) -> Result<()> {
    streams.0.reset(1);
    // The response reset confirms the peer has processed the request reset.
    assert!(timeout(Duration::from_secs(1), streams.1.read(1024))
        .await?
        .is_err());
    Ok(())
}

async fn namespace_subscription(
    session: &web_transport::Session,
    prefix: &str,
) -> Result<(web_transport::SendStream, web_transport::RecvStream)> {
    let (writer, mut reader) = send_request(
        session,
        message::SubscribeNamespace {
            id: 0,
            track_namespace_prefix: TrackNamespacePrefix::from_utf8_path(prefix),
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    assert_eq!(
        timeout(Duration::from_secs(1), read_frame(&mut reader))
            .await??
            .0,
        message::wire_id::RequestOk
    );
    Ok((writer, reader))
}

async fn namespace_update(
    reader: &mut web_transport::RecvStream,
    done: bool,
    suffix: &str,
) -> Result<()> {
    let (kind, payload) = timeout(Duration::from_secs(1), read_frame(reader)).await??;
    let expected = TrackNamespacePrefix::from_utf8_path(suffix);
    if done {
        assert_eq!(kind, message::wire_id::NamespaceDone);
        assert_eq!(
            message::NamespaceDone::decode(&mut payload.as_slice())?.track_namespace_suffix,
            expected
        );
    } else {
        assert_eq!(kind, message::wire_id::Namespace);
        assert_eq!(
            message::Namespace::decode(&mut payload.as_slice())?.track_namespace_suffix,
            expected
        );
    }
    Ok(())
}

fn track(
    namespace: &str,
    name: &str,
) -> (
    moq_transport::serve::TrackWriter,
    moq_transport::serve::TrackReader,
) {
    Track::new(TrackNamespace::from_utf8_path(namespace), name).produce()
}

mod namespace_tests;
mod routing_tests;
mod session_tests;
mod subscription_tests;
