// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Both public media pipelines over a real in-process pull-through relay.

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use moq_native_ietf::{quic, tls};
use moq_pub::{serve_fetches, FetchHistory, Media};
use moq_relay_ietf::{
    Coordinator, CoordinatorContext, CoordinatorResult, NamespaceOrigin, NamespaceRegistration,
    RelayConfig,
};
use moq_sub::{
    media::{FetchMode, JoiningStart},
    reexports::moq_transport::coding::Location,
};
use moq_transport::{
    coding::TrackNamespace,
    serve::Tracks,
    session::{Publisher, Subscriber},
};
use mp4::WriteBox;
use tokio::io::AsyncReadExt;

struct LocalCoordinator;
#[async_trait]
impl Coordinator for LocalCoordinator {
    async fn register_namespace(
        &self,
        _: Option<&str>,
        _: &TrackNamespace,
        _: &CoordinatorContext,
    ) -> CoordinatorResult<NamespaceRegistration> {
        Ok(NamespaceRegistration::new(()))
    }
    async fn unregister_namespace(
        &self,
        _: Option<&str>,
        _: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        Ok(())
    }
    async fn lookup(
        &self,
        _: Option<&str>,
        _: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<moq_native_ietf::Client>)> {
        Err(moq_relay_ietf::CoordinatorError::NamespaceNotFound)
    }
}

fn tls_config() -> tls::Config {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let der = certificate.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![der.clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(der).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls::Config {
        server: Some(server),
        client,
        fingerprints: Vec::new(),
    }
}

fn init() -> Bytes {
    let mut moov = mp4::MoovBox::default();
    moov.traks.push(Default::default());
    let track = &mut moov.traks[0];
    track.tkhd.track_id = 1;
    track.mdia.hdlr.handler_type = "vide".parse().unwrap();
    track.mdia.mdhd.timescale = 1000;
    track.mdia.minf.stbl.stsd.avc1 = Some(Default::default());
    track.mdia.minf.stbl.stco = Some(Default::default());
    let mut bytes = Vec::new();
    mp4::FtypBox::default().write_box(&mut bytes).unwrap();
    moov.write_box(&mut bytes).unwrap();
    bytes.into()
}

fn atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    [
        ((payload.len() + 8) as u32).to_be_bytes().as_slice(),
        kind,
        payload,
    ]
    .concat()
}

/// Structurally valid fragments with distinguishable payloads. The byte-level
/// comparison checks full moof+mdat Objects, without a codec or ffmpeg dependency.
fn chunk(sequence: u32, keyframe: bool) -> Bytes {
    let mfhd = atom(
        b"mfhd",
        &[0u32.to_be_bytes(), sequence.to_be_bytes()].concat(),
    );
    let tfhd = atom(b"tfhd", &[0u32.to_be_bytes(), 1u32.to_be_bytes()].concat());
    let tfdt = atom(
        b"tfdt",
        &[
            0x01000000u32.to_be_bytes().as_slice(),
            (u64::from(sequence) * 1000).to_be_bytes().as_slice(),
        ]
        .concat(),
    );
    let flags: u32 = if keyframe { 0x02000000 } else { 0x01010000 };
    let trun = atom(
        b"trun",
        &[
            0x400u32.to_be_bytes(),
            1u32.to_be_bytes(),
            flags.to_be_bytes(),
        ]
        .concat(),
    );
    let traf = atom(b"traf", &[tfhd, tfdt, trun].concat());
    [
        atom(b"moof", &[mfhd, traf].concat()),
        atom(b"mdat", &sequence.to_be_bytes()),
    ]
    .concat()
    .into()
}

async fn read_exact_media(reader: &mut tokio::io::DuplexStream, expected: &[u8]) {
    let mut received = vec![0; expected.len()];
    reader.read_exact(&mut received).await.unwrap();
    assert_eq!(received, expected);
}

async fn exercise(mode: FetchMode, slow_history: bool) {
    let tls = tls_config();
    let endpoint = quic::Endpoint::new(
        quic::Config::new("127.0.0.1:0".parse().unwrap(), None, tls.clone()).unwrap(),
    )
    .unwrap();
    let port = endpoint.client.local_addr().unwrap().port();
    let url = format!("https://localhost:{port}/").parse().unwrap();
    let relay = RelayConfig {
        bind: None,
        endpoints: vec![endpoint],
        tls: tls.clone(),
        qlog_dir: None,
        mlog_dir: None,
        announce: None,
        node: None,
        coordinator: Arc::new(LocalCoordinator),
        session: Default::default(),
        connection_tagger: None,
    }
    .build()
    .unwrap();
    let test = async {
        let endpoint = quic::Endpoint::new(
            quic::Config::new("127.0.0.1:0".parse().unwrap(), None, tls).unwrap(),
        )
        .unwrap();
        let (wire, _, transport) = endpoint.client.connect(&url, None).await.unwrap();
        let (session, mut publisher) = Publisher::connect(wire, transport).await.unwrap();
        publisher.accept_fetch();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(session.run());
        let namespace = TrackNamespace::from_utf8_path("test/media");
        let (writer, _, reader) = Tracks::new(namespace.clone()).produce();
        let history = FetchHistory::new(NonZeroUsize::new(4).unwrap());
        let mut media = Media::new_with_fetch(writer, None, Some(history.clone())).unwrap();
        let init = init();
        media.parse(&mut init.clone()).unwrap();
        let chunks = [
            chunk(0, true),
            chunk(1, false),
            chunk(2, true),
            chunk(3, false),
        ];
        for chunk in &chunks {
            media.parse(&mut chunk.clone()).unwrap();
        }
        let publication = publisher
            .publish_namespace_start(namespace.clone())
            .await
            .unwrap();
        publication.ok().await.unwrap();
        let mut serving = tokio::task::JoinSet::new();
        serving.spawn(serve_fetches(publisher, Some(history)));
        serving.spawn(async move {
            let mut subscriptions = tokio::task::JoinSet::new();
            while let Some(request) = publication.subscribed().await.unwrap() {
                subscriptions.spawn(Publisher::serve_subscribe(request, reader.clone()));
            }
            Ok(())
        });
        let (wire, _, transport) = endpoint.client.connect(&url, None).await.unwrap();
        let (session, subscriber) = Subscriber::connect(wire, transport).await.unwrap();
        tasks.spawn(session.run());
        // A one-byte output pipe deliberately stalls historical FETCH draining
        // while live Objects continue arriving through the relay.
        let (out, mut received) = tokio::io::duplex(if slow_history { 1 } else { 64 * 1024 });
        let mut playback = moq_sub::media::Media::new_with_fetch(
            subscriber,
            Tracks::new(namespace),
            out,
            true,
            Some(mode),
        )
        .await
        .unwrap();
        let run = playback.run();
        tokio::pin!(run);
        let verify = async {
            let expected = [init.as_ref(), &chunks.concat()].concat();
            if slow_history {
                let first = init.len() + 1;
                read_exact_media(&mut received, &expected[..first]).await;
                let mut live = Vec::new();
                for sequence in 4..132 {
                    let next = chunk(sequence, false);
                    media.parse(&mut next.clone()).unwrap();
                    live.extend_from_slice(&next);
                }
                let next = chunk(132, true);
                media.parse(&mut next.clone()).unwrap();
                live.extend_from_slice(&next);
                // Keep history blocked long enough for the live collector to
                // encounter the queue limit. Only the receiver is slowed;
                // the sessions and relay continue to be polled by the test.
                tokio::time::sleep(Duration::from_millis(100)).await;
                read_exact_media(&mut received, &expected[first..]).await;
                read_exact_media(&mut received, &live).await;
            } else {
                read_exact_media(&mut received, &expected).await;
            }
            if matches!(mode, FetchMode::Joining(_)) {
                // Live starts inside the boundary group, then crosses a group.
                if !slow_history {
                    for next in [chunk(4, false), chunk(5, true)] {
                        media.parse(&mut next.clone()).unwrap();
                        read_exact_media(&mut received, &next).await;
                    }
                }
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), received.read_u8())
                        .await
                        .is_err(),
                    "duplicate media after handoff"
                );
            }
        };
        if matches!(mode, FetchMode::Standalone { .. }) {
            let (result, ()) = tokio::join!(&mut run, verify);
            result.unwrap();
        } else {
            tokio::select! { result = &mut run => panic!("playback ended early: {result:?}"), () = verify => {} }
        }
    };
    tokio::select! {
        result = relay.run() => panic!("relay ended early: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(15), test) => result.expect("media FETCH test timed out"),
    }
}

#[tokio::test]
async fn cold_relay_relative_join_replays_then_continues_without_gaps_or_duplicates() {
    exercise(FetchMode::Joining(JoiningStart::Relative(1)), false).await;
}

#[tokio::test]
async fn cold_relay_absolute_join_replays_then_continues_without_gaps_or_duplicates() {
    exercise(FetchMode::Joining(JoiningStart::Absolute(0)), false).await;
}

#[tokio::test]
async fn slow_history_join_preserves_more_than_32_live_objects() {
    exercise(FetchMode::Joining(JoiningStart::Relative(1)), true).await;
}

#[tokio::test]
async fn standalone_fetch_media_playback_is_finite_and_exact() {
    exercise(
        FetchMode::Standalone {
            start: Location::new(0, 0),
            end: Location::new(1, 0),
        },
        false,
    )
    .await;
}
