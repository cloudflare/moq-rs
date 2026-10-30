// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Real relay integration: retained history, Joining FETCH, and live handoff.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use moq_native_ietf::{quic, tls};
use moq_relay_ietf::{
    Coordinator, CoordinatorContext, CoordinatorResult, NamespaceOrigin, NamespaceRegistration,
    RelayConfig,
};
use moq_transport::{
    coding::{KeyValuePairs, Location, TrackNamespace},
    data::FetchRecord,
    message::StandaloneFetch,
    reexports::bytes::Bytes,
    serve::{Subgroup, SubgroupWriter, Track, Tracks},
    session::{Publisher, Subscriber},
};

use crate::{clock, history::History};

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
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        Err(moq_relay_ietf::CoordinatorError::NamespaceNotFound)
    }
}

fn tls_config() -> tls::Config {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
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

fn write(history: &History, subgroup: &mut SubgroupWriter, object: u64, value: &'static str) {
    let payload = Bytes::from_static(value.as_bytes());
    history
        .push(Location::new(subgroup.group_id, object), payload.clone())
        .unwrap();
    subgroup.write(payload).unwrap();
}

#[tokio::test]
async fn relay_joining_fetch_replays_history_then_continues_live_once() {
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
        let namespace = TrackNamespace::from_utf8_path("clock");
        let (mut tracks, _, tracks_reader) = Tracks::new(namespace.clone()).produce();
        let mut subgroups = tracks.create("now").unwrap().subgroups().unwrap();
        let history = History::new(4);
        let mut old = subgroups
            .create(Subgroup {
                group_id: 10,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        write(&history, &mut old, 0, "2026-10-29 10:10:");
        write(&history, &mut old, 1, "58");
        write(&history, &mut old, 2, "59");
        old.finish().unwrap();
        let mut current = subgroups
            .create(Subgroup {
                group_id: 11,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        write(&history, &mut current, 0, "2026-10-29 10:11:");
        write(&history, &mut current, 1, "00");

        let publication = publisher
            .publish_namespace_start(namespace.clone())
            .await
            .unwrap();
        publication.ok().await.unwrap();
        // A JoinSet owns all serving work and aborts it on test exit.
        let mut serving = tokio::task::JoinSet::new();
        let fetch_history = history.clone();
        serving.spawn(async move {
            fetch_history
                .run(publisher, namespace.clone(), "now".into())
                .await
                .unwrap();
        });
        serving.spawn(async move {
            let mut subscriptions = tokio::task::JoinSet::new();
            while let Some(request) = publication.subscribed().await.unwrap() {
                let tracks = tracks_reader.clone();
                subscriptions.spawn(Publisher::serve_subscribe(request, tracks));
            }
        });

        let (wire, _, transport) = endpoint.client.connect(&url, None).await.unwrap();
        let (session, mut subscriber) = Subscriber::connect(wire, transport).await.unwrap();
        tasks.spawn(session.run());
        let namespace = TrackNamespace::from_utf8_path("clock");

        // Standalone FETCH uses the same publisher history through the relay.
        let mut fetch = subscriber
            .fetch(
                StandaloneFetch {
                    track_namespace: namespace.clone(),
                    track_name: "now".into(),
                    start_location: Location::new(10, 0),
                    end_location: Location::new(10, 0),
                },
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        assert_eq!(fetch.ok().await.unwrap().end_location, Location::new(10, 3));
        let mut standalone = Vec::new();
        while let Some(FetchRecord::Object(object)) = fetch.next().await.unwrap() {
            standalone.push(object.location());
            while fetch.read_payload_chunk(64).await.unwrap().is_some() {}
        }
        assert_eq!(
            standalone,
            [
                Location::new(10, 0),
                Location::new(10, 1),
                Location::new(10, 2)
            ]
        );

        let (writer, reader) = Track::new(namespace, "now").produce();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let joined = clock::Subscriber::new(reader).run_joined_with_output(
            subscriber,
            writer,
            1,
            move |value| {
                tx.send(value).unwrap();
            },
        );
        tokio::pin!(joined);
        let verify = async {
            for expected in [
                "2026-10-29 10:10:58",
                "2026-10-29 10:10:59",
                "2026-10-29 10:11:00",
            ] {
                assert_eq!(rx.recv().await.unwrap(), expected);
            }
            // Write while the joined subscriber is active. Its minute base was
            // obtained by FETCH; LargestObject live delivery omits that base.
            write(&history, &mut current, 2, "01");
            write(&history, &mut current, 3, "02");
            for expected in ["2026-10-29 10:11:01", "2026-10-29 10:11:02"] {
                assert_eq!(rx.recv().await.unwrap(), expected);
            }
            current.finish().unwrap();
            let mut next = subgroups
                .create(Subgroup {
                    group_id: 12,
                    subgroup_id: 0,
                    priority: 0,
                })
                .unwrap();
            write(&history, &mut next, 0, "2026-10-29 10:12:");
            write(&history, &mut next, 1, "00");
            assert_eq!(rx.recv().await.unwrap(), "2026-10-29 10:12:00");
        };
        tokio::select! {
            result = &mut joined => panic!("joined clock ended early: {result:?}"),
            () = verify => {},
        }
    };
    tokio::select! {
        result = relay.run() => panic!("relay ended early: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(15), test) => result.expect("relay FETCH demo timed out"),
    }
}
