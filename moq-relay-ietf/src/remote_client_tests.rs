// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use moq_native_ietf::client::{ConnectFuture, Connection, Connector};
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

struct Route(Option<Client>);

#[async_trait::async_trait]
impl Coordinator for Route {
    async fn register_namespace(
        &self,
        _: Option<&str>,
        _: &TrackNamespace,
        _: &crate::CoordinatorContext,
    ) -> crate::CoordinatorResult<crate::NamespaceRegistration> {
        Ok(crate::NamespaceRegistration::new(()))
    }
    async fn unregister_namespace(
        &self,
        _: Option<&str>,
        _: &TrackNamespace,
    ) -> crate::CoordinatorResult<()> {
        Ok(())
    }
    async fn lookup(
        &self,
        _: Option<&str>,
        ns: &TrackNamespace,
    ) -> crate::CoordinatorResult<(crate::NamespaceOrigin, Option<Client>)> {
        Ok((
            crate::NamespaceOrigin::new(
                ns.clone(),
                Url::parse("moqt://localhost/tenant").unwrap(),
                Some("127.0.0.1:4433".parse().unwrap()),
            ),
            self.0.clone(),
        ))
    }
}

struct Refuse(Arc<AtomicUsize>);
impl Connector for Refuse {
    fn connect<'a>(&'a self, _: &'a Url, _: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "selected connector refused",
            )
            .into())
        })
    }
}

#[tokio::test]
async fn opaque_default_and_lookup_override_do_not_fall_back_on_error() {
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let override_calls = Arc::new(AtomicUsize::new(0));
    for use_override in [false, true] {
        let override_client = use_override.then(|| Client::new(Refuse(override_calls.clone())));
        let manager = RemoteManager::new_with_clients(
            Arc::new(Route(override_client)),
            vec![
                Client::new(Refuse(first.clone())),
                Client::new(Refuse(second.clone())),
            ],
            SessionConfig::default(),
        );
        let request = StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path("test/route"),
            track_name: "video".into(),
            start_location: moq_transport::coding::Location::new(0, 0),
            end_location: moq_transport::coding::Location::new(1, 0),
        };
        let error = manager
            .fetch(Some("tenant"), request, KeyValuePairs::default())
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert!(manager.remotes.lock().await.is_empty());
    }
    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 0);
    assert_eq!(override_calls.load(Ordering::SeqCst), 1);
}

// A selectable connector that consumes an already-established abstract carrier;
// it contains no quic::Client and need not construct a native endpoint.
struct SuppliedTransport(std::sync::Mutex<Option<Connection>>);
impl Connector for SuppliedTransport {
    fn connect<'a>(&'a self, url: &'a Url, _: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            let connection = self
                .0
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| anyhow::anyhow!("carrier already consumed"))?;
            assert_eq!(connection.session.url(), url);
            Ok(connection)
        })
    }
}

#[tokio::test]
async fn supplied_transport_reaches_setup_and_legacy_pool_hit_precedes_policy() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = crate::test::test_endpoint();
        let native = Client::from(endpoint.client);
        let url = Url::parse("https://localhost/tenant?preserved=yes").unwrap();
        let key = (url.clone(), Some(endpoint.addr));
        let mut server = endpoint.server;
        let (connection, accepted) = tokio::join!(native.connect(&url, key.1), server.accept());
        let connection = connection.unwrap();
        let (carrier, info) = accepted.unwrap();
        assert_eq!(
            connection.initial_destination_cid.as_deref(),
            Some(info.id.as_str())
        );
        let selected = Client::new(SuppliedTransport(std::sync::Mutex::new(Some(connection))));
        let manager = RemoteManager::new_with_clients(
            Arc::new(Route(None)),
            vec![selected],
            SessionConfig::default(),
        );
        let (remote, peer) = tokio::join!(
            manager.get_or_connect(key.clone(), None),
            moq_transport::session::Session::accept(carrier, None, info.transport)
        );
        let remote = remote.unwrap();
        let (peer_session, _, _) = peer.unwrap();
        assert_eq!(peer_session.connection_path(), Some("/tenant"));
        let refused = Arc::new(AtomicUsize::new(0));
        let other_policy = Client::new(Refuse(refused.clone()));
        let cached = manager
            .get_or_connect(key.clone(), Some(&other_policy))
            .await
            .unwrap();
        assert!(remote.is_same_connection(&cached));
        assert_eq!(
            refused.load(Ordering::SeqCst),
            0,
            "legacy live hit ignores a later policy"
        );
        let different_key = (Url::parse("https://localhost/different").unwrap(), key.1);
        assert!(manager
            .get_or_connect(different_key, Some(&other_policy))
            .await
            .is_err());
        assert_eq!(refused.load(Ordering::SeqCst), 1);
        assert_eq!(manager.remotes.lock().await.len(), 1);
        manager.shutdown().await;
        assert!(!remote.is_connected());
        assert!(manager.remotes.lock().await.is_empty());
        drop(peer_session);
    })
    .await
    .expect("supplied carrier/pool test timed out");
}

struct Pending {
    entered: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl Connector for Pending {
    fn connect<'a>(&'a self, _: &'a Url, _: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            let _guard = Guard(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn opaque_cancelled_dial_releases_operation_and_empty_pool_slot() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let manager = RemoteManager::new_with_clients(
            Arc::new(Route(None)),
            vec![Client::new(Pending {
                entered: entered.clone(),
                dropped: dropped.clone(),
            })],
            SessionConfig::default(),
        );
        let key = (Url::parse("moqt://example.invalid/tenant").unwrap(), None);
        {
            let attempt = manager.get_or_connect(key, None);
            tokio::pin!(attempt);
            tokio::select! {
                _ = &mut attempt => panic!("pending connector returned"),
                _ = entered.notified() => {}
            }
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        while !manager.remotes.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
        manager.shutdown().await;
    })
    .await
    .expect("cancel cleanup timed out");
}
