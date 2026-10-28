// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

// External-style extension: no Quinn client, endpoint or TLS config is constructed.
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use moq_native_ietf::{
    client::{ConnectFuture, Connector},
    Client,
};
use tokio::sync::Notify;
use url::Url;

type Calls = Arc<Mutex<Vec<(Url, Option<SocketAddr>)>>>;
struct RefusingConnector(Calls);

impl Connector for RefusingConnector {
    fn connect<'a>(&'a self, url: &'a Url, addr: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            self.0.lock().unwrap().push((url.clone(), addr));
            Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "custom policy").into())
        })
    }
}

#[tokio::test]
async fn external_connector_is_selected_preserves_inputs_and_error_source() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(RefusingConnector(calls.clone()));
    fn send_sync<T: Send + Sync>(_: &T) {}
    send_sync(&client);
    let url =
        Url::parse("https://user:private@example.invalid:4443/scope?credential=private").unwrap();
    let addr = Some("127.0.0.1:9999".parse().unwrap());
    let clone = client.clone();
    let attempt = clone.connect(&url, addr);
    fn send<T: Send>(_: &T) {}
    send(&attempt);
    let error = attempt
        .await
        .err()
        .expect("custom refusal must not become a native dial");
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(*calls.lock().unwrap(), vec![(url.clone(), addr)]);
    assert!(!format!("{client:?}").contains("private"));
}

struct PendingConnector {
    entered: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

struct AttemptGuard(Arc<AtomicUsize>);
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Connector for PendingConnector {
    fn connect<'a>(&'a self, _: &'a Url, _: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            let _guard = AttemptGuard(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn cancelling_external_connector_drops_its_borrowed_operation() {
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let client = Client::new(PendingConnector {
        entered: entered.clone(),
        dropped: dropped.clone(),
    });
    let url = Url::parse("moqt://example.invalid/").unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let attempt = client.connect(&url, None);
        tokio::pin!(attempt);
        tokio::select! {
            _ = &mut attempt => panic!("pending connector completed"),
            _ = entered.notified() => {}
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
