// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::SocketAddr;
use std::sync::Arc;

use moq_native_ietf::quic;
use url::Url;

/// Capture events on each poll, including after an async task yields.
pub(crate) async fn capture_logs<T>(future: impl std::future::Future<Output = T>) -> (T, String) {
    use std::io::Write;
    use std::sync::Mutex;
    use tracing::instrument::WithSubscriber;

    #[derive(Clone)]
    struct Output(Arc<Mutex<Vec<u8>>>);

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let bytes = Arc::new(Mutex::new(Vec::new()));
    let output = Output(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || output.clone())
        .finish();
    let result = future.with_subscriber(subscriber).await;
    let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    (result, text)
}

pub(crate) struct TestEndpoint {
    pub client: quic::Client,
    pub server: quic::Server,
    pub url: Url,
    pub addr: SocketAddr,
}

pub(crate) fn test_endpoint() -> TestEndpoint {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let certificate = certified.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certified.key_pair.serialize_der(),
    ));
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let client_tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls = moq_native_ietf::tls::Config {
        client: client_tls,
        server: Some(server_tls),
        fingerprints: Vec::new(),
    };
    let endpoint =
        quic::Endpoint::new(quic::Config::new("0.0.0.0:0".parse().unwrap(), None, tls).unwrap())
            .unwrap();
    let client = endpoint.client;
    let server = endpoint.server.unwrap();
    let addr = SocketAddr::from(([127, 0, 0, 1], server.local_addr().unwrap().port()));

    TestEndpoint {
        client,
        server,
        url: Url::parse("moqt://localhost/").unwrap(),
        addr,
    }
}
