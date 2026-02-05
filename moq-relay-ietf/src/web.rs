use std::{net, path::PathBuf, sync::Arc};

use axum::{
    extract::{Path, State},
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use hyper_serve::tls_rustls::RustlsAcceptor;
use tower_http::cors::{Any, CorsLayer};

pub struct WebConfig {
    pub bind: net::SocketAddr,
    pub tls: moq_native_ietf::tls::Config,
    pub qlog_dir: Option<PathBuf>,
    pub mlog_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct WebState {
    fingerprint: String,
    qlog_dir: Option<Arc<PathBuf>>,
    mlog_dir: Option<Arc<PathBuf>>,
}

// Run a HTTP server using Axum
// TODO remove this when Chrome adds support for self-signed certificates using WebTransport
pub struct Web {
    app: Router,
    server: hyper_serve::Server<RustlsAcceptor>,
}

impl Web {
    pub fn new(config: WebConfig) -> Self {
        // Get the first certificate's fingerprint.
        // TODO serve all of them so we can support multiple signature algorithms.
        let fingerprint = config
            .tls
            .fingerprints
            .first()
            .expect("missing certificate")
            .clone();

        let mut tls = config.tls.server.expect("missing server configuration");
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls = hyper_serve::tls_rustls::RustlsConfig::from_config(Arc::new(tls));

        // Create shared state
        let state = WebState {
            fingerprint,
            qlog_dir: config.qlog_dir.map(Arc::new),
            mlog_dir: config.mlog_dir.map(Arc::new),
        };

        // Build router with fingerprint endpoint
        let mut app = Router::new().route("/fingerprint", get(serve_fingerprint));

        // Optionally add qlog serving endpoint
        if state.qlog_dir.is_some() {
            app = app.route("/qlog/:cid", get(serve_qlog));
            tracing::info!("qlog files available at /qlog/:cid");
        }

        // Optionally add mlog serving endpoint
        if state.mlog_dir.is_some() {
            app = app.route("/mlog/:cid", get(serve_mlog));
            tracing::info!("mlog files available at /mlog/:cid");
        }

        // Add state and CORS layer
        let app = app.with_state(state).layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET]),
        );

        let server = hyper_serve::bind_rustls(config.bind, tls);

        Self { app, server }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.server.serve(self.app.into_make_service()).await?;
        Ok(())
    }
}

async fn serve_fingerprint(State(state): State<WebState>) -> impl IntoResponse {
    state.fingerprint
}

async fn serve_qlog(
    Path(cid): Path<String>,
    State(state): State<WebState>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    // Get qlog directory or return 404
    let qlog_dir = state.qlog_dir.as_ref().ok_or((
        StatusCode::NOT_FOUND,
        "Qlog serving not enabled".to_string(),
    ))?;

    // Strip _server.qlog suffix if present to get the base CID
    let base_cid = cid.strip_suffix("_server.qlog").unwrap_or(&cid);

    // Construct the expected filename
    let filename = format!("{}_server.qlog", base_cid);
    let file_path = qlog_dir.join(&filename);

    // Security: Ensure the path is still within qlog_dir (prevent path traversal)
    let canonical_dir = qlog_dir.canonicalize().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid qlog directory: {}", e),
        )
    })?;

    let canonical_file = file_path.canonicalize().map_err(|_| {
        (
            StatusCode::NOT_FOUND,
            format!("Qlog file not found: {}", filename),
        )
    })?;

    if !canonical_file.starts_with(&canonical_dir) {
        return Err((StatusCode::FORBIDDEN, "Invalid path".to_string()));
    }

    // Read and return the file
    tokio::fs::read(&canonical_file).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            format!("Failed to read qlog file: {}", e),
        )
    })
}

async fn serve_mlog(
    Path(cid): Path<String>,
    State(state): State<WebState>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    // Get mlog directory or return 404
    let mlog_dir = state.mlog_dir.as_ref().ok_or((
        StatusCode::NOT_FOUND,
        "Mlog serving not enabled".to_string(),
    ))?;

    // Strip _server.mlog suffix if present to get the base CID
    let base_cid = cid.strip_suffix("_server.mlog").unwrap_or(&cid);

    // Construct the expected filename
    let filename = format!("{}_server.mlog", base_cid);
    let file_path = mlog_dir.join(&filename);

    // Security: Ensure the path is still within mlog_dir (prevent path traversal)
    let canonical_dir = mlog_dir.canonicalize().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid mlog directory: {}", e),
        )
    })?;

    let canonical_file = file_path.canonicalize().map_err(|_| {
        (
            StatusCode::NOT_FOUND,
            format!("Mlog file not found: {}", filename),
        )
    })?;

    if !canonical_file.starts_with(&canonical_dir) {
        return Err((StatusCode::FORBIDDEN, "Invalid path".to_string()));
    }

    // Read and return the file
    tokio::fs::read(&canonical_file).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            format!("Failed to read mlog file: {}", e),
        )
    })
}
