use crate::{coding, serve, setup};

#[derive(thiserror::Error, Debug, Clone)]
pub enum SessionError {
    #[error("webtransport session: {0}")]
    Session(#[from] web_transport::SessionError),

    #[error("webtransport write: {0}")]
    Write(#[from] web_transport::WriteError),

    #[error("webtransport read: {0}")]
    Read(#[from] web_transport::ReadError),

    #[error("encode error: {0}")]
    Encode(#[from] coding::EncodeError),

    #[error("decode error: {0}")]
    Decode(#[from] coding::DecodeError),

    // TODO move to a ConnectError
    #[error("unsupported versions: client={0:?} server={1:?}")]
    Version(setup::Versions, setup::Versions),

    /// TODO SLG - eventually remove or morph into error for incorrect control message for publisher/subscriber
    /// The role negiotiated in the handshake was violated. For example, a publisher sent a SUBSCRIBE, or a subscriber sent an OBJECT.
    #[error("role violation")]
    RoleViolation,

    /// Some VarInt was too large and we were too lazy to handle it
    #[error("varint bounds exceeded")]
    BoundsExceeded(#[from] coding::BoundsExceeded),

    /// A duplicate ID was used
    #[error("duplicate")]
    Duplicate,

    #[error("internal error")]
    Internal,

    #[error("serve error: {0}")]
    Serve(#[from] serve::ServeError),

    #[error("wrong size")]
    WrongSize,
}

// Session Termination Error Codes from draft-ietf-moq-transport-14 Section 13.1.1
impl SessionError {
    /// An integer code that is sent over the wire.
    /// Returns Session Termination Error Codes per draft-14.
    pub fn code(&self) -> u64 {
        match self {
            // PROTOCOL_VIOLATION (0x3) - The role negotiated in the handshake was violated
            Self::RoleViolation => 0x3,
            // INTERNAL_ERROR (0x1) - Generic internal errors
            Self::Session(_) => 0x1,
            Self::Read(_) => 0x1,
            Self::Write(_) => 0x1,
            Self::Encode(_) => 0x1,
            Self::BoundsExceeded(_) => 0x1,
            Self::Internal => 0x1,
            // VERSION_NEGOTIATION_FAILED (0x15)
            Self::Version(..) => 0x15,
            // PROTOCOL_VIOLATION (0x3) - Malformed messages
            Self::Decode(_) => 0x3,
            Self::WrongSize => 0x3,
            // DUPLICATE_TRACK_ALIAS (0x5)
            Self::Duplicate => 0x5,
            // Delegate to ServeError for per-request error codes
            Self::Serve(err) => err.code(),
        }
    }

    /// Helper for unimplemented protocol features
    /// Logs a warning and returns a NotImplemented error instead of panicking
    pub fn unimplemented(feature: &str) -> Self {
        Self::Serve(serve::ServeError::not_implemented_ctx(feature))
    }

    /// Returns true if this error represents a graceful connection close.
    ///
    /// A graceful close occurs when the peer sends APPLICATION_CLOSE with error code 0
    /// (NO_ERROR). This is normal session termination, not an error condition.
    ///
    /// This method checks for:
    /// - WebTransport close with code 0 (HTTP/3 encoded as 0x52e4a40fa8db)
    /// - Raw QUIC `ApplicationClosed` with code 0
    /// - The local side closing the connection (`LocallyClosed`)
    ///
    /// ## Implementation Notes
    ///
    /// We pattern match on `web_transport_quinn::SessionError` variants to access the
    /// underlying `quinn::ConnectionError`. For WebTransport connections, the close code
    /// is encoded using HTTP/3 error code space, which we decode using
    /// `web_transport_proto::error_from_http3()`.
    ///
    /// **Coupling note**: This implementation is coupled to `web-transport-quinn` and
    /// `quinn`. When transitioning to a different WebTransport backend (e.g., tokio-quiche),
    /// ensure the replacement provides equivalent error introspection, or update this
    /// method to handle the new error types.
    pub fn is_graceful_close(&self) -> bool {
        match self {
            Self::Session(session_err) => is_session_error_graceful(session_err),
            Self::Read(read_err) => {
                // ReadError::SessionError wraps SessionError
                if let web_transport::ReadError::SessionError(session_err) = read_err {
                    return is_session_error_graceful(session_err);
                }
                false
            }
            Self::Write(write_err) => {
                // WriteError::SessionError wraps SessionError
                if let web_transport::WriteError::SessionError(session_err) = write_err {
                    return is_session_error_graceful(session_err);
                }
                false
            }
            _ => false,
        }
    }
}

impl From<SessionError> for serve::ServeError {
    fn from(err: SessionError) -> Self {
        match err {
            SessionError::Serve(err) => err,
            _ => serve::ServeError::internal_ctx(format!("session error: {}", err)),
        }
    }
}

/// Helper to check if a `web_transport::SessionError` represents a graceful close.
///
/// This handles both:
/// - Raw QUIC connections: `ApplicationClosed` with code 0
/// - WebTransport connections: `ApplicationClosed` with HTTP/3 encoded code that decodes to 0
fn is_session_error_graceful(err: &web_transport::SessionError) -> bool {
    use web_transport_quinn::SessionError;

    match err {
        SessionError::ConnectionError(conn_err) => is_connection_error_graceful(conn_err),
        // WebTransportError doesn't represent connection close in 0.3.x
        SessionError::WebTransportError(_) => false,
        // SendDatagramError doesn't represent connection close
        SessionError::SendDatagramError(_) => false,
    }
}

/// Helper to check if a `quinn::ConnectionError` represents a graceful close.
fn is_connection_error_graceful(err: &quinn::ConnectionError) -> bool {
    use quinn::ConnectionError;

    match err {
        ConnectionError::ApplicationClosed(close) => {
            let code = close.error_code.into_inner();

            // Check for raw QUIC code 0 (direct MoQ-over-QUIC)
            if code == 0 {
                return true;
            }

            // Check for WebTransport code 0 (HTTP/3 encoded)
            // WebTransport code 0 maps to HTTP/3 code 0x52e4a40fa8db
            if let Some(wt_code) = web_transport_proto::error_from_http3(code) {
                return wt_code == 0;
            }

            false
        }
        // LocallyClosed means we closed the connection ourselves
        ConnectionError::LocallyClosed => true,
        // Other errors are not graceful closes
        _ => false,
    }
}
