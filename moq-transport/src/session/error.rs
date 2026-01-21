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

    #[error("protocol violation")]
    ProtocolViolation,
}

// Session Termination Error Codes from draft-ietf-moq-transport-14 Section 13.1.1
impl SessionError {
    /// An integer code that is sent over the wire.
    /// Returns Session Termination Error Codes per draft-14.
    pub fn code(&self) -> u64 {
        match self {
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
            Self::ProtocolViolation => 0x3,
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
}

impl From<SessionError> for serve::ServeError {
    fn from(err: SessionError) -> Self {
        match err {
            SessionError::Serve(err) => err,
            _ => serve::ServeError::internal_ctx(format!("session error: {}", err)),
        }
    }
}
