//! Messages used for the MoQ Transport handshake.
//!
//! After establishing the WebTransport session, the client creates a bidirectional QUIC stream.
//! The client sends the [Client] message and the server responds with the [Server] message.
//! Both sides negotate the [Version] and [Role].

mod auth_token;
mod client;
mod param_types;
mod server;
mod version;

pub use auth_token::*;
pub use client::*;
pub use param_types::*;
pub use server::*;
pub use version::*;

pub const ALPN: &[u8] = b"moqt-16";
