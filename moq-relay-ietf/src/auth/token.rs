// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AUTHORIZATION TOKEN wire-format types and SETUP parsing.
//!
//! The canonical implementation lives in `moq-transport::setup::auth_token`
//! so that fuzz harnesses can exercise the parser without depending on the
//! relay's full dependency tree (TLS, QUIC, HTTP server).
//!
//! This module re-exports those types for use within `moq-relay-ietf`.
//!
//! # Wire format (draft-18 §10.3.1.4)
//!
//! ```text
//! AUTHORIZATION TOKEN Option {
//!   Alias Type (vi),     ; USE_VALUE = 1 (draft-18; was 0x3 in draft-16)
//!   Token Type (vi),     ; e.g. 0x01 for CAT
//!   Token Value (..),    ; remainder of the parameter value
//! }
//! ```

pub use moq_transport::setup::{
    decode_setup_tokens, AliasType, AuthToken, SetupTokens, CAT_TOKEN_TYPE, MAX_SETUP_TOKENS,
};
