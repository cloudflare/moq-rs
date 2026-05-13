// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Control messages sent over the bidirectional control stream.
//!
//! Wire format per draft-ietf-moq-transport-16 §9:
//!
//! ```text
//! MOQT Control Message {
//!   Message Type (i),
//!   Message Length (16),   ← 16-bit unsigned big-endian
//!   Message Payload (..),
//! }
//! ```
//!
//! The receiver MUST close the session with PROTOCOL_VIOLATION if the
//! payload length does not match Message Length.  Unknown message types
//! MUST also close the session.

mod fetch;
mod fetch_cancel;
mod fetch_error;
mod fetch_ok;
mod fetch_type;
mod filter_type;
mod go_away;
mod group_order;
mod max_request_id;
mod pubilsh_namespace_done;
mod publish;
mod publish_done;
mod publish_error;
mod publish_namespace;
mod publish_namespace_cancel;
mod publish_namespace_error;
mod publish_namespace_ok;
mod publish_ok;
mod publisher;
mod request_error;
mod request_ok;
mod request_update;
mod requests_blocked;
mod subscribe;
mod subscribe_error;
mod subscribe_namespace;
mod subscribe_namespace_error;
mod subscribe_namespace_ok;
mod subscribe_ok;
mod subscribe_update;
mod subscriber;
mod track_status;
mod track_status_error;
mod track_status_ok;
mod unsubscribe;
mod unsubscribe_namespace;

pub use fetch::*;
pub use fetch_cancel::*;
pub use fetch_error::*;
pub use fetch_ok::*;
pub use fetch_type::*;
pub use filter_type::*;
pub use go_away::*;
pub use group_order::*;
pub use max_request_id::*;
pub use pubilsh_namespace_done::*;
pub use publish::*;
pub use publish_done::*;
pub use publish_error::*;
pub use publish_namespace::*;
pub use publish_namespace_cancel::*;
pub use publish_namespace_error::*;
pub use publish_namespace_ok::*;
pub use publish_ok::*;
pub use publisher::*;
pub use request_error::*;
pub use request_ok::*;
pub use request_update::*;
pub use requests_blocked::*;
pub use subscribe::*;
pub use subscribe_error::*;
pub use subscribe_namespace::*;
pub use subscribe_namespace_error::*;
pub use subscribe_namespace_ok::*;
pub use subscribe_ok::*;
pub use subscribe_update::*;
pub use subscriber::*;
pub use track_status::*;
pub use track_status_error::*;
pub use track_status_ok::*;
pub use unsubscribe::*;
pub use unsubscribe_namespace::*;

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use bytes::Buf as _;
use std::fmt;

// Use a macro to generate the Message enum and its encode/decode impls.
macro_rules! message_types {
    {$($name:ident = $val:expr,)*} => {
        /// All supported control message types.
        #[derive(Clone)]
        pub enum Message {
            $($name($name)),*
        }

        impl Decode for Message {
            fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
                let t = u64::decode(r)?;
                let len = u16::decode(r)? as usize;

                // Enforce the length field: read exactly `len` bytes as the
                // payload and decode from that slice, so a truncated or
                // overlong payload is detected immediately.
                <u64 as Decode>::decode_remaining(r, len)?;
                let mut payload = r.copy_to_bytes(len);

                let msg = match t {
                    $($val => {
                        let msg = $name::decode(&mut payload)?;
                        Ok(Self::$name(msg))
                    })*
                    _ => Err(DecodeError::InvalidMessage(t)),
                }?;

                // Any bytes left in the payload slice mean the message was
                // shorter than declared — that is a PROTOCOL_VIOLATION.
                if payload.has_remaining() {
                    return Err(DecodeError::InvalidMessage(t));
                }

                Ok(msg)
            }
        }

        impl Encode for Message {
            fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
                match self {
                    $(Self::$name(ref m) => {
                        self.id().encode(w)?;

                        let mut buf = Vec::new();
                        m.encode(&mut buf)?;
                        if buf.len() > u16::MAX as usize {
                            return Err(EncodeError::MsgBoundsExceeded);
                        }
                        (buf.len() as u16).encode(w)?;

                        Self::encode_remaining(w, buf.len())?;
                        w.put_slice(&buf);
                        Ok(())
                    },)*
                }
            }
        }

        impl Message {
            pub fn id(&self) -> u64 {
                match self {
                    $(Self::$name(_) => $val,)*
                }
            }

            pub fn name(&self) -> &'static str {
                match self {
                    $(Self::$name(_) => stringify!($name),)*
                }
            }

            /// Return the request ID if this message participates in request ID sequencing.
            ///
            /// Responses and cancellation messages reference existing request IDs
            /// and therefore return `None`. This is used only for request ID
            /// sequencing validation on receive.
            pub fn sequenced_request_id(&self) -> Option<u64> {
                match self {
                    Self::Subscribe(m) => Some(m.id),
                    Self::RequestUpdate(m) => Some(m.id),
                    Self::Fetch(m) => Some(m.id),
                    Self::TrackStatus(m) => Some(m.id),
                    Self::SubscribeNamespace(m) => Some(m.id),
                    Self::Publish(m) => Some(m.id),
                    Self::PublishNamespace(m) => Some(m.id),
                    // Legacy pre-draft-16 request update stub.
                    Self::SubscribeUpdate(m) => Some(m.id),
                    _ => None,
                }
            }
        }

        $(impl From<$name> for Message {
            fn from(m: $name) -> Self {
                Message::$name(m)
            }
        })*

        impl fmt::Debug for Message {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(Self::$name(ref m) => m.fmt(f),)*
                }
            }
        }
    }
}

// Wire IDs per draft-ietf-moq-transport-16 Table 1.
//
// Messages not in draft-16 (old per-request OK/ERROR types) are kept as
// internal types for session dispatch but their IDs were reassigned by the
// spec.  The stale message structs (SubscribeError, PublishNamespaceOk/Error,
// TrackStatusOk/Error, FetchError, SubscribeNamespaceOk/Error,
// UnsubscribeNamespace, PublishError) are retained as Rust types for now
// so existing session dispatch code compiles; they are wired up in the
// publisher/subscriber enums and will be replaced by itzmanish.
//
// Stub IDs below (0x100+ range) are NOT sent on the wire; they are only
// used internally so the macro-generated enum keeps compiling.  The decode
// path will never produce them; the encode path is guarded by the session
// layer which only sends draft-16 messages.
message_types! {
    // NOTE: Setup messages live in a separate module (setup::Client/Server).

    // ── Shared request responses (new in draft-16) ───────────────────────────
    RequestUpdate   = 0x2,
    RequestError    = 0x5,   // draft-16: REQUEST_ERROR
    RequestOk       = 0x7,   // draft-16: REQUEST_OK

    // ── SUBSCRIBE family ─────────────────────────────────────────────────────
    Subscribe       = 0x3,
    SubscribeOk     = 0x4,
    Unsubscribe     = 0xa,

    // ── PUBLISH_NAMESPACE family ──────────────────────────────────────────────
    PublishNamespace        = 0x6,
    PublishNamespaceDone    = 0x9,
    PublishNamespaceCancel  = 0xc,

    // ── TRACK_STATUS ──────────────────────────────────────────────────────────
    TrackStatus     = 0xd,

    // ── PUBLISH family ────────────────────────────────────────────────────────
    Publish         = 0x1d,
    PublishDone     = 0xb,
    PublishOk       = 0x1e,

    // ── FETCH family ─────────────────────────────────────────────────────────
    Fetch           = 0x16,
    FetchCancel     = 0x17,
    FetchOk         = 0x18,

    // ── SUBSCRIBE_NAMESPACE (bidi stream; §9.25) ──────────────────────────────
    SubscribeNamespace = 0x11,

    // ── Session management ────────────────────────────────────────────────────
    GoAway          = 0x10,
    MaxRequestId    = 0x15,
    RequestsBlocked = 0x1a,

    // ── Legacy/stub types (internal only; NOT sent on the wire) ─────────────
    // These retain Rust types for the session dispatch layer while the
    // per-message OK/ERROR flow is consolidated (TODO itzmanish).
    SubscribeError          = 0x100,
    PublishNamespaceOk      = 0x101,
    PublishNamespaceError   = 0x102,
    TrackStatusOk           = 0x103,
    TrackStatusError        = 0x104,
    FetchError              = 0x105,
    SubscribeNamespaceOk    = 0x106,
    SubscribeNamespaceError = 0x107,
    UnsubscribeNamespace    = 0x108,
    PublishError            = 0x109,
    SubscribeUpdate         = 0x10a,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{KeyValuePairs, Location, ReasonPhrase, TrackNamespace};

    fn namespace() -> TrackNamespace {
        TrackNamespace::from_utf8_path("test/ns")
    }

    fn assert_sequenced(msg: Message, id: u64) {
        assert_eq!(msg.sequenced_request_id(), Some(id));
    }

    fn assert_not_sequenced(msg: Message) {
        assert_eq!(msg.sequenced_request_id(), None);
    }

    #[test]
    fn sequenced_request_id_covers_all_request_start_messages() {
        assert_sequenced(
            Message::Subscribe(Subscribe {
                id: 0,
                track_namespace: namespace(),
                track_name: "track".to_string(),
                subscriber_priority: 127,
                group_order: GroupOrder::Publisher,
                forward: true,
                filter_type: FilterType::LargestObject,
                start_location: None,
                end_group_id: None,
                params: KeyValuePairs::default(),
            }),
            0,
        );

        assert_sequenced(
            Message::RequestUpdate(RequestUpdate {
                id: 2,
                existing_request_id: 0,
                params: KeyValuePairs::default(),
            }),
            2,
        );

        assert_sequenced(
            Message::Fetch(Fetch {
                id: 4,
                subscriber_priority: 127,
                group_order: GroupOrder::Ascending,
                fetch_type: FetchType::Standalone,
                standalone_fetch: Some(StandaloneFetch {
                    track_namespace: namespace(),
                    track_name: "track".to_string(),
                    start_location: Location::new(0, 0),
                    end_location: Location::new(0, 1),
                }),
                joining_fetch: None,
                params: KeyValuePairs::default(),
            }),
            4,
        );

        assert_sequenced(
            Message::TrackStatus(TrackStatus {
                id: 6,
                track_namespace: namespace(),
                track_name: "track".to_string(),
                subscriber_priority: 127,
                group_order: GroupOrder::Publisher,
                forward: true,
                filter_type: FilterType::LargestObject,
                start_location: None,
                end_group_id: None,
                params: KeyValuePairs::default(),
            }),
            6,
        );

        assert_sequenced(
            Message::SubscribeNamespace(SubscribeNamespace {
                id: 8,
                track_namespace_prefix: namespace(),
                params: KeyValuePairs::default(),
            }),
            8,
        );

        assert_sequenced(
            Message::Publish(Publish {
                id: 10,
                track_namespace: namespace(),
                track_name: "track".to_string(),
                track_alias: 1,
                group_order: GroupOrder::Ascending,
                content_exists: false,
                largest_location: None,
                forward: true,
                params: KeyValuePairs::default(),
            }),
            10,
        );

        assert_sequenced(
            Message::PublishNamespace(PublishNamespace {
                id: 12,
                track_namespace: namespace(),
                params: KeyValuePairs::default(),
            }),
            12,
        );
    }

    #[test]
    fn sequenced_request_id_ignores_messages_that_reference_existing_requests() {
        assert_not_sequenced(Message::RequestOk(RequestOk {
            id: 0,
            params: KeyValuePairs::default(),
        }));

        assert_not_sequenced(Message::RequestError(RequestError {
            id: 0,
            error_code: 0,
            retry_interval: 0,
            reason: ReasonPhrase(String::new()),
        }));

        assert_not_sequenced(Message::SubscribeOk(SubscribeOk {
            id: 0,
            track_alias: 1,
            expires: 0,
            group_order: GroupOrder::Publisher,
            content_exists: false,
            largest_location: None,
            params: KeyValuePairs::default(),
        }));

        assert_not_sequenced(Message::Unsubscribe(Unsubscribe { id: 0 }));

        assert_not_sequenced(Message::FetchCancel(FetchCancel { id: 0 }));

        assert_not_sequenced(Message::FetchOk(FetchOk {
            id: 0,
            group_order: GroupOrder::Ascending,
            end_of_track: false,
            end_location: Location::new(0, 0),
            params: KeyValuePairs::default(),
        }));

        assert_not_sequenced(Message::PublishOk(PublishOk {
            id: 0,
            forward: true,
            filter_type: FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            params: KeyValuePairs::default(),
        }));

        assert_not_sequenced(Message::PublishDone(PublishDone {
            id: 0,
            status_code: 0,
            stream_count: 0,
            reason: ReasonPhrase(String::new()),
        }));
    }
}
