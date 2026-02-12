//! Low-level message sent over the wire, as defined in the specification.
//!
//! All of these messages are sent over a bidirectional QUIC stream.
//! This introduces some head-of-line blocking but preserves ordering.
//! The only exception are OBJECT "messages", which are sent over dedicated QUIC streams.
//!

mod dynamic_groups;
mod fetch;
mod fetch_cancel;
mod fetch_ok;
mod fetch_type;
mod filter_type;
mod go_away;
mod group_order;
mod max_request_id;
mod parameters;
mod publish;
mod publish_done;
mod publish_namespace;
mod publish_namespace_cancel;
mod publish_namespace_done;
mod publish_ok;
mod publisher;
mod request_error;
mod request_ok;
mod requests_blocked;
mod subscribe;
mod subscribe_namespace;
mod subscribe_ok;
mod subscribe_update;
mod subscriber;
mod track_status;
mod track_status_ok;
mod unsubscribe;

pub use dynamic_groups::*;
pub use fetch::*;
pub use fetch_cancel::*;
pub use fetch_ok::*;
pub use fetch_type::*;
pub use filter_type::*;
pub use go_away::*;
pub use group_order::*;
pub use max_request_id::*;
pub use parameters::*;
pub use publish::*;
pub use publish_done::*;
pub use publish_namespace::*;
pub use publish_namespace_cancel::*;
pub use publish_namespace_done::*;
pub use publish_ok::*;
pub use publisher::*;
pub use request_error::*;
pub use request_ok::*;
pub use requests_blocked::*;
pub use subscribe::*;
pub use subscribe_namespace::*;
pub use subscribe_ok::*;
pub use subscribe_update::*;
pub use subscriber::*;
pub use track_status::*;
pub use track_status_ok::*;
pub use unsubscribe::*;

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use std::fmt;

// Use a macro to generate the message types rather than copy-paste.
// This implements a decode/encode method that uses the specified type.
macro_rules! message_types {
    {$($name:ident = $val:expr,)*} => {
		/// All supported message types.
		#[derive(Clone)]
		pub enum Message {
			$($name($name)),*
		}

		impl Decode for Message {
			fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
				let t = u64::decode(r)?;
				let _len = u16::decode(r)?;

				// TODO: Check the length of the message.

				match t {
					$($val => {
						let msg = $name::decode(r)?;
						Ok(Self::$name(msg))
					})*
					_ => Err(DecodeError::InvalidMessage(t)),
				}
			}
		}

		impl Encode for Message {
			fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
				match self {
					$(Self::$name(ref m) => {
						self.id().encode(w)?;

						// Find out the length of the message
						// by encoding it into a buffer and then encoding the length.
						// This is a bit wasteful, but it's the only way to know the length.
                        // TODO SLG - perhaps we can store the position of the Length field in the BufMut and
                        //       write the length later, to avoid the copy of the message bytes?
						let mut buf = Vec::new();
						m.encode(&mut buf).unwrap();
                        if buf.len() > u16::MAX as usize {
                            return Err(EncodeError::MsgBoundsExceeded);
                        }
                        (buf.len() as u16).encode(w)?;

						// At least don't encode the message twice.
						// Instead, write the buffer directly to the writer.
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
					$(Self::$name(_) => {
						$val
					},)*
				}
			}

			pub fn name(&self) -> &'static str {
				match self {
					$(Self::$name(_) => {
						stringify!($name)
					},)*
				}
			}
		}

		$(impl From<$name> for Message {
			fn from(m: $name) -> Self {
				Message::$name(m)
			}
		})*

		impl fmt::Debug for Message {
			// Delegate to the message formatter
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				match self {
					$(Self::$name(ref m) => m.fmt(f),)*
				}
			}
		}
    }
}

// Each message is prefixed with the given VarInt type.
message_types! {
    // NOTE: Setup messages are in another module.
    // SetupClient = 0x20
    // SetupServer = 0x21
    // SetupClient = 0x40  // legacy, used in draft versions <= 10
    // SetupServer = 0x41  // legacy, used in draft versions <= 10

    // Misc
    GoAway = 0x10,
    MaxRequestId = 0x15,
    RequestsBlocked = 0x1a,

    // SUBSCRIBE family, sent by subscriber
    SubscribeUpdate = 0x2,
    Subscribe = 0x3,
    Unsubscribe = 0xa,
    // SUBSCRIBE family, sent by publisher
    SubscribeOk = 0x4,

    // ANNOUNCE family, sent by publisher
    PublishNamespace = 0x6,
    PublishNamespaceDone = 0x9,
    // ANNOUNCE family, sent by subscriber
    RequestOk = 0x7,
    PublishNamespaceCancel = 0xc,

    // TRACK_STATUS family, sent by subscriber
    TrackStatus = 0xd,
    // TRACK_STATUS family, sent by publisher
    TrackStatusOk = 0xe,

    // NAMESPACE family, sent by subscriber
    SubscribeNamespace = 0x11,

    // FETCH family, sent by subscriber
    Fetch = 0x16,
    FetchCancel = 0x17,
    // FETCH family, sent by publisher
    FetchOk = 0x18,

    // PUBLISH family, sent by publisher
    Publish = 0x1d,
    PublishDone = 0xb,
    // PUBLISH family, sent by subscriber
    PublishOk = 0x1e,

    RequestError = 0x5,
}
