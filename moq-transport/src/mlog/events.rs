// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

// TODO: Unimplemented control message events (not yet needed for basic relay interop testing):
// - SubscribeUpdate (parsed/created)
// - PublishNamespaceDone (parsed/created)
// - PublishNamespaceCancel (parsed/created)
// - TrackStatus, TrackStatusOk, TrackStatusError (parsed/created)
// - SubscribeNamespace, SubscribeNamespaceOk, SubscribeNamespaceError, UnsubscribeNamespace (parsed/created)
// - Fetch, FetchOk, FetchError, FetchCancel (parsed/created)
// - Publish, PublishOk, PublishError, PublishDone (parsed/created)
// - MaxRequestId (parsed/created)
// - RequestsBlocked (parsed/created)
//
// TODO: Unimplemented data plane events (from draft-pardue-moq-qlog-moq-events):
// - stream_type_set (when stream type becomes known)
// - object_datagram_status_created/parsed
// - fetch_header_created/parsed
// - fetch_object_created/parsed
//
// TODO: stream_id field currently uses placeholder value (0)
// - Need to plumb actual QUIC stream IDs through web_transport abstractions
// - This would enable correlation between QUIC qlog and MoQ mlog events

use serde::Serialize;
use serde_json::{json, Value as JsonValue};

use crate::coding::Encode;
use crate::{coding, data, message, setup};

/// Hex-encode a byte slice (lowercase).
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// MoQ Transport event following qlog patterns
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    /// Time in milliseconds since Unix epoch (1970-01-01T00:00:00.000Z)
    pub time: f64,

    /// Event name in format "moqt:event_name"
    pub name: String,

    /// Event-specific data
    pub data: EventData,
}

/// Union of all MoQ Transport event types
///
/// Uses serde untagged representation — the event type is conveyed by
/// the `name` field on the parent Event struct, not repeated in data.
/// (Per qlog spec, the "name" field is the event type identifier.)
///
/// Note: `Deserialize` is not derived because structurally identical
/// variant pairs (e.g. `ControlMessageParsed` / `ControlMessageCreated`)
/// are ambiguous under untagged deserialization. This enum is
/// serialization-only. Consumers must use `Event.name` to determine
/// the event type.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum EventData {
    ControlMessageParsed(ControlMessageParsed),
    ControlMessageCreated(ControlMessageCreated),
    SubgroupHeaderParsed(SubgroupHeaderParsed),
    SubgroupHeaderCreated(SubgroupHeaderCreated),
    SubgroupObjectParsed(SubgroupObjectParsed),
    SubgroupObjectCreated(SubgroupObjectCreated),
    ObjectDatagramParsed(ObjectDatagramParsed),
    ObjectDatagramCreated(ObjectDatagramCreated),
    LogLevel(LogLevelEvent),
}

/// Control message parsed event
/// Per draft-pardue-moq-qlog-moq-events-03 Section 4.2
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct ControlMessageParsed {
    pub stream_id: u64,

    /// Nested control message object (contains "type" discriminator)
    pub message: JsonValue,
}

/// Control message created event
/// Per draft-pardue-moq-qlog-moq-events-03 Section 4.1
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct ControlMessageCreated {
    pub stream_id: u64,

    /// Nested control message object (contains "type" discriminator)
    pub message: JsonValue,
}

/// Subgroup header parsed event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct SubgroupHeaderParsed {
    pub stream_id: u64,

    /// Header-specific fields
    #[serde(flatten)]
    pub header: JsonValue,
}

/// Subgroup header created event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct SubgroupHeaderCreated {
    pub stream_id: u64,

    /// Header-specific fields
    #[serde(flatten)]
    pub header: JsonValue,
}

/// Subgroup object parsed event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct SubgroupObjectParsed {
    pub stream_id: u64,

    /// Object-specific fields
    #[serde(flatten)]
    pub object: JsonValue,
}

/// Subgroup object created event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct SubgroupObjectCreated {
    pub stream_id: u64,

    /// Object-specific fields
    #[serde(flatten)]
    pub object: JsonValue,
}

/// Object Datagram parsed event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct ObjectDatagramParsed {
    pub stream_id: u64,

    /// Object-specific fields
    #[serde(flatten)]
    pub object: JsonValue,
}

/// Object Datagram created event (data plane)
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct ObjectDatagramCreated {
    pub stream_id: u64,

    /// Object-specific fields
    #[serde(flatten)]
    pub object: JsonValue,
}

/// LogLevel event for flexible logging (qlog loglevel schema)
/// See: https://www.ietf.org/archive/id/draft-ietf-quic-qlog-main-schema-12.html#name-loglevel-events
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct LogLevelEvent {
    pub message: String,
}

/// Convert a KeyValuePair to a typed MOQTSetupParameter JSON object.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.2
fn setup_param_to_qlog(kvp: &coding::KeyValuePair) -> JsonValue {
    match (kvp.key, &kvp.value) {
        // PATH (0x01)
        (0x01, coding::Value::BytesValue(bytes)) => json!({
            "name": "path",
            "value": String::from_utf8_lossy(bytes),
        }),
        // MAX_REQUEST_ID (0x02)
        (0x02, coding::Value::IntValue(v)) => json!({
            "name": "max_request_id",
            "value": v,
        }),
        (key, coding::Value::IntValue(v)) => json!({
            "name": "unknown",
            "name_bytes": key,
            "value": v,
        }),
        (key, coding::Value::BytesValue(bytes)) => json!({
            "name": "unknown",
            "name_bytes": key,
            "length": bytes.len(),
            "value_bytes": hex_encode(bytes),
        }),
    }
}

/// Convert setup parameters to qlog [*MOQTSetupParameter] array.
fn setup_params_to_qlog(kvps: &[coding::KeyValuePair]) -> JsonValue {
    json!(kvps.iter().map(setup_param_to_qlog).collect::<Vec<_>>())
}

/// Convert a KeyValuePair to a typed MOQTParameter JSON object.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.3
fn param_to_qlog(kvp: &coding::KeyValuePair) -> JsonValue {
    match (kvp.key, &kvp.value) {
        // AUTHORIZATION_TOKEN (0x02) — redact value, log only presence and length
        (0x02, coding::Value::BytesValue(bytes)) => json!({
            "name": "authorization_token",
            "alias_type": kvp.key,
            "token_length": bytes.len(),
        }),
        // DELIVERY_TIMEOUT (0x03)
        (0x03, coding::Value::IntValue(v)) => json!({
            "name": "delivery_timeout",
            "value": v,
        }),
        // MAX_CACHE_DURATION (0x04)
        (0x04, coding::Value::IntValue(v)) => json!({
            "name": "max_cache_duration",
            "value": v,
        }),
        (key, coding::Value::IntValue(v)) => json!({
            "name": "unknown",
            "name_bytes": key,
            "value": v,
        }),
        (key, coding::Value::BytesValue(bytes)) => json!({
            "name": "unknown",
            "name_bytes": key,
            "length": bytes.len(),
            "value_bytes": hex_encode(bytes),
        }),
    }
}

/// Convert parameters to qlog [*MOQTParameter] array.
fn params_to_qlog(kvps: &[coding::KeyValuePair]) -> JsonValue {
    json!(kvps.iter().map(param_to_qlog).collect::<Vec<_>>())
}

/// Compute the wire byte length of extension headers by encoding to a temp buffer.
/// This matches the MoQT framing field that precedes extension headers on the wire.
fn ext_headers_wire_len(headers: &data::ExtensionHeaders) -> u64 {
    let mut tmp = bytes::BytesMut::new();
    for kvp in &headers.0 {
        let _ = kvp.encode(&mut tmp);
    }
    tmp.len() as u64
}

/// Convert extension headers to qlog [*MOQTExtensionHeader] array.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.7
fn extension_headers_to_qlog(kvps: &[coding::KeyValuePair]) -> JsonValue {
    json!(kvps
        .iter()
        .map(|kvp| match &kvp.value {
            coding::Value::IntValue(v) => json!({
                "header_type": kvp.key,
                "header_value": v,
            }),
            coding::Value::BytesValue(bytes) => json!({
                "header_type": kvp.key,
                "header_length": bytes.len(),
                "payload": hex_encode(bytes),
            }),
        })
        .collect::<Vec<_>>())
}

/// Convert a TrackNamespace to the qlog [*MOQTByteString] format.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.4
fn namespace_to_qlog(ns: &coding::TrackNamespace) -> JsonValue {
    json!(ns
        .fields
        .iter()
        .map(|f| json!({"value": String::from_utf8_lossy(&f.value)}))
        .collect::<Vec<_>>())
}

/// Convert a track name string to MOQTByteString format.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.4
fn track_name_to_qlog(name: &str) -> JsonValue {
    json!({"value": name})
}

/// Convert a Location to MOQTLocation format.
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.5
fn location_to_qlog(loc: &coding::Location) -> JsonValue {
    json!({"group": loc.group_id, "object": loc.object_id})
}

fn create_control_message_event(
    time: f64,
    stream_id: u64,
    is_parsed: bool,
    message: JsonValue,
) -> Event {
    if is_parsed {
        Event {
            time,
            name: "moqt:control_message_parsed".to_string(),
            data: EventData::ControlMessageParsed(ControlMessageParsed { stream_id, message }),
        }
    } else {
        Event {
            time,
            name: "moqt:control_message_created".to_string(),
            data: EventData::ControlMessageCreated(ControlMessageCreated { stream_id, message }),
        }
    }
}

/// Create a control_message_parsed event for CLIENT_SETUP
pub fn client_setup_parsed(time: f64, stream_id: u64, msg: &setup::Client) -> Event {
    let versions: Vec<String> = msg.versions.0.iter().map(|v| format!("{:?}", v)).collect();
    create_control_message_event(
        time,
        stream_id,
        true,
        json!({
            "type": "client_setup",
            "number_of_supported_versions": msg.versions.0.len(),
            "supported_versions": versions,
            "number_of_parameters": msg.params.0.len(),
            "setup_parameters": setup_params_to_qlog(&msg.params.0),
        }),
    )
}

/// Create a control_message_created event for SERVER_SETUP
pub fn server_setup_created(time: f64, stream_id: u64, msg: &setup::Server) -> Event {
    create_control_message_event(
        time,
        stream_id,
        false,
        json!({
            "type": "server_setup",
            "selected_version": format!("{:?}", msg.version),
            "number_of_parameters": msg.params.0.len(),
            "setup_parameters": setup_params_to_qlog(&msg.params.0),
        }),
    )
}

/// Helper to convert SUBSCRIBE message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.6
fn subscribe_to_json(msg: &message::Subscribe) -> JsonValue {
    let mut json = json!({
        "type": "subscribe",
        "request_id": msg.id,
        "track_namespace": namespace_to_qlog(&msg.track_namespace),
        "track_name": track_name_to_qlog(&msg.track_name),
        "subscriber_priority": msg.subscriber_priority,
        "group_order": format!("{:?}", msg.group_order),
        "forward": msg.forward,
        "filter_type": format!("{:?}", msg.filter_type),
        "number_of_parameters": msg.params.0.len(),
        "parameters": params_to_qlog(&msg.params.0),
    });

    // Add optional fields based on filter type
    if let Some(start_loc) = &msg.start_location {
        json["start_location"] = location_to_qlog(start_loc);
    }
    if let Some(end_group) = msg.end_group_id {
        json["end_group"] = json!(end_group);
    }

    json
}

/// Create a control_message_parsed event for SUBSCRIBE
pub fn subscribe_parsed(time: f64, stream_id: u64, msg: &message::Subscribe) -> Event {
    create_control_message_event(time, stream_id, true, subscribe_to_json(msg))
}

/// Create a control_message_created event for SUBSCRIBE
pub fn subscribe_created(time: f64, stream_id: u64, msg: &message::Subscribe) -> Event {
    create_control_message_event(time, stream_id, false, subscribe_to_json(msg))
}

/// Helper to convert SUBSCRIBE_OK message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.7
fn subscribe_ok_to_json(msg: &message::SubscribeOk) -> JsonValue {
    let mut json = json!({
        "type": "subscribe_ok",
        "request_id": msg.id,
        "track_alias": msg.track_alias,
        "expires": msg.expires,
        "group_order": format!("{:?}", msg.group_order),
        "content_exists": msg.content_exists,
        "number_of_parameters": msg.params.0.len(),
        "parameters": params_to_qlog(&msg.params.0),
    });

    // Add optional largest_location if content exists
    if msg.content_exists {
        if let Some(largest) = &msg.largest_location {
            json["largest_location"] = location_to_qlog(largest);
        }
    }

    json
}

/// Create a control_message_parsed event for SUBSCRIBE_OK
pub fn subscribe_ok_parsed(time: f64, stream_id: u64, msg: &message::SubscribeOk) -> Event {
    create_control_message_event(time, stream_id, true, subscribe_ok_to_json(msg))
}

/// Create a control_message_created event for SUBSCRIBE_OK
pub fn subscribe_ok_created(time: f64, stream_id: u64, msg: &message::SubscribeOk) -> Event {
    create_control_message_event(time, stream_id, false, subscribe_ok_to_json(msg))
}

/// Helper to convert SUBSCRIBE_ERROR message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.8
fn subscribe_error_to_json(msg: &message::SubscribeError) -> JsonValue {
    json!({
        "type": "subscribe_error",
        "request_id": msg.id,
        "error_code": msg.error_code,
        "reason": &msg.reason_phrase.0,
    })
}

/// Create a control_message_parsed event for SUBSCRIBE_ERROR
pub fn subscribe_error_parsed(time: f64, stream_id: u64, msg: &message::SubscribeError) -> Event {
    create_control_message_event(time, stream_id, true, subscribe_error_to_json(msg))
}

/// Create a control_message_created event for SUBSCRIBE_ERROR
pub fn subscribe_error_created(time: f64, stream_id: u64, msg: &message::SubscribeError) -> Event {
    create_control_message_event(time, stream_id, false, subscribe_error_to_json(msg))
}

/// Helper to convert PUBLISH_NAMESPACE message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.22
fn publish_namespace_to_json(msg: &message::PublishNamespace) -> JsonValue {
    json!({
        "type": "publish_namespace",
        "request_id": msg.id,
        "track_namespace": namespace_to_qlog(&msg.track_namespace),
        "number_of_parameters": msg.params.0.len(),
        "parameters": params_to_qlog(&msg.params.0),
    })
}

/// Create a control_message_parsed event for PUBLISH_NAMESPACE
pub fn publish_namespace_parsed(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespace,
) -> Event {
    create_control_message_event(time, stream_id, true, publish_namespace_to_json(msg))
}

/// Create a control_message_created event for PUBLISH_NAMESPACE
pub fn publish_namespace_created(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespace,
) -> Event {
    create_control_message_event(time, stream_id, false, publish_namespace_to_json(msg))
}

/// Helper to convert PUBLISH_NAMESPACE_OK message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.23
fn publish_namespace_ok_to_json(msg: &message::PublishNamespaceOk) -> JsonValue {
    json!({
        "type": "publish_namespace_ok",
        "request_id": msg.id,
    })
}

/// Create a control_message_parsed event for PUBLISH_NAMESPACE_OK
pub fn publish_namespace_ok_parsed(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespaceOk,
) -> Event {
    create_control_message_event(time, stream_id, true, publish_namespace_ok_to_json(msg))
}

/// Create a control_message_created event for PUBLISH_NAMESPACE_OK
pub fn publish_namespace_ok_created(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespaceOk,
) -> Event {
    create_control_message_event(time, stream_id, false, publish_namespace_ok_to_json(msg))
}

/// Helper to convert PUBLISH_NAMESPACE_ERROR message to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.24
fn publish_namespace_error_to_json(msg: &message::PublishNamespaceError) -> JsonValue {
    json!({
        "type": "publish_namespace_error",
        "request_id": msg.id,
        "error_code": msg.error_code,
        "reason": &msg.reason_phrase.0,
    })
}

/// Create a control_message_parsed event for PUBLISH_NAMESPACE_ERROR
pub fn publish_namespace_error_parsed(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespaceError,
) -> Event {
    create_control_message_event(time, stream_id, true, publish_namespace_error_to_json(msg))
}

/// Create a control_message_created event for PUBLISH_NAMESPACE_ERROR
pub fn publish_namespace_error_created(
    time: f64,
    stream_id: u64,
    msg: &message::PublishNamespaceError,
) -> Event {
    create_control_message_event(time, stream_id, false, publish_namespace_error_to_json(msg))
}

/// Create a control_message_parsed event for UNSUBSCRIBE
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.10
pub fn unsubscribe_parsed(time: f64, stream_id: u64, msg: &message::Unsubscribe) -> Event {
    create_control_message_event(
        time,
        stream_id,
        true,
        json!({"type": "unsubscribe", "request_id": msg.id}),
    )
}

/// Create a control_message_created event for UNSUBSCRIBE
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.10
pub fn unsubscribe_created(time: f64, stream_id: u64, msg: &message::Unsubscribe) -> Event {
    create_control_message_event(
        time,
        stream_id,
        false,
        json!({"type": "unsubscribe", "request_id": msg.id}),
    )
}

/// Create a control_message_parsed event for GOAWAY
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.3
pub fn go_away_parsed(time: f64, stream_id: u64, msg: &message::GoAway) -> Event {
    create_control_message_event(
        time,
        stream_id,
        true,
        json!({"type": "goaway", "new_session_uri": &msg.uri.0}),
    )
}

/// Create a control_message_created event for GOAWAY
/// Per draft-pardue-moq-qlog-moq-events-03 Section 5.6.3
pub fn go_away_created(time: f64, stream_id: u64, msg: &message::GoAway) -> Event {
    create_control_message_event(
        time,
        stream_id,
        false,
        json!({"type": "goaway", "new_session_uri": &msg.uri.0}),
    )
}

// Data plane events

/// Helper to convert SubgroupHeader to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 4.9
fn subgroup_header_to_json(header: &data::SubgroupHeader) -> JsonValue {
    let mut json = json!({
        "track_alias": header.track_alias,
        "group_id": header.group_id,
        "publisher_priority": header.publisher_priority,
    });

    if let Some(subgroup_id) = header.subgroup_id {
        json["subgroup_id"] = json!(subgroup_id);
    }

    json
}

/// Create a subgroup_header_parsed event
pub fn subgroup_header_parsed(time: f64, stream_id: u64, header: &data::SubgroupHeader) -> Event {
    Event {
        time,
        name: "moqt:subgroup_header_parsed".to_string(),
        data: EventData::SubgroupHeaderParsed(SubgroupHeaderParsed {
            stream_id,
            header: subgroup_header_to_json(header),
        }),
    }
}

/// Create a subgroup_header_created event
pub fn subgroup_header_created(time: f64, stream_id: u64, header: &data::SubgroupHeader) -> Event {
    Event {
        time,
        name: "moqt:subgroup_header_created".to_string(),
        data: EventData::SubgroupHeaderCreated(SubgroupHeaderCreated {
            stream_id,
            header: subgroup_header_to_json(header),
        }),
    }
}

/// Helper to convert SubgroupObject to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 4.11
fn subgroup_object_to_json(
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObject,
) -> JsonValue {
    let mut object_data = json!({
        "group_id": group_id,
        "subgroup_id": subgroup_id,
        "object_id": object_id,
        "extension_headers_length": 0,
        // TODO send object_payload itself
        "object_payload_length": object.payload_length,
    });

    if let Some(status) = object.status {
        object_data["object_status"] = json!(format!("{:?}", status));
    }

    object_data
}

/// Create a subgroup_object_parsed event
pub fn subgroup_object_parsed(
    time: f64,
    stream_id: u64,
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObject,
) -> Event {
    Event {
        time,
        name: "moqt:subgroup_object_parsed".to_string(),
        data: EventData::SubgroupObjectParsed(SubgroupObjectParsed {
            stream_id,
            object: subgroup_object_to_json(group_id, subgroup_id, object_id, object),
        }),
    }
}

/// Create a subgroup_object_created event
pub fn subgroup_object_created(
    time: f64,
    stream_id: u64,
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObject,
) -> Event {
    Event {
        time,
        name: "moqt:subgroup_object_created".to_string(),
        data: EventData::SubgroupObjectCreated(SubgroupObjectCreated {
            stream_id,
            object: subgroup_object_to_json(group_id, subgroup_id, object_id, object),
        }),
    }
}

/// Helper to convert SubgroupObjectExt to JSON
/// Per draft-pardue-moq-qlog-moq-events-03 Section 4.11
fn subgroup_object_ext_to_json(
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObjectExt,
) -> JsonValue {
    let mut object_data = json!({
        "group_id": group_id,
        "subgroup_id": subgroup_id,
        "object_id": object_id,
        "extension_headers_length": ext_headers_wire_len(&object.extension_headers),
        "extension_headers": extension_headers_to_qlog(&object.extension_headers.0),
        // TODO send object_payload itself
        "object_payload_length": object.payload_length,
    });

    if let Some(status) = object.status {
        object_data["object_status"] = json!(format!("{:?}", status));
    }

    object_data
}

/// Create a subgroup_object_parsed event (with extensions)
pub fn subgroup_object_ext_parsed(
    time: f64,
    stream_id: u64,
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObjectExt,
) -> Event {
    Event {
        time,
        name: "moqt:subgroup_object_parsed".to_string(),
        data: EventData::SubgroupObjectParsed(SubgroupObjectParsed {
            stream_id,
            object: subgroup_object_ext_to_json(group_id, subgroup_id, object_id, object),
        }),
    }
}

/// Create a subgroup_object_created event (with extensions)
pub fn subgroup_object_ext_created(
    time: f64,
    stream_id: u64,
    group_id: u64,
    subgroup_id: u64,
    object_id: u64,
    object: &data::SubgroupObjectExt,
) -> Event {
    Event {
        time,
        name: "moqt:subgroup_object_created".to_string(),
        data: EventData::SubgroupObjectCreated(SubgroupObjectCreated {
            stream_id,
            object: subgroup_object_ext_to_json(group_id, subgroup_id, object_id, object),
        }),
    }
}

/// Helper to convert Datagram to JSON
fn object_datagram_to_json(datagram: &data::Datagram) -> JsonValue {
    let mut json = json!({
        "datagram_type": format!("{:?}", datagram.datagram_type),
        "track_alias": datagram.track_alias,
        "group_id": datagram.group_id,
        "object_id": datagram.object_id.unwrap_or(0),
        "publisher_priority": datagram.publisher_priority,
        // TODO send object_playload
        "payload_length": datagram.payload.as_ref().map_or(0, |p| p.len()),
    });

    if let Some(extension_headers) = &datagram.extension_headers {
        json["extension_headers_length"] = json!(ext_headers_wire_len(extension_headers));
        json["extension_headers"] = extension_headers_to_qlog(&extension_headers.0);
    } else {
        json["extension_headers_length"] = json!(0u64);
    }

    if let Some(status) = datagram.status {
        json["object_status"] = json!(format!("{:?}", status));
    }

    json
}

/// Create a object_datagram_parsed event
pub fn object_datagram_parsed(time: f64, stream_id: u64, datagram: &data::Datagram) -> Event {
    Event {
        time,
        name: "moqt:object_datagram_parsed".to_string(),
        data: EventData::ObjectDatagramParsed(ObjectDatagramParsed {
            stream_id,
            object: object_datagram_to_json(datagram),
        }),
    }
}

/// Create a object_datagram_created event
pub fn object_datagram_created(time: f64, stream_id: u64, datagram: &data::Datagram) -> Event {
    Event {
        time,
        name: "moqt:object_datagram_created".to_string(),
        data: EventData::ObjectDatagramCreated(ObjectDatagramCreated {
            stream_id,
            object: object_datagram_to_json(datagram),
        }),
    }
}

// LogLevel events (generic logging)

/// Log levels for qlog loglevel events
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Fatal,
    Error,
    Warn,
    Info,
    Debug,
    Verbose,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Fatal => "fatal",
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Verbose => "verbose",
        }
    }
}

/// Create a loglevel event for flexible logging
///
/// # Arguments
/// * `time` - Timestamp in milliseconds since connection start
/// * `level` - Log level (debug, info, warn, error, fatal, verbose)
/// * `message` - Freeform message text with structured information
///
/// # Example
/// ```ignore
/// loglevel_event(
///     12.345,
///     LogLevel::Debug,
///     "object_queued: track_alias=1 group=5 subgroup=2 object=10 payload_len=1024"
/// )
/// ```
pub fn loglevel_event(time: f64, level: LogLevel, message: String) -> Event {
    Event {
        time,
        name: format!("loglevel:{}", level.as_str()),
        data: EventData::LogLevel(LogLevelEvent { message }),
    }
}
