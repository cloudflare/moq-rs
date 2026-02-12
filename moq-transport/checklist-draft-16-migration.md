# Migration Checklist: draft-14 → draft-latest (January 2026)

Based on the changelog in Appendix A.1 of the [MOQT draft spec](https://moq-wg.github.io/moq-transport/draft-ietf-moq-transport.html) and the current codebase.

---

## Setup and Control Plane Changes

- [x] **ALPN for version negotiation (#499)** - Always use ALPN for version negotiation instead of in-band version fields. Update session establishment to negotiate via ALPN.
  - Removed all version specific fields from messages

- [x] **Use correct setup parameters** - Verify setup parameter code points match spec.

- [x] **Consolidate Error Message types (#1159)** - Use `REQUEST_ERROR` (0x5) as unified error response for SUBSCRIBE, FETCH, PUBLISH, SUBSCRIBE_NAMESPACE, PUBLISH_NAMESPACE, TRACK_STATUS. Current codebase has separate `SubscribeError`, `FetchError`, `PublishError`, etc.

- [x] **Change MOQT_IMPLEMENTATION code point to 0x7 (#1191)** - Update setup parameter code point if implemented.

- [x] **Add Forward to SUBSCRIBE_NAMESPACE (#1220)** - Add `FORWARD` parameter support in `SubscribeNamespace` message.

- [ ] **Parameters for Group Order, Subscribe Priority and Subscription Filter (#1273)** - Move `group_order`, `subscriber_priority`, and filter to Parameters (KeyValuePairs) instead of fixed fields in SUBSCRIBE. Currently in `subscribe.rs:20-27` these are fixed fields.

- [x] **REQUEST_OK message (#1274)** - Add new `REQUEST_OK` (0x7) message type for acknowledging REQUEST_UPDATE, TRACK_STATUS, SUBSCRIBE_NAMESPACE, and PUBLISH_NAMESPACE. Current codebase uses separate OK messages.

- [ ] **Subscribe Update Acknowledgements (#1275)** - REQUEST_UPDATE now requires a REQUEST_OK or REQUEST_ERROR response.

- [ ] **Disallow DELETE and USE_ALIAS in CLIENT_SETUP (#1277)** - Add validation for Authorization Token Alias Types in CLIENT_SETUP.

- [x] **Remove Expires field from SUBSCRIBE_OK (#1282)** - Move `expires` from fixed field to parameter in `SubscribeOk`. Currently at `subscribe_ok.rs:14` as fixed field.

- [x] **Make Forward a Parameter (#1283)** - Move `forward` from fixed field to Parameter in SUBSCRIBE. Currently at `subscribe.rs:24` as fixed field.

- [ ] **Allow SUBSCRIBE_UPDATE to increase the end location (#1288)** - Update REQUEST_UPDATE handling logic.

- [x] **Add default port for raw QUIC (#1289)** - Default port 443 for `moqt://` scheme.

- [x] **Unsubscribe Namespace linked to Subscribe Namespace (#1292)** - Remove `UnsubscribeNamespace` message - cancellation now done by closing the bidirectional stream. Current codebase has `unsubscribe_namespace.rs`.

---

## Data Plane Wire Format and Handling

- [ ] **Fetch Object serialization optimization (#949)** - New FETCH stream object format with `Serialization Flags` field and delta encoding. Current FETCH implementation needs significant update.

- [x] **Make default PUBLISHER_PRIORITY a parameter, optional in Subgroup/Datagram (#1056)** - Publisher Priority becomes optional in stream headers when inherited from control message.

- [ ] **Allow datagram status with object ID=0 (#1197)** - Update datagram handling for status datagrams.

- [ ] **Disallow object extension headers in all non-Normal status objects (#1266)** - Add validation to reject extensions on status objects.

- [ ] **Objects for malformed track must not be cached (#1290)** - Add malformed track detection and cache bypass.

- [ ] **Remove NO_OBJECTS fetch error code (#1303)** - Remove this error code if present.

- [ ] **Clarify max_cache_duration parameter when omitted (#1287)** - Objects cached until eviction if parameter omitted.

---

## New/Changed Message Wire Formats

### SUBSCRIBE message changes
- [ ] Remove inline `subscriber_priority`, `group_order`, `forward`, `filter_type` fields
- [ ] Add `SUBSCRIPTION_FILTER` (0x21), `SUBSCRIBER_PRIORITY` (0x20), `GROUP_ORDER` (0x22), `FORWARD` (0x10) as Parameters

New wire format:
```
SUBSCRIBE Message {
  Type (i) = 0x3,
  Length (16),
  Request ID (i),
  Track Namespace (..),
  Track Name Length (i),
  Track Name (..),
  Number of Parameters (i),
  Parameters (..) ...
}
```

### SUBSCRIBE_OK message changes
- [ ] Remove inline `expires`, `group_order`, `content_exists`, `largest_location` fields
- [ ] Add `Track Alias` (mandatory field)
- [ ] Add `Track Extensions` field
- [ ] Move `EXPIRES` (0x8), `GROUP_ORDER` (0x22), `LARGEST_OBJECT` (0x9), `PUBLISHER_PRIORITY` (0x0E) to Parameters

New wire format:
```
SUBSCRIBE_OK Message {
  Type (i) = 0x4,
  Length (16),
  Request ID (i),
  Track Alias (i),
  Number of Parameters (i),
  Parameters (..) ...,
  Track Extensions (..),
}
```

### SUBGROUP_HEADER changes
- [x] Add SUBGROUP_HEADER types 0x30-0x3D for Priority not present variants

| Type | Subgroup ID | Extensions | Contains End of Group | Priority Present |
|------|-------------|------------|----------------------|------------------|
| 0x10 | 0 | No | No | Yes |
| 0x11 | 0 | Yes | No | Yes |
| 0x12 | First Object ID | No | No | Yes |
| 0x13 | First Object ID | Yes | No | Yes |
| 0x14 | Field Present | No | No | Yes |
| 0x15 | Field Present | Yes | No | Yes |
| 0x18 | 0 | No | Yes | Yes |
| 0x19 | 0 | Yes | Yes | Yes |
| 0x1A | First Object ID | No | Yes | Yes |
| 0x1B | First Object ID | Yes | Yes | Yes |
| 0x1C | Field Present | No | Yes | Yes |
| 0x1D | Field Present | Yes | Yes | Yes |
| 0x30 | 0 | No | No | No |
| 0x31 | 0 | Yes | No | No |
| 0x32 | First Object ID | No | No | No |
| 0x33 | First Object ID | Yes | No | No |
| 0x34 | Field Present | No | No | No |
| 0x35 | Field Present | Yes | No | No |
| 0x38 | 0 | No | Yes | No |
| 0x39 | 0 | Yes | Yes | No |
| 0x3A | First Object ID | No | Yes | No |
| 0x3B | First Object ID | Yes | Yes | No |
| 0x3C | Field Present | No | Yes | No |
| 0x3D | Field Present | Yes | Yes | No |

### OBJECT_DATAGRAM type expansion
- [x] Types 0x00-0x0F, 0x20-0x21, 0x24-0x25, 0x28-0x29, 0x2C-0x2D with bitfield encoding

| Type | End Of Group | Extensions | Object ID | Priority Present | Status/Payload |
|------|--------------|------------|-----------|------------------|----------------|
| 0x00 | No | No | Yes | Yes | Payload |
| 0x01 | No | Yes | Yes | Yes | Payload |
| 0x02 | Yes | No | Yes | Yes | Payload |
| 0x03 | Yes | Yes | Yes | Yes | Payload |
| 0x04 | No | No | No | Yes | Payload |
| 0x05 | No | Yes | No | Yes | Payload |
| 0x06 | Yes | No | No | Yes | Payload |
| 0x07 | Yes | Yes | No | Yes | Payload |
| 0x08-0x0F | ... | ... | ... | No | Payload |
| 0x20 | No | No | Yes | Yes | Status |
| 0x21 | No | Yes | Yes | Yes | Status |
| 0x24 | No | No | No | Yes | Status |
| 0x25 | No | Yes | No | Yes | Status |
| 0x28-0x2D | ... | ... | ... | No | Status |

### FETCH_HEADER object format
- [ ] New `Serialization Flags` based encoding with delta compression

```
{
  Serialization Flags (i),
  [Group ID (i),]
  [Subgroup ID (i),]
  [Object ID (i),]
  [Publisher Priority (8),]
  [Extensions (..),]
  Object Payload Length (i),
  [Object Payload (..),]
}
```

Serialization Flags bit meanings:
- 0x03 mask: Subgroup ID encoding (0=zero, 1=prior, 2=prior+1, 3=field present)
- 0x04: Object ID field present (else prior+1)
- 0x08: Group ID field present (else prior)
- 0x10: Priority field present (else prior)
- 0x20: Extensions field present

Special values:
- 0x8C: End of Non-Existent Range
- 0x10C: End of Unknown Range

---

## New Parameters (Version-Specific)

| Type | Name | Messages |
|------|------|----------|
| 0x02 | DELIVERY_TIMEOUT | REQUEST_OK, PUBLISH, PUBLISH_OK, SUBSCRIBE, SUBSCRIBE_OK, REQUEST_UPDATE |
| 0x03 | AUTHORIZATION_TOKEN | CLIENT_SETUP, SERVER_SETUP, PUBLISH, SUBSCRIBE, REQUEST_UPDATE, SUBSCRIBE_NAMESPACE, PUBLISH_NAMESPACE, TRACK_STATUS, FETCH |
| 0x04 | MAX_CACHE_DURATION | PUBLISH, SUBSCRIBE_OK, FETCH_OK, REQUEST_OK |
| 0x08 | EXPIRES | SUBSCRIBE_OK, PUBLISH, PUBLISH_OK |
| 0x09 | LARGEST_OBJECT | SUBSCRIBE_OK, PUBLISH, REQUEST_OK |
| 0x0E | PUBLISHER_PRIORITY | SUBSCRIBE_OK, PUBLISH |
| 0x10 | FORWARD | SUBSCRIBE, REQUEST_UPDATE, PUBLISH, PUBLISH_OK, SUBSCRIBE_NAMESPACE |
| 0x20 | SUBSCRIBER_PRIORITY | SUBSCRIBE, FETCH, REQUEST_UPDATE, PUBLISH_OK |
| 0x21 | SUBSCRIPTION_FILTER | SUBSCRIBE, PUBLISH_OK, REQUEST_UPDATE |
| 0x22 | GROUP_ORDER | SUBSCRIBE, SUBSCRIBE_OK, REQUEST_OK, PUBLISH, PUBLISH_OK, FETCH |
| 0x30 | DYNAMIC_GROUPS | PUBLISH, SUBSCRIBE_OK |
| 0x32 | NEW_GROUP_REQUEST | PUBLISH_OK, SUBSCRIBE, REQUEST_UPDATE |

---

## New Extension Headers

| Type | Name | Description |
|------|------|-------------|
| 0xB | Immutable Extensions | Container for immutable extension headers |
| 0x3C | Prior Group ID Gap | Indicate missing prior groups |
| 0x3E | Prior Object ID Gap | Indicate missing prior objects |

---

## Setup Parameters

| Type | Name | Description |
|------|------|-------------|
| 0x01 | PATH | Client-only, native QUIC, path-abempty + query |
| 0x02 | MAX_REQUEST_ID | Initial Maximum Request ID |
| 0x04 | MAX_AUTH_TOKEN_CACHE_SIZE | Limit on authorization token cache |
| 0x05 | AUTHORITY | Client-only, native QUIC authority |
| 0x07 | MOQT_IMPLEMENTATION | Implementation identification string |

---

## Error Code Updates

### Session Termination Codes

| Code | Name |
|------|------|
| 0x0 | NO_ERROR |
| 0x1 | INTERNAL_ERROR |
| 0x2 | UNAUTHORIZED |
| 0x3 | PROTOCOL_VIOLATION |
| 0x4 | INVALID_REQUEST_ID |
| 0x5 | DUPLICATE_TRACK_ALIAS |
| 0x6 | KEY_VALUE_FORMATTING_ERROR |
| 0x7 | TOO_MANY_REQUESTS |
| 0x8 | INVALID_PATH |
| 0x9 | MALFORMED_PATH |
| 0x10 | GOAWAY_TIMEOUT |
| 0x11 | CONTROL_MESSAGE_TIMEOUT |
| 0x12 | DATA_STREAM_TIMEOUT |
| 0x13 | AUTH_TOKEN_CACHE_OVERFLOW |
| 0x14 | DUPLICATE_AUTH_TOKEN_ALIAS |
| 0x15 | VERSION_NEGOTIATION_FAILED |
| 0x16 | MALFORMED_AUTH_TOKEN |
| 0x17 | UNKNOWN_AUTH_TOKEN_ALIAS |
| 0x18 | EXPIRED_AUTH_TOKEN |
| 0x19 | INVALID_AUTHORITY |
| 0x1A | MALFORMED_AUTHORITY |

### REQUEST_ERROR Codes

| Code | Name |
|------|------|
| 0x0 | INTERNAL_ERROR |
| 0x1 | UNAUTHORIZED |
| 0x2 | TIMEOUT |
| 0x3 | NOT_SUPPORTED |
| 0x4 | MALFORMED_AUTH_TOKEN |
| 0x5 | EXPIRED_AUTH_TOKEN |
| 0x10 | DOES_NOT_EXIST |
| 0x11 | INVALID_RANGE |
| 0x12 | MALFORMED_TRACK |
| 0x19 | DUPLICATE_SUBSCRIPTION |
| 0x20 | UNINTERESTED |
| 0x30 | PREFIX_OVERLAP |
| 0x32 | INVALID_JOINING_REQUEST_ID |

### PUBLISH_DONE Codes

| Code | Name |
|------|------|
| 0x0 | INTERNAL_ERROR |
| 0x1 | UNAUTHORIZED |
| 0x2 | TRACK_ENDED |
| 0x3 | SUBSCRIPTION_ENDED |
| 0x4 | GOING_AWAY |
| 0x5 | EXPIRED |
| 0x6 | TOO_FAR_BEHIND |
| 0x8 | UPDATE_FAILED |
| 0x12 | MALFORMED_TRACK |

### Data Stream Reset Codes

| Code | Name |
|------|------|
| 0x0 | INTERNAL_ERROR |
| 0x1 | CANCELLED |
| 0x2 | DELIVERY_TIMEOUT |
| 0x3 | SESSION_CLOSED |
| 0x4 | UNKNOWN_OBJECT_STATUS |
| 0x12 | MALFORMED_TRACK |

---

## Message Type Code Points

| Message | Code |
|---------|------|
| CLIENT_SETUP | 0x20 |
| SERVER_SETUP | 0x21 |
| GOAWAY | 0x10 |
| MAX_REQUEST_ID | 0x15 |
| REQUESTS_BLOCKED | 0x1A |
| REQUEST_OK | 0x7 |
| REQUEST_ERROR | 0x5 |
| SUBSCRIBE | 0x3 |
| SUBSCRIBE_OK | 0x4 |
| REQUEST_UPDATE | 0x2 |
| UNSUBSCRIBE | 0xA |
| PUBLISH | 0x1D |
| PUBLISH_OK | 0x1E |
| PUBLISH_DONE | 0xB |
| FETCH | 0x16 |
| FETCH_OK | 0x18 |
| FETCH_CANCEL | 0x17 |
| TRACK_STATUS | 0xD |
| PUBLISH_NAMESPACE | 0x6 |
| NAMESPACE | 0x8 |
| PUBLISH_NAMESPACE_DONE | 0x9 |
| NAMESPACE_DONE | 0xE |
| PUBLISH_NAMESPACE_CANCEL | 0xC |
| SUBSCRIBE_NAMESPACE | 0x11 |

---

## Structural/Behavioral Changes

- [ ] **Subscription State Machine** - Implement formal Idle → Pending → Established → Terminated state machine.

- [ ] **Bidirectional SUBSCRIBE_NAMESPACE stream** - SUBSCRIBE_NAMESPACE now uses bidirectional stream, response messages (NAMESPACE, NAMESPACE_DONE) sent on response half.

- [ ] **Track Extensions in control messages** - Add Track Extensions field to SUBSCRIBE_OK, PUBLISH, FETCH_OK.

- [ ] **FETCH Joining calculation** - Update joining fetch end location calculation per Section 9.16.2.1.

- [ ] **Omitting subgroup object requires reset** - Publisher MUST reset stream if omitting objects.

- [ ] **Duplicate header extension rules** - Define and enforce duplication rules for extension headers.

---

## Files Likely Requiring Changes

Based on the codebase structure:

| File | Changes Required |
|------|------------------|
| `moq-transport/src/message/subscribe.rs` | Major restructure - move fields to parameters |
| `moq-transport/src/message/subscribe_ok.rs` | Major restructure - move fields to parameters, add Track Extensions |
| `moq-transport/src/message/mod.rs` | Add REQUEST_OK, NAMESPACE, NAMESPACE_DONE; update message IDs |
| `moq-transport/src/message/subscribe_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/fetch_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/publish_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/subscribe_namespace_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/publish_namespace_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/track_status_error.rs` | Consolidate to REQUEST_ERROR |
| `moq-transport/src/message/unsubscribe_namespace.rs` | Remove (stream closure replaces it) |
| `moq-transport/src/message/subscribe_namespace_ok.rs` | Replace with REQUEST_OK |
| `moq-transport/src/message/publish_namespace_ok.rs` | Replace with REQUEST_OK |
| `moq-transport/src/message/track_status_ok.rs` | Replace with REQUEST_OK |
| `moq-transport/src/setup/` | Add AUTHORITY, MAX_AUTH_TOKEN_CACHE_SIZE, AUTHORIZATION_TOKEN params |
| `moq-transport/src/data/` | Update stream/datagram formats, add new SUBGROUP_HEADER types |
| `moq-transport/src/session/mod.rs` | Handle new message flows, REQUEST_OK/REQUEST_ERROR |
| `moq-transport/src/serve/error.rs` | Update error codes |

---

## Migration Priority

### Phase 1: Core Message Format Changes
1. ~~Consolidate error messages to REQUEST_ERROR~~ (DONE)
2. ~~Add REQUEST_OK message~~ (DONE)
3. ~~Update SUBSCRIBE/SUBSCRIBE_OK wire format (parameters)~~ (DONE)
4. ~~Remove UnsubscribeNamespace~~ (DONE)

### Phase 2: Data Plane Updates
1. ~~Add new SUBGROUP_HEADER types (0x30-0x3D)~~ (DONE)
2. ~~Update OBJECT_DATAGRAM types~~ (DONE)
3. Update FETCH object serialization

### Phase 3: New Features
1. Authorization Token support
2. Extension Headers (Immutable, Prior Gap headers)
3. Dynamic Groups support
4. Track Extensions

### Phase 4: Session Management
1. Subscription state machine
2. Bidirectional SUBSCRIBE_NAMESPACE handling
3. New error codes
