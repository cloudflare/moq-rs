//! Minimal unsigned CWT (CBOR Web Token) builder for CAT tokens.
//!
//! Builds a CBOR map with RFC 8392 claim keys that the relay's
//! `CatTokenParser` can decode directly (no COSE_Mac0 signature).
//!
//! Claim keys used:
//! - 2: sub (subject) — the JWT string for plugin-level auth
//! - 4: exp (expiration) — Unix seconds
//! - 6: iat (issued at) — Unix seconds
//! - 100: moqt (scope) — permissions array

use std::time::{SystemTime, UNIX_EPOCH};

use ciborium::Value;

/// ANNOUNCE (PUBLISH_NAMESPACE) action code.
const ACTION_ANNOUNCE: i64 = 2;
/// SUBSCRIBE action code.
const ACTION_SUBSCRIBE: i64 = 4;
/// PUBLISH action code.
const ACTION_PUBLISH: i64 = 6;

/// Build an unsigned CWT claims map as CBOR bytes.
///
/// The `subject` is typically a JWT string that the Red5Pro plugin will
/// validate at Layer 2.  `ttl_minutes` controls the token lifetime.
///
/// The moqt scope grants ANNOUNCE, SUBSCRIBE, and PUBLISH with
/// match-all namespace and track patterns (empty maps).
pub fn build_cwt(subject: &str, ttl_minutes: u64) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs();
    let exp = now + (ttl_minutes * 60);

    // Actions array: [ANNOUNCE, SUBSCRIBE, PUBLISH]
    let actions = Value::Array(vec![
        Value::Integer(ACTION_ANNOUNCE.into()),
        Value::Integer(ACTION_SUBSCRIBE.into()),
        Value::Integer(ACTION_PUBLISH.into()),
    ]);

    // Empty maps = match-all for namespace and track name
    let ns_match = Value::Map(vec![]);
    let track_match = Value::Map(vec![]);

    // Single scope: [actions, ns_match, track_match]
    let scope = Value::Array(vec![actions, ns_match, track_match]);

    // moqt claim: array of scopes
    let moqt = Value::Array(vec![scope]);

    // CWT claims map with integer keys (RFC 8392)
    let claims = Value::Map(vec![
        (Value::Integer(2i64.into()), Value::Text(subject.to_string())), // sub
        (Value::Integer(4i64.into()), Value::Integer((exp as i64).into())), // exp
        (Value::Integer(6i64.into()), Value::Integer((now as i64).into())), // iat
        (Value::Integer(100i64.into()), moqt), // moqt scope
    ]);

    let mut buf = Vec::new();
    ciborium::into_writer(&claims, &mut buf).expect("CBOR encoding failed");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cwt_round_trip() {
        let cwt_bytes = build_cwt("my-jwt-token", 60);
        // Should be valid CBOR
        let decoded: Value =
            ciborium::from_reader(&cwt_bytes[..]).expect("should decode as CBOR");
        // Should be a map
        if let Value::Map(entries) = decoded {
            // Should have 4 entries (sub, exp, iat, moqt)
            assert_eq!(entries.len(), 4);
            // Check subject claim (key 2)
            let sub = entries
                .iter()
                .find(|(k, _)| *k == Value::Integer(2i64.into()))
                .expect("should have sub claim");
            assert_eq!(sub.1, Value::Text("my-jwt-token".to_string()));
        } else {
            panic!("expected CBOR map");
        }
    }

    #[test]
    fn cwt_has_moqt_scope() {
        let cwt_bytes = build_cwt("test", 30);
        let decoded: Value =
            ciborium::from_reader(&cwt_bytes[..]).expect("should decode as CBOR");
        if let Value::Map(entries) = decoded {
            let moqt = entries
                .iter()
                .find(|(k, _)| *k == Value::Integer(100i64.into()))
                .expect("should have moqt claim");
            // moqt is an array of scopes
            if let Value::Array(scopes) = &moqt.1 {
                assert_eq!(scopes.len(), 1);
                // Each scope is [actions, ns_match, track_match]
                if let Value::Array(scope) = &scopes[0] {
                    assert_eq!(scope.len(), 3);
                    // Actions should be [2, 4, 6]
                    if let Value::Array(actions) = &scope[0] {
                        assert_eq!(actions.len(), 3);
                    } else {
                        panic!("expected actions array");
                    }
                } else {
                    panic!("expected scope array");
                }
            } else {
                panic!("expected moqt array");
            }
        } else {
            panic!("expected CBOR map");
        }
    }
}
