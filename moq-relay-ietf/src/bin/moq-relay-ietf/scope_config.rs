// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-scope token authorization policy loaded from a JSON file.
//!
//! Lets an operator turn on Common Access Token (CAT) authorization for the
//! standalone relay without writing a coordinator. The file is selected with
//! `--scope-config` and is re-checked on [`Coordinator::get_scope_config`]
//! (at most once a second), so edits reach new sessions without a restart,
//! subject to the library's per-scope policy cache; see the crate README.
//!
//! # Format
//!
//! ```json
//! {
//!   "default": {
//!     "auth": {
//!       "keys": [{ "kid": "k1", "alg": "ES256", "pem_file": "issuer.pub.pem" }],
//!       "issuers": ["https://issuer.example"],
//!       "audiences": ["my-relay"],
//!       "clock_skew_secs": 60,
//!       "ttl_secs": 300
//!     }
//!   },
//!   "scopes": {
//!     "/open": { "auth": null }
//!   }
//! }
//! ```
//!
//! A scope ID is the session's normalized connection path (the WebTransport
//! URL path, or the CLIENT_SETUP PATH parameter over raw QUIC), for example `/demo`. A
//! session's policy is the `scopes` entry for its scope ID if there is one,
//! otherwise `default`. A session with no path is unscoped and always uses
//! `default`. With neither, the scope has no token policy.
//!
//! # Failing closed
//!
//! Only public keys are accepted: a PEM that is not a single `PUBLIC KEY`
//! block is a validation error. Once the flag is given, a file that is
//! missing, unreadable, malformed or invalid makes [`ScopeConfigSource::get`]
//! return an error, which the relay turns into an UNAUTHORIZED close. It is
//! never treated as "no policy". Validation is all-or-nothing across the file
//! so that a typo in one scope cannot quietly leave the rest half-configured.
//!
//! Errors are reported without echoing file contents: `serde_json` messages
//! can quote the offending value, which here may be key material.
//!
//! [`Coordinator::get_scope_config`]: moq_relay_ietf::Coordinator::get_scope_config

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use moq_relay_ietf::{
    AuthPublicKey, CoordinatorError, CoordinatorResult, ScopeAuthConfig, ScopeConfig,
    MAX_SCOPE_AUTH_KEYS,
};
use serde::Deserialize;

/// Largest scope config file the relay will read.
const MAX_CONFIG_FILE_BYTES: u64 = 1024 * 1024;

/// Largest `pem_file` the relay will read. An ES256 SPKI PEM is ~180 bytes.
const MAX_KEY_FILE_BYTES: u64 = 16 * 1024;

/// How long a load result is reused before the files are checked again.
///
/// Shared by every scope: the library caches policy per scope, so with many
/// scopes their cache misses arrive independently, and without this each one
/// would re-read the config and every key file it references. With it, the
/// files are read at most once per interval however many scopes ask. This is
/// also the upper bound on how stale a lookup can be, and it bounds re-reads
/// of a broken file, whose failures the library does not cache.
const REVALIDATE_INTERVAL: Duration = Duration::from_secs(1);

/// `ttl_secs` above this draws a load-time warning.
///
/// The TTL bounds how long a removed or rotated key keeps admitting new
/// sessions. One hour is 12 times the library default, which leaves room for
/// deliberate tuning while flagging values that would make a key removal
/// effectively need a restart. It is a warning, not a limit.
const WARN_TTL_SECS: u64 = 3600;

const PUBLIC_KEY_BEGIN: &str = "-----BEGIN PUBLIC KEY-----";
const PUBLIC_KEY_END: &str = "-----END PUBLIC KEY-----";

// ============================================================================
// File format (DTO)
// ============================================================================

/// Top level of the scope config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDto {
    /// Policy for any scope without its own entry, and for unscoped sessions.
    #[serde(default)]
    default: Option<ScopeDto>,

    /// Policies keyed by scope ID (normalized connection path).
    #[serde(default)]
    scopes: BTreeMap<String, ScopeDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeDto {
    /// `null` or absent: the scope has no token policy.
    #[serde(default)]
    auth: Option<AuthDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthDto {
    keys: Vec<KeyDto>,
    /// Accepted `iss` values. Empty or absent accepts any issuer.
    #[serde(default)]
    issuers: Vec<String>,
    /// Accepted `aud` values. Empty or absent accepts any audience.
    #[serde(default)]
    audiences: Vec<String>,
    /// Absent or 0 uses the library default (60 s).
    #[serde(default)]
    clock_skew_secs: Option<u64>,
    /// Absent or 0 uses the library default (300 s).
    #[serde(default)]
    ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyDto {
    #[serde(default)]
    kid: Option<String>,
    alg: String,
    /// Inline PEM. Exactly one of `pem` and `pem_file` must be set.
    #[serde(default)]
    pem: Option<String>,
    /// Path to a PEM file, relative to the config file's directory.
    #[serde(default)]
    pem_file: Option<PathBuf>,
}

// ============================================================================
// Validated policy
// ============================================================================

/// A validated scope config file, ready for lookups.
#[derive(Debug, Clone, Default)]
pub struct ScopePolicies {
    default: Option<ScopeAuthConfig>,
    scopes: HashMap<String, Option<ScopeAuthConfig>>,
}

impl ScopePolicies {
    /// The token policy for a scope: its own entry if present, else `default`.
    pub fn auth_for(&self, scope: Option<&str>) -> Option<ScopeAuthConfig> {
        match scope.and_then(|scope| self.scopes.get(scope)) {
            Some(entry) => entry.clone(),
            None => self.default.clone(),
        }
    }
}

/// Why a scope config could not be loaded.
///
/// Messages identify the location of the problem (file, scope, key index)
/// and never include file contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ScopeConfigError(String);

impl ScopeConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Parse and validate a scope config file's contents.
///
/// `base_dir` resolves relative `pem_file` paths. `read_key` loads a key
/// file; it is a parameter so the loader can record what it read for change
/// detection.
fn parse(
    contents: &[u8],
    base_dir: &Path,
    read_key: &mut dyn FnMut(&Path) -> Result<String, ScopeConfigError>,
) -> Result<ScopePolicies, ScopeConfigError> {
    let dto: FileDto = serde_json::from_slice(contents).map_err(describe_json_error)?;

    let default = match dto.default {
        Some(scope) => convert_scope(scope, "default", base_dir, read_key)?,
        None => None,
    };

    let mut scopes = HashMap::with_capacity(dto.scopes.len());
    for (scope_id, scope) in dto.scopes {
        validate_scope_id(&scope_id)?;
        let label = format!("scope {scope_id:?}");
        let auth = convert_scope(scope, &label, base_dir, read_key)?;
        scopes.insert(scope_id, auth);
    }

    Ok(ScopePolicies { default, scopes })
}

/// Describe a JSON error without quoting the input.
///
/// `serde_json`'s "invalid type"/"invalid value" messages embed the
/// offending value, which in this file may be a PEM. Field-name errors are
/// kept because they name keys, not values, and are the common typo.
fn describe_json_error(err: serde_json::Error) -> ScopeConfigError {
    let position = format!("line {} column {}", err.line(), err.column());
    let message = err.to_string();
    let detail = if ["unknown field", "missing field", "duplicate field"]
        .iter()
        .any(|prefix| message.starts_with(prefix))
    {
        // Strip serde_json's own " at line X column Y" suffix; we add ours.
        message
            .split(" at line ")
            .next()
            .unwrap_or("invalid field")
            .to_string()
    } else {
        match err.classify() {
            serde_json::error::Category::Syntax => "invalid JSON syntax".to_string(),
            serde_json::error::Category::Eof => "unexpected end of file".to_string(),
            serde_json::error::Category::Data => "a value has the wrong type or shape".to_string(),
            serde_json::error::Category::Io => "read error".to_string(),
        }
    };
    ScopeConfigError::new(format!("malformed scope config ({position}): {detail}"))
}

/// Scope keys must be written exactly as the relay will look them up.
///
/// A key like `demo` or `/demo/` would never match a session, silently
/// leaving the scope on `default`; rejecting it makes the mistake visible.
fn validate_scope_id(scope_id: &str) -> Result<(), ScopeConfigError> {
    match moq_transport::session::Session::normalize_connection_path(scope_id) {
        Ok(Some(normalized)) if normalized == scope_id => Ok(()),
        Ok(Some(normalized)) => Err(ScopeConfigError::new(format!(
            "scope key {scope_id:?} is not in normalized form; write it as {normalized:?}"
        ))),
        Ok(None) => Err(ScopeConfigError::new(format!(
            "scope key {scope_id:?} names the unscoped session; configure it with \"default\""
        ))),
        Err(err) => Err(ScopeConfigError::new(format!(
            "scope key {scope_id:?} is not a valid connection path: {err}"
        ))),
    }
}

fn convert_scope(
    scope: ScopeDto,
    label: &str,
    base_dir: &Path,
    read_key: &mut dyn FnMut(&Path) -> Result<String, ScopeConfigError>,
) -> Result<Option<ScopeAuthConfig>, ScopeConfigError> {
    let Some(auth) = scope.auth else {
        return Ok(None);
    };

    if auth.keys.is_empty() {
        return Err(ScopeConfigError::new(format!(
            "{label}: auth.keys is empty; at least one public key is required"
        )));
    }
    if auth.keys.len() > MAX_SCOPE_AUTH_KEYS {
        return Err(ScopeConfigError::new(format!(
            "{label}: {} keys configured, at most {MAX_SCOPE_AUTH_KEYS} are allowed",
            auth.keys.len()
        )));
    }

    let mut kids: Vec<&str> = Vec::new();
    for (index, key) in auth.keys.iter().enumerate() {
        if let Some(kid) = key.kid.as_deref() {
            if kid.is_empty() {
                return Err(ScopeConfigError::new(format!(
                    "{label}: key {index} has an empty kid; omit it instead"
                )));
            }
            if kids.contains(&kid) {
                return Err(ScopeConfigError::new(format!(
                    "{label}: duplicate kid {kid:?}"
                )));
            }
            kids.push(kid);
        }
    }

    for (field, values) in [("issuers", &auth.issuers), ("audiences", &auth.audiences)] {
        if values.iter().any(String::is_empty) {
            return Err(ScopeConfigError::new(format!(
                "{label}: auth.{field} contains an empty string"
            )));
        }
    }

    let mut keys = Vec::with_capacity(auth.keys.len());
    for (index, key) in auth.keys.into_iter().enumerate() {
        keys.push(convert_key(key, index, label, base_dir, read_key)?);
    }

    if let Some(secs) = auth.ttl_secs {
        if secs > WARN_TTL_SECS {
            tracing::warn!(
                scope = label,
                ttl_secs = secs,
                "ttl_secs is above {WARN_TTL_SECS}; a removed key keeps admitting new sessions \
                 for up to this long"
            );
        }
    }

    // Legal, but easy to do by accident when trimming the example down.
    for (field, values) in [("issuers", &auth.issuers), ("audiences", &auth.audiences)] {
        if values.is_empty() {
            tracing::warn!(
                scope = label,
                "auth.{field} is empty, so tokens with any {} are accepted",
                if field == "issuers" { "iss" } else { "aud" }
            );
        }
    }

    let mut config = ScopeAuthConfig::new(keys)
        .with_issuers(auth.issuers)
        .with_audiences(auth.audiences);
    // Zero means "use the default" in the library as well; passing it through
    // unchanged keeps one definition of that rule.
    if let Some(secs) = auth.clock_skew_secs {
        config = config.with_clock_skew(Duration::from_secs(secs));
    }
    if let Some(secs) = auth.ttl_secs {
        config = config.with_ttl(Duration::from_secs(secs));
    }
    Ok(Some(config))
}

fn convert_key(
    key: KeyDto,
    index: usize,
    label: &str,
    base_dir: &Path,
    read_key: &mut dyn FnMut(&Path) -> Result<String, ScopeConfigError>,
) -> Result<AuthPublicKey, ScopeConfigError> {
    let what = format!("{label}: key {index}");

    if key.alg != "ES256" {
        return Err(ScopeConfigError::new(format!(
            "{what}: unsupported alg {:?}; only \"ES256\" is supported",
            key.alg
        )));
    }

    let pem = match (key.pem, key.pem_file) {
        (Some(pem), None) => pem,
        (None, Some(file)) => {
            let path = if file.is_absolute() {
                file
            } else {
                base_dir.join(file)
            };
            read_key(&path).map_err(|err| ScopeConfigError::new(format!("{what}: {err}")))?
        }
        (Some(_), Some(_)) => {
            return Err(ScopeConfigError::new(format!(
                "{what}: set exactly one of \"pem\" and \"pem_file\", not both"
            )))
        }
        (None, None) => {
            return Err(ScopeConfigError::new(format!(
                "{what}: one of \"pem\" or \"pem_file\" is required"
            )))
        }
    };

    validate_public_key_pem(&pem).map_err(|err| ScopeConfigError::new(format!("{what}: {err}")))?;

    let mut public = AuthPublicKey::es256(pem);
    if let Some(kid) = key.kid {
        public = public.with_kid(kid);
    }
    Ok(public)
}

/// Accept only a single PEM `PUBLIC KEY` (SubjectPublicKeyInfo) block.
///
/// The relay verifies tokens; it never signs them, so it has no use for a
/// private key, and an operator pasting one here has a key-handling problem
/// worth surfacing loudly rather than quietly ignoring.
fn validate_public_key_pem(pem: &str) -> Result<(), ScopeConfigError> {
    if pem.contains("PRIVATE KEY") {
        return Err(ScopeConfigError::new(
            "contains a PRIVATE KEY; the relay only needs the issuer's public key \
             (-----BEGIN PUBLIC KEY-----). Remove the private key from the relay's \
             configuration and treat it as exposed",
        ));
    }

    let trimmed = pem.trim();
    if !trimmed.starts_with(PUBLIC_KEY_BEGIN) || !trimmed.ends_with(PUBLIC_KEY_END) {
        return Err(ScopeConfigError::new(
            "is not a PEM PUBLIC KEY block (expected -----BEGIN PUBLIC KEY----- ... \
             -----END PUBLIC KEY-----)",
        ));
    }
    if trimmed.matches("-----BEGIN ").count() != 1 {
        return Err(ScopeConfigError::new(
            "contains more than one PEM block; configure one key per entry",
        ));
    }

    // With token support compiled in, parse the key now so a bad key is a
    // load error naming the scope and key, rather than a scope that later
    // denies every session with a less specific reason.
    #[cfg(feature = "auth-cat")]
    cat_token::Es256Algorithm::from_public_key_pem(trimmed).map_err(|err| {
        ScopeConfigError::new(format!("is not a valid ES256 (P-256) public key: {err}"))
    })?;

    Ok(())
}

// ============================================================================
// Reloading source
// ============================================================================

/// A scope config file, re-checked on lookup.
///
/// A lookup within [`REVALIDATE_INTERVAL`] of the previous check reuses its
/// result without touching the disk. After that the files are read again,
/// and the parsed result is still reused if the config file and every key
/// file it references are byte-for-byte unchanged. Change detection is by
/// content rather than mtime so that two edits within one timestamp tick are
/// not missed. Only fingerprints of the raw file contents are retained; the
/// read buffers are dropped once parsed. The parsed policies do keep each
/// public key's PEM text, which the library needs to build its verifier.
///
/// The files are read with the state lock held, so concurrent lookups wait
/// for one load rather than each reading the files. A filesystem that hangs
/// (a stale network mount, say) therefore stalls every lookup behind it.
/// Lookups run on the blocking pool, so the async runtime is unaffected, and
/// stalled sessions are not admitted.
pub struct ScopeConfigSource {
    path: PathBuf,
    revalidate_interval: Duration,
    state: Mutex<Option<Loaded>>,
}

struct Loaded {
    /// Fingerprint of the config file contents, or `None` if it could not
    /// be read (which never matches, so the next lookup retries).
    config: Option<u64>,
    /// Key files referenced by the config, with a fingerprint of what was
    /// read (`None` if it could not be read).
    key_files: Vec<(PathBuf, Option<u64>)>,
    result: Result<Arc<ScopePolicies>, ScopeConfigError>,
    /// When the files were last read.
    checked_at: Instant,
}

impl ScopeConfigSource {
    /// Open a scope config file and load it once.
    ///
    /// Fails if the initial load fails, so that an operator who passes a
    /// broken file finds out at startup rather than from a stream of rejected
    /// sessions. Later failures do not stop the relay; they deny sessions.
    pub fn open(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        Self::open_with_interval(path, REVALIDATE_INTERVAL)
    }

    fn open_with_interval(
        path: impl Into<PathBuf>,
        revalidate_interval: Duration,
    ) -> anyhow::Result<Self> {
        let source = Self {
            path: path.into(),
            revalidate_interval,
            state: Mutex::new(None),
        };
        let policies = source
            .load()
            .map_err(|err| anyhow::anyhow!("--scope-config {}: {err}", source.path.display()))?;
        tracing::info!(
            path = %source.path.display(),
            scopes = policies.scopes.len(),
            default_enforced = policies.default.is_some(),
            "loaded scope config"
        );
        Ok(source)
    }

    /// The scope's configuration, as [`Coordinator::get_scope_config`]
    /// returns it.
    ///
    /// [`Coordinator::get_scope_config`]: moq_relay_ietf::Coordinator::get_scope_config
    pub async fn get(self: &Arc<Self>, scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
        let this = self.clone();
        let policies = tokio::task::spawn_blocking(move || this.load()).await?;
        let policies = policies.map_err(|err| {
            CoordinatorError::Other(anyhow::anyhow!(
                "scope config {} is unusable: {err}",
                self.path.display()
            ))
        })?;

        let mut config = ScopeConfig::default();
        config.auth = policies.auth_for(scope);
        Ok(config)
    }

    /// Load the file, reusing the previous result if nothing changed.
    ///
    /// Logs only when the outcome changes. A broken file is re-read once per
    /// [`REVALIDATE_INTERVAL`] for as long as the library keeps retrying, and
    /// logging each read would bury the first, useful, message.
    fn load(&self) -> Result<Arc<ScopePolicies>, ScopeConfigError> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());

        if let Some(loaded) = state.as_ref() {
            if loaded.checked_at.elapsed() < self.revalidate_interval {
                return loaded.result.clone();
            }
        }

        let contents = match read_limited(&self.path, MAX_CONFIG_FILE_BYTES) {
            Ok(contents) => contents,
            Err(err) => {
                let err = ScopeConfigError::new(format!("cannot read file: {err}"));
                self.record(&mut state, None, Vec::new(), Err(err.clone()));
                return Err(err);
            }
        };
        let config_fingerprint = fingerprint(&contents);

        if let Some(loaded) = state.as_mut() {
            if loaded.config == Some(config_fingerprint) && key_files_unchanged(&loaded.key_files) {
                loaded.checked_at = Instant::now();
                return loaded.result.clone();
            }
        }

        let base_dir = self
            .path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let mut key_files = Vec::new();
        let mut read_key = |path: &Path| {
            let result = read_limited(path, MAX_KEY_FILE_BYTES);
            key_files.push((
                path.to_path_buf(),
                result.as_ref().ok().map(|b| fingerprint(b)),
            ));
            let bytes = result.map_err(|err| {
                ScopeConfigError::new(format!("cannot read pem_file {}: {err}", path.display()))
            })?;
            String::from_utf8(bytes).map_err(|_| {
                ScopeConfigError::new(format!("pem_file {} is not UTF-8", path.display()))
            })
        };

        let result = parse(&contents, &base_dir, &mut read_key).map(Arc::new);
        self.record(
            &mut state,
            Some(config_fingerprint),
            key_files,
            result.clone(),
        );
        result
    }

    fn record(
        &self,
        state: &mut Option<Loaded>,
        config: Option<u64>,
        key_files: Vec<(PathBuf, Option<u64>)>,
        result: Result<Arc<ScopePolicies>, ScopeConfigError>,
    ) {
        // `config` must be `Some` for a reusable result; a read failure is
        // never cached as a match.
        debug_assert!(config.is_some() || result.is_err());
        let previous = state.as_ref().map(|loaded| loaded.result.as_ref().err());

        match (previous, &result) {
            // Same failure as last time: already logged.
            (Some(Some(prev)), Err(err)) if prev == err => {}
            (_, Err(err)) => tracing::error!(
                path = %self.path.display(),
                error = %err,
                "scope config is unusable; refusing every session until it is fixed"
            ),
            (Some(_), Ok(policies)) => tracing::info!(
                path = %self.path.display(),
                scopes = policies.scopes.len(),
                default_enforced = policies.default.is_some(),
                "reloaded scope config"
            ),
            // First successful load; `open` logs it.
            (None, Ok(_)) => {}
        }

        *state = Some(Loaded {
            config,
            key_files,
            result,
            checked_at: Instant::now(),
        });
    }
}

fn key_files_unchanged(key_files: &[(PathBuf, Option<u64>)]) -> bool {
    key_files.iter().all(|(path, previous)| {
        read_limited(path, MAX_KEY_FILE_BYTES)
            .ok()
            .map(|bytes| fingerprint(&bytes))
            == *previous
    })
}

fn fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Read a file, refusing anything larger than `limit` bytes.
fn read_limited(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("larger than {limit} bytes"),
        ));
    }
    Ok(bytes)
}

/// Resolve `--scope-config` into a source, refusing it if the relay cannot
/// enforce it.
///
/// Without the `auth-cat` feature every scope with a policy would deny all
/// sessions. That is safe, but an operator who asked for token
/// authorization and got a relay that refuses everyone would reasonably
/// read it as a bug, so the mismatch is a startup error instead.
pub fn from_cli(path: Option<&Path>) -> anyhow::Result<Option<Arc<ScopeConfigSource>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    if !cfg!(feature = "auth-cat") {
        anyhow::bail!(
            "--scope-config requires a relay built with the `auth-cat` feature \
             (cargo build -p moq-relay-ietf --features auth-cat); this binary cannot \
             verify Common Access Tokens"
        );
    }
    Ok(Some(Arc::new(ScopeConfigSource::open(path)?)))
}

/// Shared [`Coordinator::get_scope_config`] body for the binary's
/// coordinators.
///
/// [`Coordinator::get_scope_config`]: moq_relay_ietf::Coordinator::get_scope_config
pub async fn get_scope_config(
    source: Option<&Arc<ScopeConfigSource>>,
    scope: Option<&str>,
) -> CoordinatorResult<ScopeConfig> {
    match source {
        Some(source) => source.get(scope).await,
        // Exactly the trait's default: no flag, no change in behaviour.
        None => Ok(ScopeConfig::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

    /// A fresh directory per test, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "moq-scope-config-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn write(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn signing_key() -> SigningKey {
        SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng)
    }

    fn public_pem(key: &SigningKey) -> String {
        p256::ecdsa::VerifyingKey::from(key)
            .to_public_key_pem(Default::default())
            .unwrap()
    }

    fn pem() -> String {
        public_pem(&signing_key())
    }

    fn parse_str(json: &str) -> Result<ScopePolicies, ScopeConfigError> {
        parse(json.as_bytes(), Path::new("/nonexistent"), &mut |_| {
            Err(ScopeConfigError::new("no key files in this test"))
        })
    }

    fn auth_json(pem: &str) -> serde_json::Value {
        serde_json::json!({
            "keys": [{ "kid": "k1", "alg": "ES256", "pem": pem }],
            "issuers": ["https://issuer.example"],
            "audiences": ["my-relay"],
            "clock_skew_secs": 30,
            "ttl_secs": 10,
        })
    }

    fn expect_err(json: serde_json::Value, needle: &str) {
        let err = parse_str(&json.to_string()).expect_err("must be rejected");
        assert!(
            err.to_string().contains(needle),
            "error {err:?} should mention {needle:?}"
        );
    }

    // ------------------------------------------------------------------
    // Parsing and validation
    // ------------------------------------------------------------------

    #[test]
    fn parses_a_full_config() {
        let key = pem();
        let json = serde_json::json!({
            "default": { "auth": auth_json(&key) },
            "scopes": { "/open": { "auth": null }, "/also-open": {} },
        });
        let policies = parse_str(&json.to_string()).unwrap();

        let auth = policies.auth_for(None).expect("default enforced");
        assert_eq!(auth.keys.len(), 1);
        assert_eq!(auth.keys[0].kid.as_deref(), Some("k1"));
        assert_eq!(auth.keys[0].pem, key);
        assert_eq!(auth.issuers, vec!["https://issuer.example".to_string()]);
        assert_eq!(auth.audiences, vec!["my-relay".to_string()]);
        assert_eq!(auth.clock_skew, Duration::from_secs(30));
        assert_eq!(auth.ttl, Duration::from_secs(10));
    }

    #[test]
    fn optional_fields_take_library_defaults() {
        let json = serde_json::json!({
            "default": { "auth": { "keys": [{ "alg": "ES256", "pem": pem() }] } },
        });
        let auth = parse_str(&json.to_string())
            .unwrap()
            .auth_for(None)
            .unwrap();
        assert!(auth.keys[0].kid.is_none());
        assert!(auth.issuers.is_empty());
        assert!(auth.audiences.is_empty());
        assert_eq!(
            auth.clock_skew,
            moq_relay_ietf::DEFAULT_SCOPE_AUTH_CLOCK_SKEW
        );
        assert_eq!(auth.ttl, moq_relay_ietf::DEFAULT_SCOPE_AUTH_TTL);
    }

    #[test]
    fn an_empty_object_configures_nothing() {
        let policies = parse_str("{}").unwrap();
        assert!(policies.auth_for(None).is_none());
        assert!(policies.auth_for(Some("/x")).is_none());
    }

    #[test]
    fn rejects_malformed_json() {
        let err = parse_str("{ \"default\": ").unwrap_err();
        assert!(err.to_string().contains("malformed"), "{err}");
    }

    #[test]
    fn rejects_unknown_fields() {
        expect_err(serde_json::json!({ "defualt": {} }), "unknown field");
        let mut auth = auth_json(&pem());
        auth["audience"] = serde_json::json!("my-relay");
        expect_err(
            serde_json::json!({ "default": { "auth": auth } }),
            "unknown field",
        );
    }

    #[test]
    fn rejects_an_empty_key_list() {
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [] } } }),
            "auth.keys is empty",
        );
    }

    #[test]
    fn rejects_too_many_keys() {
        let keys: Vec<_> = (0..=MAX_SCOPE_AUTH_KEYS)
            .map(|_| serde_json::json!({ "alg": "ES256", "pem": pem() }))
            .collect();
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": keys } } }),
            "at most",
        );
    }

    #[test]
    fn rejects_duplicate_and_empty_kids() {
        let key = pem();
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "kid": "a", "alg": "ES256", "pem": key },
                { "kid": "a", "alg": "ES256", "pem": key },
            ] } } }),
            "duplicate kid",
        );
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "kid": "", "alg": "ES256", "pem": key },
            ] } } }),
            "empty kid",
        );
    }

    /// A long TTL is allowed (with a warning), not rejected.
    #[test]
    fn a_long_ttl_is_accepted() {
        let mut auth = auth_json(&pem());
        auth["ttl_secs"] = serde_json::json!(WARN_TTL_SECS * 24);
        let policies =
            parse_str(&serde_json::json!({ "default": { "auth": auth } }).to_string()).unwrap();
        assert_eq!(
            policies.auth_for(None).unwrap().ttl,
            Duration::from_secs(WARN_TTL_SECS * 24)
        );
    }

    /// `"auth": {}` is not an opt-out: an auth object needs keys. The empty
    /// scope entry `{}` is.
    #[test]
    fn an_empty_auth_object_is_rejected_but_an_empty_scope_entry_opts_out() {
        expect_err(
            serde_json::json!({ "scopes": { "/x": { "auth": {} } } }),
            "missing field `keys`",
        );
        let policies = parse_str(
            &serde_json::json!({
                "default": { "auth": auth_json(&pem()) },
                "scopes": { "/x": {} },
            })
            .to_string(),
        )
        .unwrap();
        assert!(policies.auth_for(Some("/x")).is_none());
    }

    #[test]
    fn rejects_unsupported_algorithms() {
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "RS256", "pem": pem() },
            ] } } }),
            "unsupported alg",
        );
    }

    #[test]
    fn requires_exactly_one_key_source() {
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [{ "alg": "ES256" }] } } }),
            "is required",
        );
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem": pem(), "pem_file": "k.pem" },
            ] } } }),
            "not both",
        );
    }

    #[test]
    fn rejects_empty_issuer_or_audience_strings() {
        let mut auth = auth_json(&pem());
        auth["issuers"] = serde_json::json!([""]);
        expect_err(
            serde_json::json!({ "default": { "auth": auth } }),
            "auth.issuers contains an empty string",
        );
        let mut auth = auth_json(&pem());
        auth["audiences"] = serde_json::json!([""]);
        expect_err(
            serde_json::json!({ "default": { "auth": auth } }),
            "auth.audiences contains an empty string",
        );
    }

    #[test]
    fn rejects_a_private_key_without_echoing_it() {
        let private = signing_key()
            .to_pkcs8_pem(Default::default())
            .unwrap()
            .to_string();
        let json = serde_json::json!({ "default": { "auth": { "keys": [
            { "alg": "ES256", "pem": private },
        ] } } });
        let err = parse_str(&json.to_string()).unwrap_err().to_string();
        assert!(err.contains("PRIVATE KEY"), "{err}");
        // The base64 body of the key must not appear in the message.
        let body: String = private.lines().nth(1).unwrap().to_string();
        assert!(!err.contains(&body), "error must not echo key material");
    }

    #[test]
    fn rejects_a_non_public_key_pem() {
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n" },
            ] } } }),
            "not a PEM PUBLIC KEY block",
        );
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem": format!("{}{}", pem(), pem()) },
            ] } } }),
            "more than one PEM block",
        );
    }

    /// With the feature the key is parsed at load time, so garbage inside a
    /// well-formed PEM armour is caught here, naming the key.
    #[cfg(feature = "auth-cat")]
    #[test]
    fn rejects_an_unparseable_public_key() {
        expect_err(
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem": "-----BEGIN PUBLIC KEY-----\nnope\n-----END PUBLIC KEY-----\n" },
            ] } } }),
            "default: key 0: is not a valid ES256",
        );
    }

    #[test]
    fn rejects_scope_keys_the_relay_would_never_look_up() {
        let open = serde_json::json!({ "auth": null });
        expect_err(
            serde_json::json!({ "scopes": { "demo": open } }),
            "not a valid connection path",
        );
        expect_err(
            serde_json::json!({ "scopes": { "/demo/": open } }),
            "normalized form",
        );
        expect_err(serde_json::json!({ "scopes": { "/": open } }), "unscoped");
        expect_err(
            serde_json::json!({ "scopes": { "/a/../b": open } }),
            "not a valid connection path",
        );
    }

    #[test]
    fn a_wrong_type_does_not_echo_the_value() {
        // `serde_json` would quote the string; the error must not.
        let err = parse_str(r#"{ "default": { "auth": { "keys": "SECRETISH" } } }"#)
            .unwrap_err()
            .to_string();
        assert!(!err.contains("SECRETISH"), "{err}");
    }

    #[test]
    fn one_bad_scope_fails_the_whole_file() {
        expect_err(
            serde_json::json!({
                "default": { "auth": auth_json(&pem()) },
                "scopes": { "/bad": { "auth": { "keys": [] } } },
            }),
            "scope \"/bad\"",
        );
    }

    // ------------------------------------------------------------------
    // Lookup
    // ------------------------------------------------------------------

    #[test]
    fn exact_scope_beats_default() {
        let default_key = pem();
        let scoped_key = pem();
        let json = serde_json::json!({
            "default": { "auth": auth_json(&default_key) },
            "scopes": {
                "/open": { "auth": null },
                "/tenant": { "auth": auth_json(&scoped_key) },
            },
        });
        let policies = parse_str(&json.to_string()).unwrap();

        assert!(
            policies.auth_for(Some("/open")).is_none(),
            "explicit null opts out"
        );
        assert_eq!(
            policies.auth_for(Some("/tenant")).unwrap().keys[0].pem,
            scoped_key
        );
        assert_eq!(
            policies.auth_for(Some("/other")).unwrap().keys[0].pem,
            default_key
        );
        assert_eq!(policies.auth_for(None).unwrap().keys[0].pem, default_key);
        // Matching is exact; there is no prefix inheritance.
        assert_eq!(
            policies.auth_for(Some("/tenant/sub")).unwrap().keys[0].pem,
            default_key
        );
    }

    #[test]
    fn without_a_default_unlisted_scopes_have_no_policy() {
        let json = serde_json::json!({
            "scopes": { "/locked": { "auth": auth_json(&pem()) } },
        });
        let policies = parse_str(&json.to_string()).unwrap();
        assert!(policies.auth_for(Some("/locked")).is_some());
        assert!(policies.auth_for(Some("/other")).is_none());
        assert!(policies.auth_for(None).is_none());
    }

    // ------------------------------------------------------------------
    // Files, reload and failing closed
    // ------------------------------------------------------------------

    /// Re-checks the files on every lookup, so edits are visible at once.
    fn source_without_throttle(path: &Path) -> ScopeConfigSource {
        ScopeConfigSource::open_with_interval(path, Duration::ZERO).unwrap()
    }

    fn write_config(dir: &TempDir, json: serde_json::Value) -> PathBuf {
        dir.write("scope-config.json", &json.to_string())
    }

    #[tokio::test]
    async fn pem_file_is_resolved_relative_to_the_config() {
        let dir = TempDir::new();
        let key = pem();
        dir.write("issuer.pub.pem", &key);
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem_file": "issuer.pub.pem" },
            ] } } }),
        );

        let source = Arc::new(source_without_throttle(&path));
        let config = source.get(Some("/x")).await.unwrap();
        assert_eq!(config.auth.unwrap().keys[0].pem, key);
    }

    #[tokio::test]
    async fn a_missing_pem_file_is_rejected() {
        let dir = TempDir::new();
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem_file": "absent.pem" },
            ] } } }),
        );
        let err = ScopeConfigSource::open(&path).err().unwrap().to_string();
        assert!(err.contains("cannot read pem_file"), "{err}");
    }

    #[tokio::test]
    async fn a_private_key_pem_file_is_rejected() {
        let dir = TempDir::new();
        dir.write(
            "issuer.key",
            &signing_key().to_pkcs8_pem(Default::default()).unwrap(),
        );
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem_file": "issuer.key" },
            ] } } }),
        );
        let err = ScopeConfigSource::open(&path).err().unwrap().to_string();
        assert!(err.contains("PRIVATE KEY"), "{err}");
    }

    #[tokio::test]
    async fn a_missing_file_fails_at_startup() {
        let dir = TempDir::new();
        let err = ScopeConfigSource::open(dir.0.join("absent.json"))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("cannot read file"), "{err}");
    }

    #[tokio::test]
    async fn edits_are_picked_up_without_a_restart() {
        let dir = TempDir::new();
        let path = write_config(&dir, serde_json::json!({}));
        let source = Arc::new(source_without_throttle(&path));
        assert!(source.get(Some("/demo")).await.unwrap().auth.is_none());

        let key = pem();
        write_config(
            &dir,
            serde_json::json!({ "scopes": { "/demo": { "auth": auth_json(&key) } } }),
        );
        let auth = source.get(Some("/demo")).await.unwrap().auth.unwrap();
        assert_eq!(auth.keys[0].pem, key);

        write_config(&dir, serde_json::json!({}));
        assert!(source.get(Some("/demo")).await.unwrap().auth.is_none());
    }

    /// Within the interval every scope shares one result and the disk is not
    /// touched; after it, the edit is seen.
    #[tokio::test]
    async fn lookups_within_the_interval_reuse_the_last_check() {
        let dir = TempDir::new();
        let path = write_config(&dir, serde_json::json!({}));
        let source = Arc::new(
            ScopeConfigSource::open_with_interval(&path, Duration::from_millis(200)).unwrap(),
        );

        write_config(
            &dir,
            serde_json::json!({ "default": { "auth": auth_json(&pem()) } }),
        );
        for scope in [Some("/a"), Some("/b"), None] {
            assert!(
                source.get(scope).await.unwrap().auth.is_none(),
                "inside the interval the previous result is reused"
            );
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(source.get(Some("/a")).await.unwrap().auth.is_some());
    }

    #[tokio::test]
    async fn editing_a_pem_file_is_picked_up() {
        let dir = TempDir::new();
        let first = pem();
        dir.write("k.pem", &first);
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": { "keys": [
                { "alg": "ES256", "pem_file": "k.pem" },
            ] } } }),
        );
        let source = Arc::new(source_without_throttle(&path));
        assert_eq!(
            source.get(None).await.unwrap().auth.unwrap().keys[0].pem,
            first
        );

        let second = pem();
        dir.write("k.pem", &second);
        assert_eq!(
            source.get(None).await.unwrap().auth.unwrap().keys[0].pem,
            second
        );
    }

    /// Each failure mode after a good start must produce an error, never an
    /// `auth: None` that would admit sessions without a token, and recovery
    /// must work once the file is fixed.
    #[tokio::test]
    async fn breakage_after_startup_fails_closed_and_recovers() {
        let dir = TempDir::new();
        let good = serde_json::json!({ "default": { "auth": auth_json(&pem()) } });
        let path = write_config(&dir, good.clone());
        let source = Arc::new(source_without_throttle(&path));
        assert!(source.get(Some("/demo")).await.unwrap().auth.is_some());

        let private = signing_key().to_pkcs8_pem(Default::default()).unwrap();
        let private = private.to_string();
        let private_key_config = serde_json::json!({ "default": { "auth": { "keys": [
            { "alg": "ES256", "pem": private },
        ] } } })
        .to_string();
        let no_keys_config =
            serde_json::json!({ "default": { "auth": { "keys": [] } } }).to_string();

        // `None` deletes the file.
        let breakages = [
            ("malformed", Some("{ not json")),
            ("private key", Some(private_key_config.as_str())),
            ("invalid", Some(no_keys_config.as_str())),
            ("missing", None),
        ];

        for (name, contents) in breakages {
            match contents {
                Some(contents) => {
                    dir.write("scope-config.json", contents);
                }
                None => std::fs::remove_file(&path).unwrap(),
            }
            for scope in [Some("/demo"), None] {
                let result = source.get(scope).await;
                assert!(result.is_err(), "{name}: must fail closed, got {result:?}");
            }
            write_config(&dir, good.clone());
            assert!(
                source.get(Some("/demo")).await.unwrap().auth.is_some(),
                "{name}: must recover once fixed"
            );
        }
    }

    /// The error that reaches the relay's logs names the file and the
    /// problem but carries no key material.
    #[tokio::test]
    async fn coordinator_errors_do_not_leak_key_material() {
        let dir = TempDir::new();
        let path = write_config(&dir, serde_json::json!({}));
        let source = Arc::new(source_without_throttle(&path));

        let private = signing_key().to_pkcs8_pem(Default::default()).unwrap();
        write_config(
            &dir,
            serde_json::json!({ "default": { "auth": { "keys": [
            { "alg": "ES256", "pem": private.to_string() },
        ] } } }),
        );
        let err = source.get(None).await.unwrap_err().to_string();
        assert!(err.contains("scope-config.json"), "{err}");
        let body = private.lines().nth(1).unwrap();
        assert!(!err.contains(body), "{err}");
    }

    // ------------------------------------------------------------------
    // CLI wiring
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn no_flag_means_no_policy() {
        assert!(from_cli(None).unwrap().is_none());
        for scope in [None, Some("/demo")] {
            assert!(get_scope_config(None, scope).await.unwrap().auth.is_none());
        }
    }

    #[cfg(not(feature = "auth-cat"))]
    #[test]
    fn the_flag_without_auth_cat_is_a_startup_error() {
        // Even a valid file is refused: the relay could not enforce it.
        let dir = TempDir::new();
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": auth_json(&pem()) } }),
        );
        let err = from_cli(Some(&path))
            .err()
            .expect("must refuse")
            .to_string();
        assert!(err.contains("auth-cat"), "{err}");
    }

    #[cfg(feature = "auth-cat")]
    #[test]
    fn the_flag_with_auth_cat_loads_the_file() {
        let dir = TempDir::new();
        let path = write_config(
            &dir,
            serde_json::json!({ "default": { "auth": auth_json(&pem()) } }),
        );
        assert!(from_cli(Some(&path)).unwrap().is_some());
        // A broken file is still a startup error with the feature.
        let bad = dir.write("bad.json", "{");
        assert!(from_cli(Some(&bad)).is_err());
    }

    // ------------------------------------------------------------------
    // End to end through the CAT hook
    // ------------------------------------------------------------------

    /// A file-loaded policy produces a hook that admits a token minted for
    /// it and rejects the same token for the wrong audience, using the same
    /// `CatAuthHook` the relay builds per scope.
    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn a_file_policy_admits_matching_tokens_and_rejects_the_wrong_audience() {
        use cat_token::{encode_token, CatToken, Es256Algorithm, MoqtAction, MoqtScopeBuilder};
        use moq_relay_ietf::{AuthHook, AuthToken, CatAuthHook, SessionContext, CAT_TOKEN_TYPE};

        let issuer = Es256Algorithm::new_with_key_pair().unwrap();
        let public = issuer
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();

        let dir = TempDir::new();
        dir.write("issuer.pub.pem", &public);
        let path = write_config(
            &dir,
            serde_json::json!({ "scopes": { "/demo": { "auth": {
                "keys": [{ "kid": "k1", "alg": "ES256", "pem_file": "issuer.pub.pem" }],
                "issuers": ["https://issuer.example"],
                "audiences": ["my-relay"],
            } } } }),
        );
        let source = Arc::new(source_without_throttle(&path));
        let auth = source.get(Some("/demo")).await.unwrap().auth.unwrap();
        let hook = CatAuthHook::new(&auth, Some("/demo")).unwrap();

        let token = |audience: &str| {
            let claims = CatToken::new()
                .with_issuer("https://issuer.example")
                .with_single_audience(audience)
                .with_expires_in(600)
                .with_moqt_scope(
                    MoqtScopeBuilder::new()
                        .action(MoqtAction::ClientSetup)
                        .build(),
                )
                .with_moqt_scope(
                    MoqtScopeBuilder::new()
                        .subscriber()
                        .namespace_prefix(b"clock")
                        .track_prefix(b"")
                        .build(),
                );
            AuthToken::new(CAT_TOKEN_TYPE, encode_token(&claims, &issuer).unwrap())
        };
        let session = SessionContext::public(Some("/demo".to_string()));

        let admitted = hook.on_setup(&session, &[token("my-relay")]).await.unwrap();
        assert!(admitted.is_allowed(), "{:?}", admitted.deny_reason());

        let rejected = hook
            .on_setup(&session, &[token("someone-else")])
            .await
            .unwrap();
        assert!(!rejected.is_allowed());

        let missing = hook.on_setup(&session, &[]).await.unwrap();
        assert!(!missing.is_allowed());
    }
}
