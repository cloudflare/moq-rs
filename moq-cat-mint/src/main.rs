// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! moq-cat-mint: mint Common Access Tokens (CAT, draft-ietf-moq-c4m) for
//! testing MoQ relays that enforce them, such as `moq-relay-ietf` built with
//! the `auth-cat` feature.
//!
//! Testing only. The signing key is a local file; never point this at a
//! production issuer key.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::Engine;
use cat_token::{encode_token, CatToken, Es256Algorithm, MoqtAction, MoqtScopeBuilder};
use clap::{Parser, Subcommand};
use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

#[derive(Parser)]
#[command(about = "Mint Common Access Tokens for testing MoQ relays")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate an ES256 (P-256) keypair. Writes the private key (PKCS#8,
    /// mode 0600) and the public key (SPKI). Give the relay only the public one.
    Keygen {
        #[arg(long, default_value = "issuer.key")]
        private: PathBuf,
        #[arg(long, default_value = "issuer.pub.pem")]
        public: PathBuf,
    },
    /// Mint a token and print it as base64url on stdout. Use with
    /// `moq-clock-ietf --auth-token 1:$(moq-cat-mint mint ...)`.
    Mint {
        /// Private key from `keygen`.
        #[arg(long, default_value = "issuer.key")]
        key: PathBuf,
        #[command(flatten)]
        claims: Claims,
    },
}

/// What the minted token says.
#[derive(clap::Args)]
struct Claims {
    /// `iss` claim.
    #[arg(long)]
    iss: String,
    /// `aud` claim.
    #[arg(long)]
    aud: String,
    /// `sub` claim (optional).
    #[arg(long)]
    sub: Option<String>,
    /// Lifetime in seconds. Relays may cap `exp`; moq-relay-ietf refuses
    /// tokens valid for more than 7 days.
    #[arg(long, default_value_t = 600)]
    ttl: i64,
    /// Namespace prefix the grants cover (one tuple field; repeat for more
    /// fields). Empty means every namespace.
    #[arg(long = "ns")]
    namespace: Vec<String>,
    /// Track-name prefix the grants cover. Empty means every track.
    #[arg(long, default_value = "")]
    track: String,
    /// Grant publisher actions (PublishNamespace 2, Publish 6).
    #[arg(long)]
    publish: bool,
    /// Grant subscriber actions (Subscribe 4, SubscribeNamespace 3,
    /// TrackStatus 8, Fetch 7).
    #[arg(long)]
    subscribe: bool,
    /// Grant explicit actions by number, instead of or in addition to the
    /// presets.
    #[arg(long = "action")]
    actions: Vec<u8>,
}

fn action(n: u8) -> Result<MoqtAction> {
    Ok(match n {
        0 => MoqtAction::ClientSetup,
        2 => MoqtAction::PublishNamespace,
        3 => MoqtAction::SubscribeNamespace,
        4 => MoqtAction::Subscribe,
        6 => MoqtAction::Publish,
        7 => MoqtAction::Fetch,
        8 => MoqtAction::TrackStatus,
        _ => bail!("unsupported moqt action {n}"),
    })
}

/// Build the token. Every token grants `ClientSetup` (0) on its own scope,
/// plus the requested actions scoped to the namespace and track prefixes.
fn build_token(claims: Claims) -> Result<CatToken> {
    let mut grant = Vec::new();
    if claims.publish {
        grant.extend([MoqtAction::PublishNamespace, MoqtAction::Publish]);
    }
    if claims.subscribe {
        grant.extend([
            MoqtAction::Subscribe,
            MoqtAction::SubscribeNamespace,
            MoqtAction::TrackStatus,
            MoqtAction::Fetch,
        ]);
    }
    for n in claims.actions {
        grant.push(action(n)?);
    }
    if grant.is_empty() {
        bail!("nothing granted beyond ClientSetup; pass --publish, --subscribe or --action");
    }

    let mut scope = MoqtScopeBuilder::new().actions(&grant);
    for field in &claims.namespace {
        scope = scope.namespace_prefix(field.as_bytes());
    }
    let scope = scope.track_prefix(claims.track.as_bytes()).build();

    let mut token = CatToken::new()
        .with_issuer(claims.iss)
        .with_single_audience(claims.aud)
        .with_expires_in(claims.ttl)
        .with_moqt_scope(
            MoqtScopeBuilder::new()
                .action(MoqtAction::ClientSetup)
                .build(),
        )
        .with_moqt_scope(scope);
    if let Some(sub) = claims.sub {
        token = token.with_subject(sub);
    }
    Ok(token)
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Keygen { private, public } => {
            // Check both up front so a clash on the second cannot leave an
            // orphaned first file behind.
            for path in [&private, &public] {
                if path.exists() {
                    bail!("{} already exists (refusing to overwrite)", path.display());
                }
            }
            let sk = p256::ecdsa::SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
            let pem = sk.to_pkcs8_pem(Default::default())?;
            let public_pem =
                p256::ecdsa::VerifyingKey::from(&sk).to_public_key_pem(Default::default())?;
            write_new(&private, pem.as_bytes(), true)?;
            write_new(&public, public_pem.as_bytes(), false)?;
            eprintln!(
                "wrote {} (keep private) and {}",
                private.display(),
                public.display()
            );
        }
        Command::Mint { key, claims } => {
            let pem = std::fs::read_to_string(&key)
                .with_context(|| format!("reading {}", key.display()))?;
            let signer = Es256Algorithm::from_private_key_pem(&pem)?;
            let bytes = encode_token(&build_token(claims)?, &signer)?;
            println!(
                "{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
            );
        }
    }
    Ok(())
}

/// Write a key file without clobbering an existing one. `private` restricts
/// it to the owner (mode 0600) on Unix.
fn write_new(path: &Path, bytes: &[u8], private: bool) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut f = options
        .open(path)
        .with_context(|| format!("creating {} (refusing to overwrite)", path.display()))?;
    f.write_all(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cat_token::Decoder;

    fn claims(args: &[&str]) -> Claims {
        #[derive(Parser)]
        struct Wrapper {
            #[command(flatten)]
            claims: Claims,
        }
        let mut argv = vec!["test", "--iss", "https://issuer.example", "--aud", "relay"];
        argv.extend_from_slice(args);
        Wrapper::try_parse_from(argv).unwrap().claims
    }

    fn keypair() -> (Es256Algorithm, Es256Algorithm) {
        let sk = p256::ecdsa::SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        let private = sk.to_pkcs8_pem(Default::default()).unwrap();
        let public = p256::ecdsa::VerifyingKey::from(&sk)
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            Es256Algorithm::from_private_key_pem(&private).unwrap(),
            Es256Algorithm::from_public_key_pem(&public).unwrap(),
        )
    }

    #[test]
    fn minted_token_verifies_and_carries_grants() {
        let (signer, verifier) = keypair();
        let token = build_token(claims(&["--ns", "clock", "--publish"])).unwrap();
        let bytes = encode_token(&token, &signer).unwrap();

        let verified = Decoder::with_algorithm(&verifier).decode(&bytes).unwrap();
        let token = verified.claims();
        assert_eq!(token.core.iss.as_deref(), Some("https://issuer.example"));
        assert!(token.core.exp.is_some());

        let scopes = token.moqt.moqt.as_ref().unwrap();
        let allows = |action: MoqtAction, ns: &[u8]| {
            scopes
                .iter()
                .any(|s| s.allows_action(&action) && s.matches_namespace(&[ns.to_vec()]))
        };
        assert!(scopes
            .iter()
            .any(|s| s.allows_action(&MoqtAction::ClientSetup)));
        assert!(allows(MoqtAction::PublishNamespace, b"clock"));
        assert!(allows(MoqtAction::Publish, b"clock"));
        assert!(!allows(MoqtAction::Subscribe, b"clock"));
        assert!(!allows(MoqtAction::Publish, b"other"));
    }

    #[test]
    fn token_does_not_verify_under_another_key() {
        let (signer, _) = keypair();
        let (_, other) = keypair();
        let token = build_token(claims(&["--subscribe"])).unwrap();
        let bytes = encode_token(&token, &signer).unwrap();
        assert!(Decoder::with_algorithm(&other).decode(&bytes).is_err());
    }

    #[test]
    fn refuses_empty_grant_and_unknown_action() {
        assert!(build_token(claims(&[])).is_err());
        assert!(build_token(claims(&["--action", "5"])).is_err());
    }
}
