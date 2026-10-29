// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{future::Future, net, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_native_ietf::quic::{self, Endpoint};
use moq_transport::session::transport as web_transport;
use moq_transport::session::SessionConfig;
use url::Url;

use crate::auth::{decode_setup_tokens, ScopeAuthorizer, SessionAuth, SESSION_ERROR_UNAUTHORIZED};
use crate::upstream_namespaces::{UpstreamNamespaces, UpstreamNamespacesRunner};
use crate::{
    metrics::GaugeGuard, ConnectionMeta, ConnectionTagger, Consumer, Coordinator, Locals, Producer,
    RelayInfo, RemoteManager, Session, SessionContext, SessionInterface,
    DEFAULT_CACHE_IDLE_TIMEOUT,
};

// A type alias for boxed future
type ServerFuture = Pin<
    Box<
        dyn Future<
            Output = (
                anyhow::Result<(web_transport::Session, quic::ConnInfo)>,
                quic::Server,
            ),
        >,
    >,
>;

/// Configuration for the relay.
pub struct RelayConfig {
    /// Listen on this address
    pub bind: Option<net::SocketAddr>,

    /// Optional list of endpoints if provided, we won't use bind
    pub endpoints: Vec<Endpoint>,

    /// The TLS configuration.
    pub tls: moq_native_ietf::tls::Config,

    /// Directory to write qlog files (one per connection)
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    pub mlog_dir: Option<PathBuf>,

    /// Forward all PUBLISH_NAMESPACE messages to the (optional) upstream URL.
    pub announce: Option<Url>,

    /// Our hostname which we advertise to other origins.
    /// We use QUIC, so the certificate must be valid for this address.
    pub node: Option<Url>,

    /// The coordinator for namespace/track registration and discovery.
    pub coordinator: Arc<dyn Coordinator>,

    /// MoQT session configuration used for inbound and relay-to-relay sessions.
    pub session: SessionConfig,

    /// Classifies inbound connections as public clients or internal relay
    /// peers via connection tags (the well-known `interface` tag). Consulted
    /// once per accepted connection with the peer's socket address and
    /// connection path.
    ///
    /// When `None`, every inbound connection is treated as a public client.
    /// Outbound connections the relay dials itself (`--announce`,
    /// [`RemoteManager`]) are always tagged internal and bypass this.
    pub connection_tagger: Option<Arc<dyn ConnectionTagger>>,
}

impl RelayConfig {
    /// Build a relay from this configuration.
    pub fn build(self) -> anyhow::Result<Relay> {
        Relay::new(self)
    }

    /// Build a relay with a custom pull-through cache idle timeout.
    ///
    /// See [`Relay::new_with_cache_idle_timeout`]. This is a separate
    /// constructor rather than a `RelayConfig` field so that adding it does not
    /// break existing struct-literal construction of [`RelayConfig`].
    pub fn build_with_cache_idle_timeout(
        self,
        cache_idle_timeout: Duration,
    ) -> anyhow::Result<Relay> {
        Relay::new_with_cache_idle_timeout(self, cache_idle_timeout)
    }
}

/// Why a session was refused at setup.
///
/// Carries the QUIC application close code and a fixed reason phrase. The
/// phrase is deliberately coarse: the peer learns that it was refused, not why
/// validation failed.
#[derive(Debug)]
struct SessionRejection {
    code: u32,
    reason: &'static str,
}

/// Authorize an accepted session against its scope's policy.
///
/// Returns the session's token authorization state, or `None` for internal
/// relay peers and scopes without a policy. Token decoding errors refuse every
/// session; policy lookup and authorization failures refuse public sessions.
///
/// Takes the peer's SETUP options rather than the session so the admission
/// decision is unit-testable without a live transport.
async fn authorize_session(
    authorizer: &ScopeAuthorizer,
    context: &SessionContext,
    setup_params: &moq_transport::coding::KeyValuePairs,
) -> Result<Option<SessionAuth>, SessionRejection> {
    // Decode before resolving the scope's policy. Draft-18 §10.2.2 makes
    // rejecting a malformed Token structure, or an alias directive in SETUP,
    // an unconditional obligation on the receiver -- it is not predicated on
    // the receiver requiring authorization. Doing this after the policy lookup
    // would skip both MUSTs for every scope that has no policy, which is the
    // default.
    let tokens = match decode_setup_tokens(setup_params) {
        Ok(tokens) => tokens,
        Err(err) => {
            // A wire-format violation, so the close code comes from the draft
            // rather than being UNAUTHORIZED.
            tracing::debug!(
                scope = context.scope(),
                error = %err,
                "invalid AUTHORIZATION TOKEN option in SETUP"
            );
            metrics::counter!(
                "moq_relay_auth_denied_total",
                "phase" => "setup",
                "operation" => "client_setup",
                "reason" => err.metric_label(),
            )
            .increment(1);
            return Err(SessionRejection {
                code: err.session_error_code(),
                reason: err.reason_phrase(),
            });
        }
    };

    // Embedders must tag only authenticated relay peers as Internal: these
    // sessions bypass token authorization at setup and on later requests.
    if context.interface == SessionInterface::Internal {
        return Ok(None);
    }

    let hook = match authorizer.resolve(context.scope()).await {
        Ok(Some(hook)) => hook,
        // The scope did not opt in to token authorization.
        Ok(None) => return Ok(None),
        Err(err) => {
            // We could not learn whether this scope requires a token, so we
            // must not assume it does not.
            tracing::error!(
                scope = context.scope(),
                error = %err,
                "could not determine the scope's authorization policy"
            );
            metrics::counter!("moq_relay_auth_errors_total", "stage" => "scope_config")
                .increment(1);
            return Err(SessionRejection {
                code: SESSION_ERROR_UNAUTHORIZED,
                reason: "authorization unavailable",
            });
        }
    };

    let decision = hook.on_setup(context, &tokens).await.map_err(|err| {
        tracing::error!(
            scope = context.scope(),
            error = %err,
            "authorization hook failed during setup"
        );
        metrics::counter!("moq_relay_auth_errors_total", "stage" => "setup").increment(1);
        SessionRejection {
            code: SESSION_ERROR_UNAUTHORIZED,
            reason: "authorization unavailable",
        }
    })?;

    match decision.into_principal() {
        Ok(principal) => {
            tracing::debug!(
                scope = context.scope(),
                subject = principal.subject(),
                "session authorized"
            );
            Ok(Some(SessionAuth::new(hook, principal)))
        }
        Err(reason) => {
            tracing::debug!(
                scope = context.scope(),
                reason = %reason,
                "session authorization denied"
            );
            metrics::counter!(
                "moq_relay_auth_denied_total",
                "phase" => "setup",
                "operation" => "client_setup",
                "reason" => reason.label(),
            )
            .increment(1);
            Err(SessionRejection {
                code: SESSION_ERROR_UNAUTHORIZED,
                reason: "unauthorized",
            })
        }
    }
}

/// The scope identity used for this session's coordinator calls and media
/// lookups.
///
/// An unscoped session and one whose `scope_id` is empty are the same thing,
/// and are normalized to `None` here so that they stay the same thing
/// everywhere downstream. [`Locals`] already collapses the two into one media
/// bucket (it keys unscoped sessions under `""`), but [`ScopeAuthorizer`]
/// caches by `Option<String>`, where `None` and `Some("")` are distinct keys.
/// Left unnormalized, a coordinator that returned an empty `scope_id` could
/// have its authorization policy looked up under one key while its media
/// landed in the other's bucket — an unauthenticated session sharing tracks
/// with an enforced one.
///
/// Not reachable through the in-tree coordinators, whose `scope_id` comes from
/// a connection path that [`Session::normalize_connection_path`] never yields
/// empty. It is reachable through a custom [`Coordinator`], which is reason
/// enough to close it here rather than rely on every implementation not to.
///
/// [`Session::normalize_connection_path`]: moq_transport::session::Session::normalize_connection_path
fn normalize_scope_id(scope_info: Option<&crate::ScopeInfo>) -> Option<String> {
    scope_info
        .map(|info| info.scope_id.clone())
        .filter(|scope_id| !scope_id.is_empty())
}

/// MoQ Relay server.
pub struct Relay {
    config: RelayConfig,
    locals: Locals,
    remotes: RemoteManager,
    upstream_namespaces: UpstreamNamespaces,
    upstream_namespaces_runner: UpstreamNamespacesRunner,

    /// Per-scope authorization policy, resolved from the coordinator and
    /// cached. Shared by every accepted connection.
    authorizer: Arc<ScopeAuthorizer>,
}

impl Relay {
    pub fn new(config: RelayConfig) -> anyhow::Result<Self> {
        Self::new_with_cache_idle_timeout(config, DEFAULT_CACHE_IDLE_TIMEOUT)
    }

    /// Create a relay that releases upstream subscriptions for cached tracks that
    /// have had no downstream subscribers for `cache_idle_timeout`.
    ///
    /// The relay caches tracks it does not publish itself so multiple downstream
    /// subscribers can share one upstream subscription. Without a timeout that
    /// subscription outlives the last subscriber and the relay keeps receiving a
    /// track nobody is watching. A zero timeout restores that behaviour.
    pub fn new_with_cache_idle_timeout(
        mut config: RelayConfig,
        cache_idle_timeout: Duration,
    ) -> anyhow::Result<Self> {
        if config.bind.is_some() && !config.endpoints.is_empty() {
            anyhow::bail!("cannot specify both bind and endpoints");
        }

        if let Some(bind) = config.bind.take() {
            let endpoint = quic::Endpoint::new(quic::Config::new(
                bind,
                config.qlog_dir.clone(),
                config.tls.clone(),
            )?)?;
            config.endpoints = vec![endpoint];
        }

        if config.endpoints.is_empty() {
            anyhow::bail!("no endpoints available to start the server");
        }

        // Validate mlog directory if provided
        if let Some(mlog_dir) = &config.mlog_dir {
            if !mlog_dir.exists() {
                anyhow::bail!("mlog directory does not exist: {}", mlog_dir.display());
            }
            if !mlog_dir.is_dir() {
                anyhow::bail!("mlog path is not a directory: {}", mlog_dir.display());
            }
            tracing::info!("mlog output enabled: {}", mlog_dir.display());
        }

        let locals = Locals::with_cache_idle_timeout(cache_idle_timeout);

        // FIXME(itzmanish): have a generic filter to find endpoints for forward, remote etc.
        let remote_clients = config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.client.clone())
            .collect::<Vec<_>>();

        // Create remote manager - uses coordinator for namespace lookups
        let remotes = RemoteManager::new_with_session_config(
            config.coordinator.clone(),
            remote_clients,
            config.session,
        )
        .with_cache_idle_timeout(cache_idle_timeout);
        let (upstream_namespaces, upstream_namespaces_runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), config.coordinator.clone());
        let authorizer = Arc::new(ScopeAuthorizer::new(config.coordinator.clone()));

        Ok(Self {
            config,
            locals,
            remotes,
            upstream_namespaces,
            upstream_namespaces_runner,
            authorizer,
        })
    }

    /// Run the relay server.
    pub async fn run(self) -> anyhow::Result<()> {
        let Self {
            config,
            locals,
            remotes,
            upstream_namespaces,
            upstream_namespaces_runner,
            authorizer,
        } = self;

        let RelayConfig {
            endpoints: quic_endpoints,
            announce: announce_url,
            mlog_dir,
            coordinator,
            session: session_config,
            connection_tagger,
            ..
        } = config;

        let run_result = async {
            let mut tasks = FuturesUnordered::new();
            tasks.push(
                async move {
                    upstream_namespaces_runner.run().await;
                    Ok::<(), anyhow::Error>(())
                }
                .boxed(),
            );

            // Use the remote manager for routing to remote relays.
            let remote_manager = remotes.clone();

            // Start the forwarder, if any
            let forward_producer = if let Some(url) = &announce_url {
                tracing::info!("forwarding PUBLISH_NAMESPACE messages to {}", url);

                // Establish a QUIC connection to the forward URL
                let (session, _quic_client_initial_cid, transport) = quic_endpoints[0]
                    .client
                    .connect(url, None)
                    .await
                    .context("failed to establish forward connection")?;

                // Create the MoQ session over the connection
                let (session, publisher, subscriber) = moq_transport::session::Session::connect_with_config(
                    session,
                    None,
                    transport,
                    session_config,
                )
                .await
                .context("failed to establish forward session")?;

                // Use the connection path already validated and stored by Session::connect().
                // The forward session is scoped to whatever path the announce URL specifies.
                //
                // Note: the forward connection intentionally does not call
                // coordinator.resolve_scope(). The announce URL is operator-configured
                // (via --announce), not client-supplied, so it doesn't need the same
                // auth/permission checks that incoming client connections get. The
                // forward session always gets both Producer and Consumer (full
                // read-write) since it's acting as a relay peer, not a client.
                //
                // Limitation: all incoming scopes are forwarded to this single upstream scope.
                // Multi-scope forwarding (routing different incoming scopes to different
                // upstream paths) would require per-scope forward connections.
                let forward_scope = session.connection_path().map(|s| s.to_string());
                // The forward link is bidirectional: the upstream can publish
                // and subscribe back into this scope over it. Token
                // authorization is not applied — the relay has no credential
                // of its own to present, and the upstream is operator-chosen —
                // so if the scope does require tokens, say so loudly rather
                // than leaving an unauthenticated path into it undocumented at
                // runtime.
                match coordinator.get_scope_config(forward_scope.as_deref()).await {
                    Ok(config) if config.auth.is_some() => {
                        tracing::warn!(
                            url = %url,
                            scope = forward_scope.as_deref(),
                            "--announce targets a scope that requires token authorization, but \
                             the forward link is not authorized: the upstream relay can publish \
                             and subscribe in this scope without presenting a token. Ensure the \
                             announce target is trusted and reachable only over a trusted path."
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        // Not fatal — the link is operator-configured either
                        // way — but the warning above is the only signal that
                        // this path exists, so its absence must not be silent.
                        tracing::warn!(
                            url = %url,
                            scope = forward_scope.as_deref(),
                            error = %err,
                            "could not determine whether the --announce target's scope requires \
                             token authorization; the forward link is unauthorized regardless"
                        );
                    }
                }

                let forward_context = SessionContext::internal(
                    forward_scope,
                    Some(RelayInfo::new(url.clone())),
                );

                let forward_coordinator = coordinator.clone();
                let session = Session {
                    session,
                    producer: Some(Producer::new_with_upstream_namespaces(
                        publisher,
                        locals.clone(),
                        remote_manager.clone(),
                        upstream_namespaces.clone(),
                        forward_context.clone(),
                        // Relay-initiated and operator-configured: no token.
                        None,
                    )),
                    consumer: Some(Consumer::new(
                        subscriber,
                        locals.clone(),
                        forward_coordinator,
                        remote_manager.clone(),
                        None,
                        forward_context,
                        None,
                    )),
                    // Forward connections are always full read-write relay peers,
                    // so no reject loops needed.
                    reject_publishes: None,
                    reject_subscribes: None,
                };

                let forward_producer = session.producer.clone();

                tasks.push(async move { session.run().await.context("forwarding failed") }.boxed());

                forward_producer
            } else {
                None
            };

            let servers: Vec<quic::Server> = quic_endpoints
                .into_iter()
                .map(|endpoint| endpoint.server.context("missing TLS certificate for server"))
                .collect::<anyhow::Result<_>>()?;

            // This will hold the futures for all our listening servers.
            let mut accepts: FuturesUnordered<ServerFuture> = FuturesUnordered::new();
            for mut server in servers {
                tracing::info!("listening on {}", server.local_addr()?);

                // Create a future, box it, and push it to the collection.
                accepts.push(
                    async move {
                        let conn = server.accept().await.context("accept failed");
                        (conn, server)
                    }
                    .boxed(),
                );
            }

            loop {
                tokio::select! {
                    // This branch polls all the `accept` futures concurrently.
                    Some((conn_result, mut server)) = accepts.next() => {
                        // An accept operation has completed.
                        // First, immediately queue up the next accept() call for this server.
                        accepts.push(
                            async move {
                                let conn = server.accept().await.context("accept failed");
                                (conn, server)
                            }
                            .boxed(),
                        );

                        let (conn, info) = conn_result.context("failed to accept QUIC connection")?;
                        let quic::ConnInfo {
                            id: connection_id,
                            transport,
                            remote_address: remote_addr,
                            // The local IP the connection was accepted on
                            // (destination IP the peer targeted); forwarded to the
                            // connection tagger for inbound-interface classification.
                            local_ip,
                            server_name,
                        } = info;

                        metrics::counter!("moq_relay_connections_total").increment(1);

                        // Construct mlog path from connection ID if mlog directory is configured
                        let mlog_path = mlog_dir.as_ref()
                            .map(|dir| dir.join(format!("{}_server.mlog", connection_id)));

                        let locals = locals.clone();
                        let remotes = remote_manager.clone();
                        let forward = forward_producer.clone();
                        let coordinator = coordinator.clone();
                        let upstream_namespaces = upstream_namespaces.clone();
                        let connection_tagger = connection_tagger.clone();
                        let authorizer = authorizer.clone();

                        // Spawn a new task to handle the connection
                        tasks.push(async move {
                            // Track active connections - decrements when task completes
                            let _conn_guard = GaugeGuard::new("moq_relay_active_connections");

                            // Clone the raw connection so we can close it with a proper
                            // error code if scope resolution fails after the MoQ handshake.
                            let raw_conn = conn.clone();

                            // Create the MoQ session over the connection (setup handshake etc)
                            let (session, publisher, subscriber) = match moq_transport::session::Session::accept_with_config(conn, mlog_path, transport, session_config).await {
                                Ok(session) => session,
                                Err(err) => {
                                    tracing::warn!(error = %err, "failed to accept MoQ session: {}", err);
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_accept").increment(1);
                                    // Maintain invariant: connections_total - connections_closed_total == active_connections
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                    return Ok(());
                                }
                            };

                            // Create our MoQ relay session
                            let moq_session = session;

                            // Resolve the connection path to a scope (identity + permissions).
                            // This translates the raw transport-level path into an application-level
                            // scope_id and determines what the connection is allowed to do.
                            let scope_info = match coordinator.resolve_scope(moq_session.connection_path()).await {
                                Ok(info) => info,
                                Err(err) => {
                                    tracing::warn!(
                                        connection_path = moq_session.connection_path(),
                                        error = %err,
                                        "scope resolution failed, rejecting session"
                                    );
                                    // Close with PROTOCOL_VIOLATION (0x3) so the client
                                    // gets a meaningful error instead of an abrupt reset.
                                    // This is a QUIC APPLICATION_CLOSE, not a MoQT SESSION_CLOSE
                                    // control message. Sending a proper SESSION_CLOSE would require
                                    // running the MoQ session's send loop, which is not warranted
                                    // for a pre-session rejection. The QUIC close code and reason
                                    // string are visible to the client's transport layer.
                                    raw_conn.close(0x3, "scope resolution failed");
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "scope_resolve").increment(1);
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                    return Ok(());
                                }
                            };

                            let can_publish = scope_info.as_ref().is_none_or(|s| s.permissions.can_publish());
                            let can_subscribe = scope_info.as_ref().is_none_or(|s| s.permissions.can_subscribe());

                            // Classify the connection interface (public client vs internal
                            // relay peer). This is deliberately separate from scope
                            // resolution above: resolve_scope() returns identity +
                            // permissions, while the embedder-supplied tagger decides the
                            // transport interface from the peer socket address, TLS SNI, and
                            // connection path. With no tagger configured every inbound
                            // connection is treated as a public client. For connections
                            // classified internal, the peer relay identity is derived from
                            // the inbound socket address (see RelayInfo::from_socket_addr).
                            let scope = normalize_scope_id(scope_info.as_ref());
                            let context = match connection_tagger.as_ref() {
                                Some(tagger) => {
                                    let meta = ConnectionMeta::new(
                                        Some(remote_addr),
                                        server_name,
                                        moq_session.connection_path().map(str::to_string),
                                    )
                                    .with_local_ip(local_ip);
                                    let tags = tagger.tag(&meta);
                                    SessionContext::from_tags(scope, &tags, Some(remote_addr))
                                }
                                None => SessionContext::public(scope),
                            };

                            // The resolved interface controls whether an inbound
                            // PUBLISH_NAMESPACE is owned here or advertised only
                            // for peer discovery. Internal sessions are rare and
                            // logged at info; public client sessions stay at
                            // debug to avoid a per-connection info log.
                            macro_rules! log_session_accepted {
                                ($level:ident) => {
                                    tracing::$level!(
                                    interface = ?context.interface,
                                    local_ip = ?local_ip,
                                    remote_addr = %remote_addr,
                                    tagger = connection_tagger.is_some(),
                                    "session accepted"
                                    )
                                };
                            }
                            match context.interface {
                                SessionInterface::Internal => {
                                    log_session_accepted!(info)
                                }
                                SessionInterface::Public => {
                                    log_session_accepted!(debug)
                                }
                            }

                            if let Some(ref info) = scope_info {
                                tracing::debug!(
                                    connection_path = moq_session.connection_path(),
                                    scope_id = %info.scope_id,
                                    permissions = ?info.permissions,
                                    "scope resolved"
                                );
                            }

                            // Authorize the session, if this scope requires it.
                            //
                            // resolve_scope() established *which* scope this
                            // connection belongs to and its coarse permissions;
                            // this establishes *who* the peer is, from the
                            // AUTHORIZATION TOKEN in the client's SETUP. It
                            // runs before either session half is built so that
                            // an unauthorized peer never gets one.
                            let session_auth = match authorize_session(
                                &authorizer,
                                &context,
                                moq_session.setup_params(),
                            ).await {
                                Ok(auth) => auth,
                                Err(rejection) => {
                                    tracing::info!(
                                        connection_path = moq_session.connection_path(),
                                        scope = context.scope(),
                                        code = rejection.code,
                                        reason = rejection.reason,
                                        "session authorization failed, closing"
                                    );
                                    // As with scope resolution above this is a
                                    // QUIC APPLICATION_CLOSE rather than a
                                    // MoQT-level close; the code is the
                                    // draft-18 §15.10.1 session error code.
                                    raw_conn.close(rejection.code, rejection.reason);
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "auth_setup").increment(1);
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                    return Ok(());
                                }
                            };

                            // Gate Producer/Consumer creation on permissions.
                            // Note the intentional inversion:
                            // - Producer serves SUBSCRIBEs → gated on can_subscribe
                            // - Consumer handles PUBLISH_NAMESPACEs → gated on can_publish
                            //
                            // When a half is disabled, we pass its transport counterpart
                            // to the Session's reject fields so unauthorized messages get
                            // an explicit error response instead of being silently ignored.
                            let (producer, reject_subscribes) = if can_subscribe {
                                (publisher.map(|publisher| Producer::new_with_upstream_namespaces(publisher, locals.clone(), remotes.clone(), upstream_namespaces, context.clone(), session_auth.clone())), None)
                            } else {
                                (None, publisher)
                            };

                            let (consumer, reject_publishes) = if can_publish {
                                (subscriber.map(|subscriber| Consumer::new(subscriber, locals, coordinator, remotes.clone(), forward, context, session_auth)), None)
                            } else {
                                (None, subscriber)
                            };

                            let session = Session {
                                session: moq_session,
                                producer,
                                consumer,
                                reject_publishes,
                                reject_subscribes,
                            };

                            match session.run().await {
                                Ok(()) => {
                                    // Session ended cleanly (uncommon - usually ends via close)
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                                Err(err) if err.is_graceful_close() => {
                                    // Graceful close - peer sent APPLICATION_CLOSE with code 0
                                    tracing::debug!("MoQ session closed gracefully");
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                                Err(err) => {
                                    // Actual error - protocol violation, timeout, etc.
                                    tracing::warn!(error = %err, "MoQ session error: {}", err);
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_run").increment(1);
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                            }

                            Ok(())
                        }.boxed());
                    },
                    res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                }
            }
        }
        .await;

        remotes.shutdown().await;
        run_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{ScopeInfo, ScopePermissions};

    fn scope_info(scope_id: &str) -> ScopeInfo {
        ScopeInfo {
            scope_id: scope_id.to_string(),
            permissions: ScopePermissions::ReadWrite,
        }
    }

    #[test]
    fn a_named_scope_is_preserved() {
        assert_eq!(
            normalize_scope_id(Some(&scope_info("tenant"))),
            Some("tenant".to_string())
        );
        // Including shapes that merely look empty.
        assert_eq!(
            normalize_scope_id(Some(&scope_info("/"))),
            Some("/".to_string())
        );
        assert_eq!(
            normalize_scope_id(Some(&scope_info(" "))),
            Some(" ".to_string())
        );
    }

    #[test]
    fn an_unscoped_session_stays_unscoped() {
        assert_eq!(normalize_scope_id(None), None);
    }

    /// The case this exists for: `Some("")` and `None` must not be distinct
    /// keys, or a scope's authorization policy and its media could be looked
    /// up under different ones.
    #[test]
    fn an_empty_scope_id_is_normalized_to_unscoped() {
        assert_eq!(normalize_scope_id(Some(&scope_info(""))), None);
        assert_eq!(
            normalize_scope_id(Some(&scope_info(""))),
            normalize_scope_id(None),
            "an empty scope id and no scope must agree"
        );
    }

    /// `Locals` keys unscoped media under `""`, so normalizing to `None` here
    /// is what keeps the authorization key and the media key in agreement.
    #[test]
    fn normalization_agrees_with_the_locals_bucket() {
        let normalized = normalize_scope_id(Some(&scope_info("")));
        assert_eq!(normalized.as_deref().unwrap_or(""), "");
        assert_eq!(
            normalize_scope_id(None).as_deref().unwrap_or(""),
            normalized.as_deref().unwrap_or(""),
        );
    }

    // -----------------------------------------------------------------------
    // Session admission
    // -----------------------------------------------------------------------

    mod admission {
        use super::*;

        use std::sync::atomic::{AtomicBool, Ordering};

        use async_trait::async_trait;
        use moq_transport::coding::{
            Encode, KeyValuePair, KeyValuePairs, TrackName, TrackNamespace, TrackNamespacePrefix,
            VarInt,
        };
        use moq_transport::setup::ParameterType;
        use p256::pkcs8::EncodePublicKey;

        use crate::auth::{authorize, AuthzOperation};
        use crate::ConnectionTags;
        use crate::{
            AuthPublicKey, CoordinatorContext, CoordinatorError, CoordinatorResult,
            NamespaceOrigin, NamespaceRegistration, ScopeAuthConfig, ScopeConfig,
        };

        /// Serves a fixed scope configuration, or fails every lookup.
        struct FixedConfig {
            config: ScopeConfig,
            fail: AtomicBool,
        }

        impl FixedConfig {
            fn new(config: ScopeConfig) -> Arc<Self> {
                Arc::new(Self {
                    config,
                    fail: AtomicBool::new(false),
                })
            }

            fn failing() -> Arc<Self> {
                let this = Self::new(ScopeConfig::default());
                this.fail.store(true, Ordering::SeqCst);
                this
            }
        }

        #[async_trait]
        impl Coordinator for FixedConfig {
            async fn register_namespace(
                &self,
                _scope: Option<&str>,
                _namespace: &TrackNamespace,
                _context: &CoordinatorContext,
            ) -> CoordinatorResult<NamespaceRegistration> {
                Ok(NamespaceRegistration::new(()))
            }

            async fn unregister_namespace(
                &self,
                _scope: Option<&str>,
                _namespace: &TrackNamespace,
            ) -> CoordinatorResult<()> {
                Ok(())
            }

            async fn lookup(
                &self,
                _scope: Option<&str>,
                _namespace: &TrackNamespace,
            ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
                Err(CoordinatorError::NamespaceNotFound)
            }

            async fn get_scope_config(
                &self,
                _scope: Option<&str>,
            ) -> CoordinatorResult<ScopeConfig> {
                if self.fail.load(Ordering::SeqCst) {
                    return Err(CoordinatorError::Other(anyhow::anyhow!("backend down")));
                }
                Ok(self.config.clone())
            }
        }

        fn enforced() -> ScopeConfig {
            ScopeConfig {
                auth: Some(ScopeAuthConfig::new(vec![AuthPublicKey::es256(
                    "-----BEGIN PUBLIC KEY-----\nplaceholder\n-----END PUBLIC KEY-----\n",
                )])),
                ..Default::default()
            }
        }

        fn context() -> SessionContext {
            SessionContext::public(Some("tenant".to_string()))
        }

        fn varint(value: u64) -> Vec<u8> {
            let mut buf = Vec::new();
            VarInt::from(value).encode(&mut buf).unwrap();
            buf
        }

        fn setup_with(values: Vec<Vec<u8>>) -> KeyValuePairs {
            let key = u64::from(ParameterType::AuthorizationToken);
            KeyValuePairs(
                values
                    .into_iter()
                    .map(|value| KeyValuePair::new_bytes(key, value))
                    .collect(),
            )
        }

        /// USE_VALUE (draft-18 0x3) carrying a CAT-typed payload.
        fn use_value_token() -> Vec<u8> {
            let mut value = varint(0x3);
            value.extend_from_slice(&varint(crate::CAT_TOKEN_TYPE));
            value.extend_from_slice(b"not-a-real-cat");
            value
        }

        async fn admit(
            coordinator: Arc<FixedConfig>,
            params: &KeyValuePairs,
        ) -> Result<Option<SessionAuth>, SessionRejection> {
            let authorizer = ScopeAuthorizer::new(coordinator);
            authorize_session(&authorizer, &context(), params).await
        }

        fn enforced_with_valid_key() -> ScopeConfig {
            let key = p256::ecdsa::SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
            let pem = p256::ecdsa::VerifyingKey::from(&key)
                .to_public_key_pem(Default::default())
                .expect("public key PEM");
            ScopeConfig {
                auth: Some(ScopeAuthConfig::new(vec![AuthPublicKey::es256(pem)])),
                ..Default::default()
            }
        }

        #[tokio::test]
        async fn internal_session_without_token_is_admitted_and_authorizes_requests() {
            let context = SessionContext::internal(Some("tenant".to_string()), None);
            let namespace = TrackNamespace::from_utf8_path("sports/football");
            let track = TrackName::from("video");
            let prefix = TrackNamespacePrefix::from_utf8_path("sports");

            for coordinator in [
                FixedConfig::new(enforced_with_valid_key()),
                FixedConfig::new(ScopeConfig::default()),
                FixedConfig::failing(),
            ] {
                let authorizer = ScopeAuthorizer::new(coordinator);
                let auth = authorize_session(&authorizer, &context, &KeyValuePairs::default())
                    .await
                    .expect("internal relay session must be admitted without a token");

                for operation in [
                    AuthzOperation::Subscribe {
                        namespace: &namespace,
                        track: &track,
                    },
                    AuthzOperation::Fetch {
                        namespace: &namespace,
                        track: &track,
                    },
                    AuthzOperation::PublishNamespace {
                        namespace: &namespace,
                    },
                    AuthzOperation::SubscribeNamespace { prefix: &prefix },
                    AuthzOperation::Publish {
                        namespace: &namespace,
                        track: &track,
                    },
                    AuthzOperation::TrackStatus {
                        namespace: &namespace,
                        track: &track,
                    },
                ] {
                    let label = operation.label();
                    authorize(auth.as_ref(), &context, operation, Some(1))
                        .await
                        .unwrap_or_else(|reason| {
                            panic!("internal relay {label} must be authorized: {reason}")
                        });
                }
            }
        }

        #[tokio::test]
        async fn untagged_session_without_token_on_enforced_scope_is_denied() {
            let authorizer = ScopeAuthorizer::new(FixedConfig::new(enforced_with_valid_key()));
            let context = SessionContext::from_tags(
                Some("tenant".to_string()),
                &ConnectionTags::default(),
                None,
            );
            let rejection = authorize_session(&authorizer, &context, &KeyValuePairs::default())
                .await
                .expect_err("untagged session must present a token on an enforced scope");
            assert_eq!(rejection.code, SESSION_ERROR_UNAUTHORIZED);
            assert_eq!(rejection.reason, "unauthorized");
        }

        #[tokio::test]
        async fn malformed_setup_tokens_are_rejected_on_both_interfaces() {
            for config in [enforced_with_valid_key(), ScopeConfig::default()] {
                let authorizer = ScopeAuthorizer::new(FixedConfig::new(config));
                for context in [
                    SessionContext::internal(Some("tenant".to_string()), None),
                    context(),
                ] {
                    // Draft-18 section 10.2.2 requires distinct close codes for a
                    // malformed structure and a forbidden alias directive in SETUP.
                    for (value, code, reason) in [
                        (vec![0x3], 0x6, "malformed authorization token"),
                        (vec![0x2], 0x3, "token alias not permitted in setup"),
                    ] {
                        let rejection =
                            authorize_session(&authorizer, &context, &setup_with(vec![value]))
                                .await
                                .expect_err("invalid Token structure must reject SETUP");
                        assert_eq!(rejection.code, code);
                        assert_eq!(rejection.reason, reason);
                    }
                }
            }
        }

        /// A scope with no policy behaves exactly as before: admitted, and the
        /// session carries no authorization state.
        #[tokio::test]
        async fn a_scope_without_policy_admits_with_no_auth_state() {
            let auth = admit(
                FixedConfig::new(ScopeConfig::default()),
                &KeyValuePairs::default(),
            )
            .await
            .expect("admitted");
            assert!(auth.is_none());

            // A well-formed token is ignored rather than refused.
            let auth = admit(
                FixedConfig::new(ScopeConfig::default()),
                &setup_with(vec![use_value_token()]),
            )
            .await
            .expect("admitted");
            assert!(auth.is_none());
        }

        /// The wire-format MUSTs apply whether or not the scope has a policy.
        #[tokio::test]
        async fn a_malformed_token_is_refused_even_without_policy() {
            let err = admit(
                FixedConfig::new(ScopeConfig::default()),
                &setup_with(vec![varint(0x2)]), // USE_ALIAS in SETUP
            )
            .await
            .expect_err("refused");
            assert_eq!(err.code, 0x3, "PROTOCOL_VIOLATION (draft-18 §10.2.2)");

            let err = admit(
                FixedConfig::new(ScopeConfig::default()),
                &setup_with(vec![varint(0x9)]), // unknown alias type
            )
            .await
            .expect_err("refused");
            assert_eq!(
                err.code, 0x6,
                "KEY_VALUE_FORMATTING_ERROR (draft-18 §10.2.2)"
            );
        }

        /// A policy whose token is absent, or not a valid CAT, refuses with
        /// UNAUTHORIZED — never admits. Without `auth-cat` the scope cannot
        /// build a hook at all; with it, the placeholder key is unusable.
        /// Either way it must fail closed.
        #[tokio::test]
        async fn a_configured_policy_fails_closed() {
            for params in [
                KeyValuePairs::default(),
                setup_with(vec![use_value_token()]),
            ] {
                let err = admit(FixedConfig::new(enforced()), &params)
                    .await
                    .expect_err("must not admit");
                assert_eq!(err.code, SESSION_ERROR_UNAUTHORIZED);
                assert_eq!(err.reason, "unauthorized");
            }
        }

        /// If the relay cannot learn the scope's policy it must not assume
        /// there is none.
        #[tokio::test]
        async fn an_unreachable_coordinator_fails_closed() {
            let err = admit(FixedConfig::failing(), &KeyValuePairs::default())
                .await
                .expect_err("must not admit");
            assert_eq!(err.code, SESSION_ERROR_UNAUTHORIZED);
            assert_eq!(err.reason, "authorization unavailable");
        }

        /// The reason phrase is fixed text: nothing from the token or the
        /// validation failure reaches the peer.
        #[tokio::test]
        async fn rejection_reasons_are_fixed_text() {
            let err = admit(
                FixedConfig::new(enforced()),
                &setup_with(vec![use_value_token()]),
            )
            .await
            .expect_err("refused");
            assert!(!err.reason.contains("not-a-real-cat"));
            assert!(!err.reason.contains("auth-cat"));
        }

        /// End to end through the draft-18 wire encoding: a real CAT in a
        /// USE_VALUE (0x3) SETUP option admits; the same token with the
        /// wrong key, or a SETUP with no token, is refused.
        #[cfg(feature = "auth-cat")]
        #[tokio::test]
        async fn a_valid_cat_in_setup_is_admitted() {
            use cat_token::{encode_token, CatToken, Es256Algorithm, MoqtAction, MoqtScopeBuilder};
            use p256::pkcs8::EncodePublicKey;

            fn key() -> (Es256Algorithm, String) {
                let signer = Es256Algorithm::new_with_key_pair().expect("keypair");
                let pem = signer
                    .verifying_key()
                    .to_public_key_pem(Default::default())
                    .expect("pem");
                (signer, pem)
            }

            let (signer, pem) = key();
            let (other_signer, _) = key();
            let config = ScopeConfig {
                auth: Some(
                    ScopeAuthConfig::new(vec![AuthPublicKey::es256(pem)])
                        .with_issuers(vec!["issuer".to_string()])
                        .with_audiences(vec!["relay".to_string()]),
                ),
                ..Default::default()
            };

            let mint = |signer: &Es256Algorithm| {
                let token = CatToken::new()
                    .with_issuer("issuer")
                    .with_single_audience("relay")
                    .with_subject("alice")
                    .with_expires_in(600)
                    .with_moqt_scope(
                        MoqtScopeBuilder::new()
                            .action(MoqtAction::ClientSetup)
                            .build(),
                    );
                let mut value = varint(0x3); // USE_VALUE, draft-18 §15.5
                value.extend_from_slice(&varint(crate::CAT_TOKEN_TYPE));
                value.extend_from_slice(&encode_token(&token, signer).expect("encode"));
                value
            };

            let auth = admit(
                FixedConfig::new(config.clone()),
                &setup_with(vec![mint(&signer)]),
            )
            .await
            .expect("valid token admits")
            .expect("enforced scope carries auth state");
            assert_eq!(auth.subject(), Some("alice"));

            let err = admit(
                FixedConfig::new(config.clone()),
                &setup_with(vec![mint(&other_signer)]),
            )
            .await
            .expect_err("wrong key refused");
            assert_eq!(err.code, SESSION_ERROR_UNAUTHORIZED);

            let err = admit(FixedConfig::new(config), &KeyValuePairs::default())
                .await
                .expect_err("missing token refused");
            assert_eq!(err.code, SESSION_ERROR_UNAUTHORIZED);
        }
    }
}
