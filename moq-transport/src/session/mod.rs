mod announce;
mod announced;
mod error;
mod publisher;
mod reader;
mod subscribe;
mod subscribed;
mod subscriber;
mod track_status_requested;
mod writer;

pub use announce::*;
pub use announced::*;
pub use error::*;
pub use publisher::*;
pub use subscribe::*;
pub use subscribed::*;
pub use subscriber::*;
pub use track_status_requested::*;

use reader::*;
use writer::*;

use futures::{stream::FuturesUnordered, StreamExt};
use std::sync::{atomic, Arc, Mutex};

use crate::coding::{KeyValuePairs, Value};
use crate::message::Message;
use crate::mlog;
use crate::watch::Queue;
use crate::{message, setup};
use std::path::PathBuf;

/// Session object for managing all communications in a single QUIC connection.
#[must_use = "run() must be called"]
pub struct Session {
    webtransport: web_transport::Session,

    /// Control Stream Reader and Writer (QUIC bi-directional stream)
    sender: Writer, // Control Stream Sender
    recver: Reader, // Control Stream Receiver

    publisher: Option<Publisher>, // Contains Publisher side logic, uses outgoing message queue to send control messages
    subscriber: Option<Subscriber>, // Contains Subscriber side logic, uses outgoing message queue to send control messages

    /// Queue used by Publisher and Subscriber for sending Control Messages
    outgoing: Queue<Message>,

    /// Optional mlog writer for MoQ Transport events
    /// Wrapped in Arc<Mutex<>> to share across send/recv tasks when enabled
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,

    /// The connection path, derived from the WebTransport URL path or CLIENT_SETUP PATH parameter.
    /// For incoming connections: extracted during accept() from the WebTransport CONNECT URL
    /// (takes precedence) or the CLIENT_SETUP PATH parameter (key 0x1).
    /// For outgoing connections: auto-extracted from the session URL in connect().
    connection_path: Option<String>,
}

impl Session {
    /// Returns the connection path, if one was present on the incoming connection.
    ///
    /// For server-side sessions (created via `accept()`), this is derived from:
    /// 1. The WebTransport CONNECT URL path (takes precedence), or
    /// 2. The CLIENT_SETUP PATH parameter (key 0x1), used for raw QUIC connections.
    ///
    /// Returns `None` if no path was present or if the path was just "/".
    pub fn connection_path(&self) -> Option<&str> {
        self.connection_path.as_deref()
    }

    // Helper for determining the largest supported version
    fn largest_common<T: Ord + Clone + Eq>(a: &[T], b: &[T]) -> Option<T> {
        a.iter()
            .filter(|x| b.contains(x)) // keep only items also in b
            .cloned() // clone because we return T, not &T
            .max() // take the largest
    }

    /// Log a control message with structured fields for observability.
    /// Uses target "moq_transport::control" so it can be filtered independently.
    fn log_control_message(msg: &Message, direction: &str) {
        match msg {
            Message::Subscribe(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE",
                    subscribe_id = m.id,
                    namespace = %m.track_namespace,
                    track_name = %m.track_name,
                    filter_type = ?m.filter_type,
                    "MoQT control message"
                );
            }
            Message::SubscribeOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_OK",
                    subscribe_id = m.id,
                    track_alias = m.track_alias,
                    content_exists = m.content_exists,
                    "MoQT control message"
                );
            }
            Message::SubscribeError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_ERROR",
                    subscribe_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::SubscribeUpdate(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_UPDATE",
                    request_id = m.id,
                    subscription_request_id = m.subscription_request_id,
                    "MoQT control message"
                );
            }
            Message::Unsubscribe(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "UNSUBSCRIBE",
                    subscribe_id = m.id,
                    "MoQT control message"
                );
            }
            Message::PublishNamespace(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_NAMESPACE",
                    request_id = m.id,
                    namespace = %m.track_namespace,
                    "MoQT control message"
                );
            }
            Message::PublishNamespaceOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_NAMESPACE_OK",
                    request_id = m.id,
                    "MoQT control message"
                );
            }
            Message::PublishNamespaceError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_NAMESPACE_ERROR",
                    request_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::PublishNamespaceDone(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_NAMESPACE_DONE",
                    namespace = %m.track_namespace,
                    "MoQT control message"
                );
            }
            Message::PublishNamespaceCancel(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_NAMESPACE_CANCEL",
                    namespace = %m.track_namespace,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::TrackStatus(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "TRACK_STATUS",
                    request_id = m.id,
                    namespace = %m.track_namespace,
                    track_name = %m.track_name,
                    "MoQT control message"
                );
            }
            Message::TrackStatusOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "TRACK_STATUS_OK",
                    request_id = m.id,
                    track_alias = m.track_alias,
                    content_exists = m.content_exists,
                    "MoQT control message"
                );
            }
            Message::TrackStatusError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "TRACK_STATUS_ERROR",
                    request_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::SubscribeNamespace(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_NAMESPACE",
                    request_id = m.id,
                    namespace_prefix = %m.track_namespace_prefix,
                    "MoQT control message"
                );
            }
            Message::SubscribeNamespaceOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_NAMESPACE_OK",
                    request_id = m.id,
                    "MoQT control message"
                );
            }
            Message::SubscribeNamespaceError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "SUBSCRIBE_NAMESPACE_ERROR",
                    request_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::UnsubscribeNamespace(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "UNSUBSCRIBE_NAMESPACE",
                    namespace_prefix = %m.track_namespace_prefix,
                    "MoQT control message"
                );
            }
            Message::Fetch(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "FETCH",
                    request_id = m.id,
                    fetch_type = ?m.fetch_type,
                    "MoQT control message"
                );
            }
            Message::FetchOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "FETCH_OK",
                    request_id = m.id,
                    end_of_track = m.end_of_track,
                    "MoQT control message"
                );
            }
            Message::FetchError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "FETCH_ERROR",
                    request_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::FetchCancel(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "FETCH_CANCEL",
                    request_id = m.id,
                    "MoQT control message"
                );
            }
            Message::Publish(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH",
                    request_id = m.id,
                    namespace = %m.track_namespace,
                    track_name = %m.track_name,
                    track_alias = m.track_alias,
                    "MoQT control message"
                );
            }
            Message::PublishOk(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_OK",
                    request_id = m.id,
                    filter_type = ?m.filter_type,
                    "MoQT control message"
                );
            }
            Message::PublishError(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_ERROR",
                    request_id = m.id,
                    error_code = m.error_code,
                    reason = %m.reason_phrase.0,
                    "MoQT control message"
                );
            }
            Message::PublishDone(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "PUBLISH_DONE",
                    request_id = m.id,
                    status_code = m.status_code,
                    stream_count = m.stream_count,
                    "MoQT control message"
                );
            }
            Message::GoAway(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "GOAWAY",
                    uri = %m.uri.0,
                    "MoQT control message"
                );
            }
            Message::MaxRequestId(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "MAX_REQUEST_ID",
                    request_id = m.request_id,
                    "MoQT control message"
                );
            }
            Message::RequestsBlocked(m) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    direction,
                    msg_type = "REQUESTS_BLOCKED",
                    max_request_id = m.max_request_id,
                    "MoQT control message"
                );
            }
        }
    }

    fn new(
        webtransport: web_transport::Session,
        sender: Writer,
        recver: Reader,
        first_requestid: u64,
        mlog: Option<mlog::MlogWriter>,
        connection_path: Option<String>,
    ) -> (Self, Option<Publisher>, Option<Subscriber>) {
        let next_requestid = Arc::new(atomic::AtomicU64::new(first_requestid));
        let outgoing = Queue::default().split();

        // Wrap mlog in Arc<Mutex<>> for sharing across tasks
        let mlog_shared = mlog.map(|m| Arc::new(Mutex::new(m)));

        let publisher = Some(Publisher::new(
            outgoing.0.clone(),
            webtransport.clone(),
            next_requestid.clone(),
            mlog_shared.clone(),
        ));
        let subscriber = Some(Subscriber::new(
            outgoing.0,
            next_requestid,
            mlog_shared.clone(),
        ));

        let session = Self {
            webtransport,
            sender,
            recver,
            publisher: publisher.clone(),
            subscriber: subscriber.clone(),
            outgoing: outgoing.1,
            mlog: mlog_shared,
            connection_path,
        };

        (session, publisher, subscriber)
    }

    /// Create an outbound/client QUIC connection, by opening a bi-directional QUIC stream for
    /// MOQT control messaging.  Performs SETUP messaging and version negotiation.
    ///
    /// If the session URL contains a non-trivial path (not empty or "/"), the PATH
    /// parameter (key 0x1) is automatically sent in CLIENT_SETUP. This propagates
    /// the connection path (App ID / MoQT scope) to the remote peer, which is needed
    /// for relay-to-relay connections. To connect without sending PATH, use a URL
    /// with no path component.
    pub async fn connect(
        session: web_transport::Session,
        mlog_path: Option<PathBuf>,
    ) -> Result<(Session, Publisher, Subscriber), SessionError> {
        // Auto-extract path from the session URL.
        // This aligns with the unified moqt:// URI scheme direction (IETF PR #1486)
        // where the path is always part of the URI regardless of transport.
        let url_path = session.url().path();
        let path = if url_path.is_empty() || url_path == "/" {
            None
        } else {
            Some(url_path.to_string())
        };
        let mlog = mlog_path.and_then(|path| {
            mlog::MlogWriter::new(path)
                .map_err(|e| tracing::warn!("Failed to create mlog: {}", e))
                .ok()
        });
        let control = session.open_bi().await?;
        let mut sender = Writer::new(control.0);
        let mut recver = Reader::new(control.1);

        let versions: setup::Versions = [setup::Version::DRAFT_14].into();

        // TODO SLG - make configurable?
        let mut params = KeyValuePairs::default();
        params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);

        // Set PATH parameter in CLIENT_SETUP for relay-to-relay path propagation.
        if let Some(ref path) = path {
            params.set_bytesvalue(
                setup::ParameterType::Path.into(),
                path.as_bytes().to_vec(),
            );
        }

        let client = setup::Client {
            versions: versions.clone(),
            params,
        };

        tracing::debug!(
            target: "moq_transport::control",
            direction = "sent",
            msg_type = "CLIENT_SETUP",
            versions = ?client.versions,
            path = path.as_deref(),
            "MoQT control message"
        );
        sender.encode(&client).await?;

        // TODO: emit client_setup_created event when we add that

        let server: setup::Server = recver.decode().await?;
        tracing::debug!(
            target: "moq_transport::control",
            direction = "recv",
            msg_type = "SERVER_SETUP",
            version = ?server.version,
            "MoQT control message"
        );

        // TODO: emit server_setup_parsed event

        // We are the client, so the first request id is 0
        let session = Session::new(session, sender, recver, 0, mlog, path);
        Ok((session.0, session.1.unwrap(), session.2.unwrap()))
    }

    /// Accepts an inbound/server QUIC connection, by accepting a bi-directional QUIC stream for
    /// MOQT control messaging.  Performs SETUP messaging and version negotiation.
    pub async fn accept(
        session: web_transport::Session,
        mlog_path: Option<PathBuf>,
    ) -> Result<(Session, Option<Publisher>, Option<Subscriber>), SessionError> {
        let mut mlog = mlog_path.and_then(|path| {
            mlog::MlogWriter::new(path)
                .map_err(|e| tracing::warn!("Failed to create mlog: {}", e))
                .ok()
        });
        let control = session.accept_bi().await?;
        let mut sender = Writer::new(control.0);
        let mut recver = Reader::new(control.1);

        let client: setup::Client = recver.decode().await?;
        tracing::debug!(
            target: "moq_transport::control",
            direction = "recv",
            msg_type = "CLIENT_SETUP",
            versions = ?client.versions,
            "MoQT control message"
        );

        // Extract CLIENT_SETUP PATH parameter (key 0x1, BytesValue).
        // Used for raw QUIC connections where there's no HTTP CONNECT URL.
        let client_setup_path = client
            .params
            .get(setup::ParameterType::Path.into())
            .and_then(|kvp| match &kvp.value {
                Value::BytesValue(bytes) => String::from_utf8(bytes.clone()).ok(),
                _ => None,
            });

        // Extract WebTransport URL path from the underlying session.
        // For WebTransport connections, this comes from the HTTP/3 CONNECT :path.
        // For raw QUIC, this is the placeholder URL ("moqt://localhost") and has no meaningful path.
        let wt_url_path = session.url().path();
        let wt_path = if wt_url_path.is_empty() || wt_url_path == "/" {
            None
        } else {
            Some(wt_url_path.to_string())
        };

        // Combine: WebTransport URL path takes precedence over CLIENT_SETUP PATH.
        // WebTransport connections always have the path in the CONNECT URL.
        // Raw QUIC connections only have CLIENT_SETUP PATH.
        let connection_path = wt_path.or(client_setup_path);

        if connection_path.is_some() {
            tracing::debug!(
                connection_path = connection_path.as_deref(),
                "Connection path resolved"
            );
        }

        // Emit mlog event for CLIENT_SETUP parsed
        if let Some(ref mut mlog) = mlog {
            let event = mlog::events::client_setup_parsed(mlog.elapsed_ms(), 0, &client);
            let _ = mlog.add_event(event);
        }

        let server_versions = setup::Versions(vec![setup::Version::DRAFT_14]);

        if let Some(largest_common_version) =
            Self::largest_common(&server_versions, &client.versions)
        {
            // TODO SLG - make configurable?
            let mut params = KeyValuePairs::default();
            params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);

            let server = setup::Server {
                version: largest_common_version,
                params,
            };

            tracing::debug!(
                target: "moq_transport::control",
                direction = "sent",
                msg_type = "SERVER_SETUP",
                version = ?server.version,
                "MoQT control message"
            );

            // Emit mlog event for SERVER_SETUP created
            if let Some(ref mut mlog) = mlog {
                let event = mlog::events::server_setup_created(mlog.elapsed_ms(), 0, &server);
                let _ = mlog.add_event(event);
            }

            sender.encode(&server).await?;

            // We are the server, so the first request id is 1
            Ok(Session::new(session, sender, recver, 1, mlog, connection_path))
        } else {
            Err(SessionError::Version(client.versions, server_versions))
        }
    }

    /// Run Tasks for the session, including sending of control messages, receiving and processing
    /// inbound control messages, receiving and processing new inbound uni-directional QUIC streams,
    /// and receiving and processing QUIC datagrams received
    pub async fn run(self) -> Result<(), SessionError> {
        tokio::select! {
            res = Self::run_recv(self.recver, self.publisher, self.subscriber.clone(), self.mlog.clone()) => res,
            res = Self::run_send(self.sender, self.outgoing, self.mlog.clone()) => res,
            res = Self::run_streams(self.webtransport.clone(), self.subscriber.clone()) => res,
            res = Self::run_datagrams(self.webtransport, self.subscriber) => res,
        }
    }

    /// Processes the outgoing control message queue, and sends queued messages on the control stream sender/writer.
    async fn run_send(
        mut sender: Writer,
        mut outgoing: Queue<message::Message>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        while let Some(msg) = outgoing.pop().await {
            // Emit structured tracing log for sent control messages
            Self::log_control_message(&msg, "sent");

            // Emit mlog event for sent control messages
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // Control stream is always stream 0

                    // Emit events based on message type
                    let event = match &msg {
                        Message::Subscribe(m) => {
                            Some(mlog::events::subscribe_created(time, stream_id, m))
                        }
                        Message::SubscribeOk(m) => {
                            Some(mlog::events::subscribe_ok_created(time, stream_id, m))
                        }
                        Message::SubscribeError(m) => {
                            Some(mlog::events::subscribe_error_created(time, stream_id, m))
                        }
                        Message::Unsubscribe(m) => {
                            Some(mlog::events::unsubscribe_created(time, stream_id, m))
                        }
                        Message::PublishNamespace(m) => {
                            Some(mlog::events::publish_namespace_created(time, stream_id, m))
                        }
                        Message::PublishNamespaceOk(m) => Some(
                            mlog::events::publish_namespace_ok_created(time, stream_id, m),
                        ),
                        Message::PublishNamespaceError(m) => Some(
                            mlog::events::publish_namespace_error_created(time, stream_id, m),
                        ),
                        Message::GoAway(m) => {
                            Some(mlog::events::go_away_created(time, stream_id, m))
                        }
                        _ => None, // TODO: Add other message types
                    };

                    if let Some(event) = event {
                        let _ = mlog_guard.add_event(event);
                    }
                }
            }

            sender.encode(&msg).await?;
        }

        Ok(())
    }

    /// Receives inbound messages from the control stream reader/receiver.  Analyzes if the message
    /// is to be handled by Subscriber or Publisher logic and calls recv_message on either the
    /// Publisher or Subscriber.
    /// Note:  Should also be handling messages common to both roles, ie: GOAWAY, MAX_REQUEST_ID and
    ///        REQUESTS_BLOCKED
    async fn run_recv(
        mut recver: Reader,
        mut publisher: Option<Publisher>,
        mut subscriber: Option<Subscriber>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        loop {
            let msg: message::Message = recver.decode().await?;

            // Emit structured tracing log for received control messages
            Self::log_control_message(&msg, "recv");

            // Emit mlog event for received control messages
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // Control stream is always stream 0

                    // Emit events based on message type
                    let event = match &msg {
                        Message::Subscribe(m) => {
                            Some(mlog::events::subscribe_parsed(time, stream_id, m))
                        }
                        Message::SubscribeOk(m) => {
                            Some(mlog::events::subscribe_ok_parsed(time, stream_id, m))
                        }
                        Message::SubscribeError(m) => {
                            Some(mlog::events::subscribe_error_parsed(time, stream_id, m))
                        }
                        Message::Unsubscribe(m) => {
                            Some(mlog::events::unsubscribe_parsed(time, stream_id, m))
                        }
                        Message::PublishNamespace(m) => {
                            Some(mlog::events::publish_namespace_parsed(time, stream_id, m))
                        }
                        Message::PublishNamespaceOk(m) => Some(
                            mlog::events::publish_namespace_ok_parsed(time, stream_id, m),
                        ),
                        Message::PublishNamespaceError(m) => Some(
                            mlog::events::publish_namespace_error_parsed(time, stream_id, m),
                        ),
                        Message::GoAway(m) => {
                            Some(mlog::events::go_away_parsed(time, stream_id, m))
                        }
                        _ => None, // TODO: Add other message types
                    };

                    if let Some(event) = event {
                        let _ = mlog_guard.add_event(event);
                    }
                }
            }

            let msg = match TryInto::<message::Publisher>::try_into(msg) {
                Ok(msg) => {
                    subscriber
                        .as_mut()
                        .ok_or(SessionError::RoleViolation)?
                        .recv_message(msg)?;
                    continue;
                }
                Err(msg) => msg,
            };

            let msg = match TryInto::<message::Subscriber>::try_into(msg) {
                Ok(msg) => {
                    publisher
                        .as_mut()
                        .ok_or(SessionError::RoleViolation)?
                        .recv_message(msg)?;
                    continue;
                }
                Err(msg) => msg,
            };

            // TODO GOAWAY, MAX_REQUEST_ID, REQUESTS_BLOCKED
            tracing::warn!("Unimplemented message type received: {:?}", msg);
            return Err(SessionError::unimplemented(&format!(
                "message type {:?}",
                msg
            )));
        }
    }

    /// Accepts uni-directional quic streams and starts handling for them.
    /// Will read stream header to know what type of stream it is and create
    /// the appropriate stream handlers.
    async fn run_streams(
        webtransport: web_transport::Session,
        subscriber: Option<Subscriber>,
    ) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                res = webtransport.accept_uni() => {
                    let stream = res?;
                    let subscriber = subscriber.clone().ok_or(SessionError::RoleViolation)?;

                    tasks.push(async move {
                        if let Err(err) = Subscriber::recv_stream(subscriber, stream).await {
                            tracing::warn!("failed to serve stream: {}", err);
                        };
                    });
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
            };
        }
    }

    /// Receives QUIC datagrams and processes them using the Subscriber logic
    async fn run_datagrams(
        webtransport: web_transport::Session,
        mut subscriber: Option<Subscriber>,
    ) -> Result<(), SessionError> {
        loop {
            let datagram = webtransport.recv_datagram().await?;
            subscriber
                .as_mut()
                .ok_or(SessionError::RoleViolation)?
                .recv_datagram(datagram)
                .await?;
        }
    }
}
