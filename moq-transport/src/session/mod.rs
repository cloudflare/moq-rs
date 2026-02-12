mod error;
mod publish_namespace;
mod publish_namespace_received;
mod publish_received;
mod published;
mod publisher;
mod reader;
mod subscribe;
mod subscribe_namespace;
mod subscribe_namespace_received;
mod subscribed;
mod subscriber;
mod track_status_requested;
mod writer;

pub use error::*;
pub use publish_namespace::*;
pub use publish_namespace_received::*;
pub use publish_received::*;
pub use published::*;
pub use publisher::*;
pub use subscribe::*;
pub use subscribe_namespace::*;
pub use subscribe_namespace_received::*;
pub use subscribed::*;
pub use subscriber::*;
pub use track_status_requested::*;

use reader::*;
use writer::*;

use futures::{stream::FuturesUnordered, StreamExt};
use std::sync::{atomic, Arc, Mutex};

use crate::coding::KeyValuePairs;
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
}

impl Session {
    fn new(
        webtransport: web_transport::Session,
        sender: Writer,
        recver: Reader,
        first_requestid: u64,
        mlog: Option<mlog::MlogWriter>,
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
        };

        (session, publisher, subscriber)
    }

    /// Create an outbound/client QUIC connection, by opening a bi-directional QUIC stream for
    /// MOQT control messaging.  Performs SETUP messaging and version negotiation.
    pub async fn connect(
        mut session: web_transport::Session,
        mlog_path: Option<PathBuf>,
    ) -> Result<(Session, Publisher, Subscriber), SessionError> {
        let mlog = mlog_path.and_then(|path| {
            mlog::MlogWriter::new(path)
                .map_err(|e| log::warn!("Failed to create mlog: {}", e))
                .ok()
        });
        let control = session.open_bi().await?;
        let mut sender = Writer::new(control.0);
        let mut recver = Reader::new(control.1);

        let mut params = KeyValuePairs::default();
        params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);

        let client = setup::Client { params };

        log::debug!("sending CLIENT_SETUP: {:?}", client);
        sender.encode(&client).await?;

        // TODO: emit client_setup_created event when we add that

        let server: setup::Server = recver.decode().await?;
        log::debug!("received SERVER_SETUP: {:?}", server);

        // TODO: emit server_setup_parsed event

        // We are the client, so the first request id is 0
        let session = Session::new(session, sender, recver, 0, mlog);
        Ok((session.0, session.1.unwrap(), session.2.unwrap()))
    }

    /// Accepts an inbound/server QUIC connection, by accepting a bi-directional QUIC stream for
    /// MOQT control messaging.  Performs SETUP messaging and version negotiation.
    pub async fn accept(
        mut session: web_transport::Session,
        mlog_path: Option<PathBuf>,
    ) -> Result<(Session, Option<Publisher>, Option<Subscriber>), SessionError> {
        let mut mlog = mlog_path.and_then(|path| {
            mlog::MlogWriter::new(path)
                .map_err(|e| log::warn!("Failed to create mlog: {}", e))
                .ok()
        });
        let control = session.accept_bi().await?;
        let mut sender = Writer::new(control.0);
        let mut recver = Reader::new(control.1);

        let client: setup::Client = recver.decode().await?;
        log::debug!("received CLIENT_SETUP: {:?}", client);

        // Emit mlog event for CLIENT_SETUP parsed
        if let Some(ref mut mlog) = mlog {
            let event = mlog::events::client_setup_parsed(mlog.elapsed_ms(), 0, &client);
            let _ = mlog.add_event(event);
        }

        // TODO SLG - make configurable?
        let mut params = KeyValuePairs::default();
        params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);

        let server = setup::Server { params };

        log::debug!("sending SERVER_SETUP: {:?}", server);

        // Emit mlog event for SERVER_SETUP created
        if let Some(ref mut mlog) = mlog {
            let event = mlog::events::server_setup_created(mlog.elapsed_ms(), 0, &server);
            let _ = mlog.add_event(event);
        }

        sender.encode(&server).await?;

        // We are the server, so the first request id is 1
        Ok(Session::new(session, sender, recver, 1, mlog))
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
            log::debug!("sending message: {:?}", msg);

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
                        Message::RequestError(m) => {
                            Some(mlog::events::reqeust_error_created(time, stream_id, m))
                        }
                        Message::Unsubscribe(m) => {
                            Some(mlog::events::unsubscribe_created(time, stream_id, m))
                        }
                        Message::PublishNamespace(m) => {
                            Some(mlog::events::publish_namespace_created(time, stream_id, m))
                        }
                        Message::RequestOk(m) => {
                            Some(mlog::events::reqeust_ok_created(time, stream_id, m))
                        }
                        Message::GoAway(m) => {
                            Some(mlog::events::go_away_created(time, stream_id, m))
                        }
                        Message::Publish(m) => {
                            Some(mlog::events::publish_created(time, stream_id, m))
                        }
                        Message::PublishOk(m) => {
                            Some(mlog::events::publish_ok_created(time, stream_id, m))
                        }
                        Message::PublishDone(m) => {
                            Some(mlog::events::publish_done_created(time, stream_id, m))
                        }
                        _ => None,
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
            log::debug!("received message: {:?}", msg);

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
                        Message::RequestError(m) => {
                            Some(mlog::events::request_error_parsed(time, stream_id, m))
                        }
                        Message::Unsubscribe(m) => {
                            Some(mlog::events::unsubscribe_parsed(time, stream_id, m))
                        }
                        Message::PublishNamespace(m) => {
                            Some(mlog::events::publish_namespace_parsed(time, stream_id, m))
                        }
                        Message::RequestOk(m) => {
                            Some(mlog::events::request_ok_parsed(time, stream_id, m))
                        }
                        Message::GoAway(m) => {
                            Some(mlog::events::go_away_parsed(time, stream_id, m))
                        }
                        Message::Publish(m) => {
                            Some(mlog::events::publish_parsed(time, stream_id, m))
                        }
                        Message::PublishOk(m) => {
                            Some(mlog::events::publish_ok_parsed(time, stream_id, m))
                        }
                        Message::PublishDone(m) => {
                            Some(mlog::events::publish_done_parsed(time, stream_id, m))
                        }
                        _ => None,
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
            log::warn!("Unimplemented message type received: {:?}", msg);
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
        mut webtransport: web_transport::Session,
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
                            log::warn!("failed to serve stream: {}", err);
                        };
                    });
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
            };
        }
    }

    /// Receives QUIC datagrams and processes them using the Subscriber logic
    async fn run_datagrams(
        mut webtransport: web_transport::Session,
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
