// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Backend-neutral transport seam for the MoQ session layer.
//!
//! Supply any [`web_transport_trait::Session`] with [`Session::new`]. Connection
//! establishment, TLS, and URL selection belong to the embedder. The URL is
//! explicit metadata: the transport trait does not carry it, but MoQ needs it
//! for connection paths and raw-QUIC SETUP parameters.
//!
//! Type erasure keeps protocol handles independent of backend types. Futures
//! are boxed at this boundary; payload chunks remain owned `Bytes`. Dropping
//! an operation drops its backend future without spawning background work.

use std::{any::Any, fmt, future::Future, pin::Pin, sync::Arc};

use bytes::{Buf, BufMut, Bytes};
use futures::lock::Mutex;
use url::Url;
use web_transport_trait::{MaybeSend, MaybeSync};

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
pub use web_transport_quinn as quinn;

#[cfg(not(target_family = "wasm"))]
type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_family = "wasm")]
type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

trait ErrorSource: web_transport_trait::Error {}

impl<E: web_transport_trait::Error> ErrorSource for E {}

/// An error from a transport other than the built-in Quinn backend.
#[derive(Clone)]
pub struct BackendError(Arc<dyn ErrorSource>);

impl fmt::Debug for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// A cloneable transport failure, retaining backend error details.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("backend read exceeded the requested size")]
    InvalidReadSize,
    #[error("backend write exceeded the supplied size")]
    InvalidWriteSize,
    #[error("backend accepted zero bytes from a nonempty write")]
    WriteZero,
    #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
    #[error("session error: {0}")]
    Session(#[from] quinn::SessionError),
    #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
    #[error("write error: {0}")]
    Write(quinn::WriteError),
    #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
    #[error("read error: {0}")]
    Read(quinn::ReadError),
    #[error("{0}")]
    Backend(#[source] BackendError),
}

impl Error {
    fn new<E: web_transport_trait::Error>(err: E) -> Self {
        // Preserve the existing native error variants and their classification.
        #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
        {
            let any = &err as &dyn Any;
            if let Some(err) = any.downcast_ref::<quinn::SessionError>() {
                return Self::Session(err.clone());
            }
            if let Some(err) = any.downcast_ref::<quinn::ReadError>() {
                return match err {
                    quinn::ReadError::SessionError(err) => Self::Session(err.clone()),
                    err => Self::Read(err.clone()),
                };
            }
            if let Some(err) = any.downcast_ref::<quinn::WriteError>() {
                return match err {
                    quinn::WriteError::SessionError(err) => Self::Session(err.clone()),
                    err => Self::Write(err.clone()),
                };
            }
        }
        Self::Backend(BackendError(Arc::new(err)))
    }

    /// Application close information supplied by the backend's error trait.
    pub fn session_error(&self) -> Option<(u32, String)> {
        use web_transport_trait::Error as _;
        match self {
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Session(err) => err.session_error(),
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Write(err) => err.session_error(),
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Read(err) => err.session_error(),
            Self::Backend(err) => err.0.session_error(),
            Self::InvalidReadSize | Self::InvalidWriteSize | Self::WriteZero => None,
        }
    }

    /// RESET_STREAM or STOP_SENDING information supplied by the backend.
    pub fn stream_error(&self) -> Option<u32> {
        use web_transport_trait::Error as _;
        match self {
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Session(err) => err.stream_error(),
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Write(err) => err.stream_error(),
            #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
            Self::Read(err) => err.stream_error(),
            Self::Backend(err) => err.0.stream_error(),
            Self::InvalidReadSize | Self::InvalidWriteSize | Self::WriteZero => None,
        }
    }
}

trait SessionOps: MaybeSend + MaybeSync {
    fn accept_uni(&self) -> TransportFuture<'_, Result<RecvStream, Error>>;
    fn accept_bi(&self) -> TransportFuture<'_, Result<(SendStream, RecvStream), Error>>;
    fn open_uni(&self) -> TransportFuture<'_, Result<SendStream, Error>>;
    fn open_bi(&self) -> TransportFuture<'_, Result<(SendStream, RecvStream), Error>>;
    fn send_datagram(&self, data: Bytes) -> Result<(), Error>;
    fn recv_datagram(&self) -> TransportFuture<'_, Result<Bytes, Error>>;
    fn max_datagram_size(&self) -> usize;
    fn protocol(&self) -> Option<&str>;
    fn close(&self, code: u32, reason: &str);
    fn closed(&self) -> TransportFuture<'_, Error>;
}

impl<T> SessionOps for T
where
    T: web_transport_trait::Session,
    T::SendStream: 'static,
    T::RecvStream: 'static,
{
    fn accept_uni(&self) -> TransportFuture<'_, Result<RecvStream, Error>> {
        Box::pin(async move {
            T::accept_uni(self)
                .await
                .map(RecvStream::new)
                .map_err(Error::new)
        })
    }

    fn accept_bi(&self) -> TransportFuture<'_, Result<(SendStream, RecvStream), Error>> {
        Box::pin(async move {
            T::accept_bi(self)
                .await
                .map(|(send, recv)| (SendStream::new(send), RecvStream::new(recv)))
                .map_err(Error::new)
        })
    }

    fn open_uni(&self) -> TransportFuture<'_, Result<SendStream, Error>> {
        Box::pin(async move {
            T::open_uni(self)
                .await
                .map(SendStream::new)
                .map_err(Error::new)
        })
    }

    fn open_bi(&self) -> TransportFuture<'_, Result<(SendStream, RecvStream), Error>> {
        Box::pin(async move {
            T::open_bi(self)
                .await
                .map(|(send, recv)| (SendStream::new(send), RecvStream::new(recv)))
                .map_err(Error::new)
        })
    }

    fn send_datagram(&self, data: Bytes) -> Result<(), Error> {
        T::send_datagram(self, data).map_err(Error::new)
    }

    fn recv_datagram(&self) -> TransportFuture<'_, Result<Bytes, Error>> {
        Box::pin(async move { T::recv_datagram(self).await.map_err(Error::new) })
    }

    fn max_datagram_size(&self) -> usize {
        T::max_datagram_size(self)
    }
    fn protocol(&self) -> Option<&str> {
        T::protocol(self)
    }
    fn close(&self, code: u32, reason: &str) {
        T::close(self, code, reason);
    }
    fn closed(&self) -> TransportFuture<'_, Error> {
        Box::pin(async move { Error::new(T::closed(self).await) })
    }
}

/// A transport supplied by an embedder, plus the connection's original URL.
#[derive(Clone)]
pub struct Session {
    inner: Arc<dyn SessionOps>,
    url: Url,
}

impl Session {
    /// Adapt a backend without exposing its concrete types to MoQ handles.
    pub fn new<T>(session: T, url: Url) -> Self
    where
        T: web_transport_trait::Session,
        T::SendStream: 'static,
        T::RecvStream: 'static,
    {
        Self {
            inner: Arc::new(session),
            url,
        }
    }

    pub async fn accept_uni(&self) -> Result<RecvStream, Error> {
        self.inner.accept_uni().await
    }
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), Error> {
        self.inner.accept_bi().await
    }
    pub async fn open_uni(&self) -> Result<SendStream, Error> {
        self.inner.open_uni().await
    }
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), Error> {
        self.inner.open_bi().await
    }
    pub async fn send_datagram(&self, data: Bytes) -> Result<(), Error> {
        self.inner.send_datagram(data)
    }
    pub async fn recv_datagram(&self) -> Result<Bytes, Error> {
        self.inner.recv_datagram().await
    }
    pub fn max_datagram_size(&self) -> usize {
        self.inner.max_datagram_size()
    }
    pub fn protocol(&self) -> Option<&str> {
        self.inner.protocol()
    }
    pub fn close(&self, code: u32, reason: &str) {
        self.inner.close(code, reason);
    }
    pub async fn closed(&self) -> Error {
        self.inner.closed().await
    }
    pub fn url(&self) -> &Url {
        &self.url
    }
}

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
impl From<quinn::Session> for Session {
    fn from(session: quinn::Session) -> Self {
        let url = session.request().url.clone();
        Self::new(session, url)
    }
}

trait SendOps: MaybeSend {
    fn write<'a>(&'a mut self, data: &'a [u8]) -> TransportFuture<'a, Result<usize, Error>>;
    fn set_priority(&mut self, priority: u8);
    fn finish(&mut self) -> Result<(), Error>;
    fn reset(&mut self, code: u32);
    fn closed(&mut self) -> TransportFuture<'_, Result<(), Error>>;
}

impl<T: web_transport_trait::SendStream> SendOps for T {
    fn write<'a>(&'a mut self, data: &'a [u8]) -> TransportFuture<'a, Result<usize, Error>> {
        Box::pin(async move { T::write(self, data).await.map_err(Error::new) })
    }
    fn set_priority(&mut self, priority: u8) {
        T::set_priority(self, priority);
    }
    fn finish(&mut self) -> Result<(), Error> {
        T::finish(self).map_err(Error::new)
    }
    fn reset(&mut self, code: u32) {
        T::reset(self, code);
    }
    fn closed(&mut self) -> TransportFuture<'_, Result<(), Error>> {
        Box::pin(async move { T::closed(self).await.map_err(Error::new) })
    }
}

/// An owned send stream. FIN/reset/drop behavior belongs to its backend.
pub struct SendStream {
    // A sendable stream need not be Sync. The mutex gives handles the Sync
    // bound used by shared request state; exclusive operations use get_mut,
    // without acquiring a lock or holding a guard across an await.
    inner: Mutex<Box<dyn SendOps>>,
}

impl SendStream {
    fn new<T: web_transport_trait::SendStream + 'static>(stream: T) -> Self {
        Self {
            inner: Mutex::new(Box::new(stream)),
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let size = self.inner.get_mut().write(data).await?;
        if size > data.len() {
            return Err(Error::InvalidWriteSize);
        }
        if size == 0 && !data.is_empty() {
            return Err(Error::WriteZero);
        }
        Ok(size)
    }

    pub async fn write_buf<B: Buf>(&mut self, data: &mut B) -> Result<usize, Error> {
        // Advance only after the backend accepts bytes. In particular, don't
        // use the old Quinn adapter's consume-before-await write_buf override.
        let size = self.write(data.chunk()).await?;
        data.advance(size);
        Ok(size)
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.inner.get_mut().set_priority(priority);
    }
    pub fn finish(&mut self) -> Result<(), Error> {
        self.inner.get_mut().finish()
    }
    pub fn reset(&mut self, code: u32) {
        self.inner.get_mut().reset(code);
    }

    /// Preserve the previous wrapper's byte-sized STOP_SENDING result.
    pub async fn closed(&mut self) -> Result<Option<u8>, Error> {
        match self.inner.get_mut().closed().await {
            Ok(()) => Ok(None),
            Err(err) => match err.stream_error() {
                Some(code) => Ok(Some(code as u8)),
                None => Err(err),
            },
        }
    }
}

trait RecvOps: MaybeSend {
    fn read_chunk(&mut self, max: usize) -> TransportFuture<'_, Result<Option<Bytes>, Error>>;
    fn stop(&mut self, code: u32);
}

impl<T: web_transport_trait::RecvStream> RecvOps for T {
    fn read_chunk(&mut self, max: usize) -> TransportFuture<'_, Result<Option<Bytes>, Error>> {
        Box::pin(async move { T::read_chunk(self, max).await.map_err(Error::new) })
    }
    fn stop(&mut self, code: u32) {
        T::stop(self, code);
    }
}

/// An owned receive stream, preserving backend-owned payload chunks.
pub struct RecvStream {
    inner: Mutex<Box<dyn RecvOps>>,
}

impl RecvStream {
    fn new<T: web_transport_trait::RecvStream + 'static>(stream: T) -> Self {
        Self {
            inner: Mutex::new(Box::new(stream)),
        }
    }

    pub async fn read(&mut self, max: usize) -> Result<Option<Bytes>, Error> {
        if max == 0 {
            return Ok(Some(Bytes::new()));
        }
        let chunk = self.inner.get_mut().read_chunk(max).await?;
        if chunk.as_ref().is_some_and(|chunk| chunk.len() > max) {
            return Err(Error::InvalidReadSize);
        }
        Ok(chunk)
    }

    pub async fn read_buf<B: BufMut>(&mut self, data: &mut B) -> Result<Option<usize>, Error> {
        let max = data.remaining_mut().min(8192);
        match self.read(max).await? {
            Some(chunk) => {
                let size = chunk.len();
                data.put_slice(&chunk);
                Ok(Some(size))
            }
            None => Ok(None),
        }
    }

    pub fn stop(&mut self, code: u32) {
        self.inner.get_mut().stop(code);
    }
}

#[cfg(test)]
mod tests;
