//! A track is a collection of semi-reliable and semi-ordered streams, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] creates streams with a sequence number and priority.
//! The sequence number is used to determine the order of streams, while the priority is used to determine which stream to transmit first.
//! This may seem counter-intuitive, but is designed for live streaming where the newest streams may be higher priority.
//! A cloned [Writer] can be used to create streams in parallel, but will error if a duplicate sequence number is used.
//!
//! A [Reader] may not receive all streams in order or at all.
//! These streams are meant to be transmitted over congested networks and the key to MoQ Tranport is to not block on them.
//! streams will be cached for a potentially limited duration added to the unreliable nature.
//! A cloned [Reader] will receive a copy of all new stream going forward (fanout).
//!
//! The track is closed with [ServeError::Closed] when all writers or readers are dropped.

use crate::watch::State;

use super::{
    Datagrams, DatagramsReader, DatagramsWriter, ObjectsWriter, ServeError, Stream, StreamReader,
    StreamWriter, Subgroups, SubgroupsReader, SubgroupsWriter,
};
use crate::coding::{Location, TrackNamespace};
use paste::paste;
use std::{ops::Deref, sync::Arc};

/// Static information about a track.
#[derive(Debug, Clone, PartialEq)]
pub struct Track {
    pub namespace: TrackNamespace,
    pub name: String,
}

impl Track {
    pub fn new(namespace: TrackNamespace, name: String) -> Self {
        Self { namespace, name }
    }

    pub fn produce(self) -> (TrackWriter, TrackReader) {
        // Create sharable TrackState and Info(Track)
        let (writer_track_state, reader_track_state) = State::default().split();
        let info = Arc::new(self);

        // Create TrackReader and TrackWriter with shared state and info
        let writer = TrackWriter::new(writer_track_state, info.clone());
        let reader = TrackReader::new(reader_track_state, info);

        (writer, reader)
    }
}

struct TrackState {
    /// The ReaderMode for this track. Set to None on creation.
    reader_mode: Option<TrackReaderMode>,
    /// Watchable closed state
    closed: Result<(), ServeError>,
}

impl Default for TrackState {
    fn default() -> Self {
        Self {
            reader_mode: None,
            closed: Ok(()),
        }
    }
}

/// Creates new streams for a track.
pub struct TrackWriter {
    state: State<TrackState>,
    pub info: Arc<Track>,
}

impl TrackWriter {
    /// Create a track with the given name (info/Track)
    fn new(state: State<TrackState>, info: Arc<Track>) -> Self {
        Self { state, info }
    }

    /// Create a new stream with the given priority, inserting it into the track.
    pub fn stream(self, priority: u8) -> Result<StreamWriter, ServeError> {
        // Create new StreamWriter/StreamReader pair
        let (writer, reader) = Stream {
            track: self.info.clone(),
            priority,
        }
        .produce();

        // Lock state to modify it
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        // Set the Stream mode to TrackReaderMode::Stream
        state.reader_mode = Some(reader.into());
        Ok(writer)
    }

    // TODO: rework this whole interface for clarity?
    /// Create a new subgroups stream with the given priority, inserting it into the track.
    pub fn subgroups(self) -> Result<SubgroupsWriter, ServeError> {
        let (writer, reader) = Subgroups {
            track: self.info.clone(),
        }
        .produce();

        // Lock state to modify it
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        // Set the Stream mode to TrackReaderMode::Subgroups
        state.reader_mode = Some(reader.into());
        Ok(writer)
    }

    pub fn datagrams(self) -> Result<DatagramsWriter, ServeError> {
        let (writer, reader) = Datagrams {
            track: self.info.clone(),
        }
        .produce();

        // Lock state to modify it
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        // Set the Stream mode to TrackReaderMode::Datagrams
        state.reader_mode = Some(reader.into());
        Ok(writer)
    }

    /// Close the track with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);
        Ok(())
    }
}

impl Deref for TrackWriter {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Receives new streams for a track.
#[derive(Clone)]
pub struct TrackReader {
    state: State<TrackState>,
    pub info: Arc<Track>,
}

impl TrackReader {
    fn new(state: State<TrackState>, info: Arc<Track>) -> Self {
        Self { state, info }
    }

    /// Get the current mode of the track, waiting if necessary.
    pub async fn mode(&self) -> Result<TrackReaderMode, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if let Some(mode) = &state.reader_mode {
                    return Ok(mode.clone());
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Err(ServeError::Done),
                }
            }
            .await;
        }
    }

    // Returns the largest group/sequence
    pub fn largest_location(&self) -> Option<Location> {
        // We don't even know the mode yet.
        // TODO populate from SUBSCRIBE_OK
        None
    }

    /// Wait until the track is closed, returning the closing error.
    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    /// Check if the track is closed or the writer has been dropped.
    /// This is used to detect stale cached TrackReaders that should not be reused.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();
        // Track is closed if:
        // 1. It was explicitly closed with an error, OR
        // 2. The writer side has been dropped (modified() returns None)
        state.closed.is_err() || state.modified().is_none()
    }
}

impl Deref for TrackReader {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

macro_rules! track_readers {
    {$($name:ident,)*} => {
		paste! {
			#[derive(Clone)]
			pub enum TrackReaderMode {
				$($name([<$name Reader>])),*
			}

			$(impl From<[<$name Reader>]> for TrackReaderMode {
				fn from(reader: [<$name Reader >]) -> Self {
					Self::$name(reader)
				}
			})*

			impl TrackReaderMode {
				pub fn latest(&self) -> Option<(u64, u64)> {
					match self {
						$(Self::$name(reader) => reader.latest(),)*
					}
				}
			}
		}
	}
}

track_readers!(Stream, Subgroups, Datagrams,);

macro_rules! track_writers {
    {$($name:ident,)*} => {
		paste! {
			pub enum TrackWriterMode {
				$($name([<$name Writer>])),*
			}

			$(impl From<[<$name Writer>]> for TrackWriterMode {
				fn from(writer: [<$name Writer>]) -> Self {
					Self::$name(writer)
				}
			})*

			impl TrackWriterMode {
				pub fn close(self, err: ServeError) -> Result<(), ServeError>{
					match self {
						$(Self::$name(writer) => writer.close(err),)*
					}
				}
			}
		}
	}
}

track_writers!(Track, Stream, Subgroups, Objects, Datagrams,);
