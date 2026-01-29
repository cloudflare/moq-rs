use std::{collections::VecDeque, fmt, sync::Arc};

use crate::watch::State;

use super::{ServeError, Track};

/// Maximum number of datagrams to buffer before dropping old ones.
/// This prevents unbounded memory growth while allowing burst handling.
const MAX_DATAGRAM_BUFFER: usize = 1024;

pub struct Datagrams {
    pub track: Arc<Track>,
}

impl Datagrams {
    pub fn produce(self) -> (DatagramsWriter, DatagramsReader) {
        let (writer, reader) = State::default().split();

        let writer = DatagramsWriter::new(writer, self.track.clone());
        let reader = DatagramsReader::new(reader, self.track);

        (writer, reader)
    }
}

struct DatagramsState {
    // Queue of pending datagrams (FIFO)
    queue: VecDeque<Datagram>,

    // Global write counter - incremented each time a datagram is added
    write_count: u64,

    // Number of datagrams dropped from front due to buffer overflow
    dropped_count: u64,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for DatagramsState {
    fn default() -> Self {
        Self {
            queue: VecDeque::with_capacity(256),
            write_count: 0,
            dropped_count: 0,
            closed: Ok(()),
        }
    }
}

pub struct DatagramsWriter {
    state: State<DatagramsState>,
    pub track: Arc<Track>,
}

impl DatagramsWriter {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self { state, track }
    }

    pub fn write(&mut self, datagram: Datagram) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        // If queue is full, drop oldest datagram to make room
        if state.queue.len() >= MAX_DATAGRAM_BUFFER {
            state.queue.pop_front();
            state.dropped_count += 1;
        }

        state.queue.push_back(datagram);
        state.write_count += 1;

        Ok(())
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

#[derive(Clone)]
pub struct DatagramsReader {
    state: State<DatagramsState>,
    pub track: Arc<Track>,

    // Track how many datagrams this reader has consumed (absolute count)
    // This allows us to calculate our position in the queue
    consumed_count: u64,
}

impl DatagramsReader {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        // Initialize consumed_count to current dropped_count so we start from current position
        let initial_dropped = {
            let state = state.lock();
            state.dropped_count
        };

        Self {
            state,
            track,
            consumed_count: initial_dropped,
        }
    }

    pub async fn read(&mut self) -> Result<Option<Datagram>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                // Calculate our index in the current queue
                // queue index = consumed_count - dropped_count
                // If consumed_count < dropped_count, we missed some datagrams (they were dropped)
                let queue_index = if self.consumed_count >= state.dropped_count {
                    (self.consumed_count - state.dropped_count) as usize
                } else {
                    // We're behind - some datagrams were dropped that we haven't seen
                    // Skip to the beginning of current queue
                    self.consumed_count = state.dropped_count;
                    0
                };

                // Check if there's a datagram we haven't read yet
                if queue_index < state.queue.len() {
                    let datagram = state.queue.get(queue_index).cloned();
                    self.consumed_count += 1;
                    return Ok(datagram);
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None), // No more updates will come
                }
            }
            .await;
        }
    }

    // Returns the largest group/sequence from the most recent datagram
    pub fn latest(&self) -> Option<(u64, u64)> {
        let state = self.state.lock();
        state
            .queue
            .back()
            .map(|datagram| (datagram.group_id, datagram.object_id))
    }
}

/// Static information about the datagram.
#[derive(Clone)]
pub struct Datagram {
    pub group_id: u64,
    pub object_id: u64,
    pub priority: u8,
    pub payload: bytes::Bytes,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,
}

impl fmt::Debug for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Datagram")
            .field("object_id", &self.object_id)
            .field("group_id", &self.group_id)
            .field("priority", &self.priority)
            .field("payload", &self.payload.len())
            .field("extension_headers", &self.extension_headers)
            .finish()
    }
}
