// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::Event;

/// Writer for MoQ Transport logs (mlog)
/// Writes JSON-SEQ format compatible with qlog aggregation
pub struct MlogWriter {
    writer: BufWriter<File>,
    start: Instant,
    epoch_offset_ms: f64,
}

impl MlogWriter {
    /// Create a new mlog writer for the given file path
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Capture the epoch offset once at startup, then use cheap Instant
        // for per-event timing. This avoids a SystemTime syscall per event
        // while still producing absolute epoch-ms timestamps.
        let start = Instant::now();
        let epoch_offset_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0;

        // Write qlog-compatible header as first record
        // This follows qlog JSON-SEQ format (RFC 7464) per
        // draft-ietf-quic-qlog-main-schema-13 Section 5
        //
        // Uses epoch-relative timestamps (absolute epoch-ms) so that
        // consumers can use the time field directly as a native timestamp
        // without needing to know the trace start time.
        let header = serde_json::json!({
            "file_schema": "urn:ietf:params:qlog:file:sequential",
            "serialization_format": "JSON-SEQ",
            "title": "moq-relay",
            "description": "MoQ Transport events",
            "trace": {
                "vantage_point": {
                    "type": "server"
                },
                "common_fields": {
                    "time_format": "relative_to_epoch",
                    "reference_time": {
                        "clock_type": "system",
                        "epoch": "1970-01-01T00:00:00.000Z"
                    }
                },
                "event_schemas": [
                    "urn:ietf:params:qlog:events:moqt-03"
                ]
            }
        });

        writer.write_all(b"\x1e")?;
        serde_json::to_writer(&mut writer, &header)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(Self {
            writer,
            start,
            epoch_offset_ms,
        })
    }

    /// Get current time as epoch milliseconds for event timestamps.
    /// Per qlog-main-schema-13 Section 7.1, with time_format "relative_to_epoch"
    /// and epoch "1970-01-01T00:00:00.000Z", time values are absolute Unix epoch ms.
    ///
    /// Uses a cached epoch offset from startup plus cheap monotonic elapsed time,
    /// avoiding a SystemTime syscall per event.
    pub fn epoch_ms(&self) -> f64 {
        self.epoch_offset_ms + self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Add an event to the log
    pub fn add_event(&mut self, event: Event) -> io::Result<()> {
        self.writer.write_all(b"\x1e")?;
        serde_json::to_writer(&mut self.writer, &event)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }

    /// Flush and close the log
    pub fn finish(mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
