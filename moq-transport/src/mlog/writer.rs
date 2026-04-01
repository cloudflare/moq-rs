// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use super::Event;

/// Writer for MoQ Transport logs (mlog)
/// Writes JSON-SEQ format compatible with qlog aggregation
pub struct MlogWriter {
    writer: BufWriter<File>,
    start_time: Instant,
}

impl MlogWriter {
    /// Create a new mlog writer for the given file path
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        let start_time = Instant::now();

        // Write qlog-compatible header as first record
        // This follows qlog JSON-SEQ format (RFC 7464) per
        // draft-ietf-quic-qlog-main-schema-13 Section 5
        let header = serde_json::json!({
            "file_schema": "urn:ietf:params:qlog:file:sequential",
            "serialization_format": "JSON-SEQ",
            "title": "moq-relay",
            "description": "MoQ Transport events",
            "trace": {
                "vantage_point": {
                    "type": "server"
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

        Ok(Self { writer, start_time })
    }

    /// Get elapsed time in milliseconds since connection start
    pub fn elapsed_ms(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64() * 1000.0
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
