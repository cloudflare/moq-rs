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
    start_time: Instant,
}

impl MlogWriter {
    /// Create a new mlog writer for the given file path
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        let start_time = Instant::now();
        // Wall-clock time at `start_time`, in Unix epoch milliseconds. Sampled
        // once, next to `start_time`; event times stay monotonic. Rounded to
        // whole microseconds: an f64 this large resolves only ~0.24 us, so
        // finer digits would be noise.
        let reference_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_micros() as f64 / 1000.0);

        // Event times are milliseconds since `start_time`. qlog 0.3 reads a
        // missing time_format as "absolute", so declare "relative" and anchor
        // it with reference_time.
        let mut common_fields = serde_json::json!({ "time_format": "relative" });
        if let Some(reference_time) = reference_time {
            common_fields["reference_time"] = reference_time.into();
        }

        // Write qlog-compatible header as first record
        // This follows qlog JSON-SEQ format (RFC 7464)
        let header = serde_json::json!({
            "qlog_version": "0.3",
            "qlog_format": "JSON-SEQ",
            "title": "moq-relay",
            "description": "MoQ Transport events",
            "trace": {
                "common_fields": common_fields,
                "vantage_point": {
                    "type": "server"
                },
                "event_schemas": [
                    "urn:ietf:params:qlog:events:loglevel",
                    "urn:ietf:params:qlog:events:moqt"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_declares_relative_times_anchored_to_wall_clock() {
        let path = std::env::temp_dir().join(format!(
            "mlog-header-test-{}-{}.mlog",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        // Same microsecond rounding as the writer, so the bounds can't be
        // finer than the value they bracket.
        let now_ms = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as f64
                / 1000.0
        };

        let before = now_ms();
        MlogWriter::new(&path).unwrap().finish().unwrap();
        let after = now_ms();

        let contents = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        let record = contents
            .strip_prefix('\x1e')
            .and_then(|s| s.lines().next())
            .expect("header record");
        let header: serde_json::Value = serde_json::from_str(record).unwrap();
        let common = &header["trace"]["common_fields"];

        assert_eq!(common["time_format"], "relative");
        let reference_time = common["reference_time"].as_f64().expect("reference_time");
        assert!(
            (before..=after).contains(&reference_time),
            "reference_time {reference_time} not within [{before}, {after}]"
        );

        // Whole microseconds: no more than three digits after the point.
        let text = common["reference_time"].to_string();
        let decimals = text.split_once('.').map_or(0, |(_, frac)| frac.len());
        assert!(decimals <= 3, "reference_time {text} finer than 1 us");
    }
}
