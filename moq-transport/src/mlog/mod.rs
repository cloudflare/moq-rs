//! MoQ Transport logging (mlog) following qlog patterns
//!
//! Based on draft-pardue-moq-qlog-moq-events but adapted for MoQ Transport draft-14
//! This creates qlog-compatible JSON-SEQ files that can be aggregated with QUIC qlog files

mod writer;
pub use writer::MlogWriter;

pub mod events;
pub use events::{
    client_setup_parsed, loglevel_event, object_datagram_created, object_datagram_parsed,
    server_setup_created, subgroup_header_created, subgroup_header_parsed, subgroup_object_created,
    subgroup_object_ext_created, subgroup_object_ext_parsed, subgroup_object_parsed, Event,
    EventData, LogLevel,
};
