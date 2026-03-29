mod decode;
mod encode;
pub(crate) mod wire;

pub(crate) use decode::validate_append_entries_body;
pub use decode::{
    decode_message, decode_message_view, parse_append_entries_resp_view, parse_append_entries_view,
    parse_custom_message_view, parse_custom_response_view, parse_install_snapshot_resp_view,
    parse_install_snapshot_view, parse_raft_frame_view, parse_request_vote_resp_view,
    parse_request_vote_view,
};
pub use encode::{encode_message, encode_message_into};
pub(crate) use encode::encode_append_entries_frame;
pub(crate) use wire::{AppendEntriesBody, EntryHeader};
pub use wire::{
    EntryHeaderView, KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM,
    KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE,
    KIND_REQUEST_VOTE_RESP, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};

use crate::PeerId;
use std::sync::OnceLock;

pub(crate) fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

pub(crate) fn trace_log(from: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        tracing::trace!(node_id = from.0, msg = msg.as_ref(), "raft-codec");
    }
}
