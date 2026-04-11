pub mod decode;
pub mod encode;
pub mod wire;
pub mod view;

pub(crate) use decode::validate_append_entries_body;
pub use decode::{
    decode_message,
};
pub use view::AppendEntriesView;

pub use encode::{
    encode_append_entries_vectored, encode_message_to_bytes, encode_message_vectored,
};

pub use wire::{
    AppendEntries, AppendEntriesResp, EntryHeader, InstallSnapshot, InstallSnapshotResp,
    RequestVote, RequestVoteResp,
    KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM,
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
