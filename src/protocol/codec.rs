pub mod decode;
pub mod encode;
pub mod wire;
pub mod view;

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
    KIND_REQUEST_VOTE_RESP, KIND_PRE_VOTE, KIND_PRE_VOTE_RESP, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC,
    RAFT_VERSION,
};
