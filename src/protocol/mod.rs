mod codec;
mod message;
mod view;

pub(crate) use codec::encode_append_entries_frame;

pub use codec::{
    decode_message, decode_message_view, encode_message, encode_message_into, KIND_CUSTOM, KIND_CUSTOM_RESPONSE, RAFT_FRAME_HEADER_SIZE,
    RAFT_MAGIC, RAFT_VERSION,
};
pub use message::{
    AppendEntries, AppendEntriesResp, InboundRaftMessage, InstallSnapshot, InstallSnapshotResp,
    RaftCustomMessage, RaftCustomResponse, RaftMessage, RequestVote, RequestVoteResp,
    SnapshotChunk,
};
pub use view::{
    AppendEntriesRespView, AppendEntriesView, EntryView, InboundRaftMessageView,
    InstallSnapshotRespView, InstallSnapshotView, RaftCustomMessageView,
    RaftCustomResponseView, RaftMessageView, RequestVoteRespView, RequestVoteView,
};
