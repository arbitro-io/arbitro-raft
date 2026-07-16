pub mod api;
pub mod config;
pub mod dispatch;
pub mod election;
pub mod entry;
pub mod error;
pub mod protocol;
pub mod state;
pub mod traits;
pub mod types;
pub mod validation;

pub use api::node::membership::{
    ConfigChangeEntry, ConfigChangePhase, CONFIG_CHANGE_MAGIC, CONFIG_CHANGE_VERSION,
};
pub use api::{
    ArbitroRaft, ClientHandle, CommitIndexObserver, RaftCustomRegistry, RaftMetrics,
    RaftMetricsSnapshot, RaftNode, RaftStatus,
};
pub use config::BootstrapPeer;
pub use config::{LimitsConfig, NodeConfig, TimingConfig};
pub use dispatch::{
    encode_dispatch_response, DispatchAckOptions, DispatchAckPolicy, DispatchBuilder,
    DispatchContextView, DispatchEnvelope, DispatchFailOptions, DispatchFailPolicy,
    DispatchFailure, DispatchHandle, DispatchNodeRole, DispatchOptions, DispatchPeerResult,
    DispatchPeerState, DispatchRequester, DispatchResponder, DispatchResponse,
    DispatchResponseKind, DispatchResponseRef, DispatchResponseView, DispatchResult, DispatchRoute,
    DispatchScope, DispatchSpec, DispatchStreamView, DispatchTx, DispatchTxResponder, DispatchView,
    RAFT_DISPATCH_FRAME_HEADER_SIZE, RAFT_DISPATCH_MAGIC, RAFT_DISPATCH_RESPONSE_FRAME_HEADER_SIZE,
    RAFT_DISPATCH_RESPONSE_MAGIC, RAFT_DISPATCH_RESPONSE_VERSION, RAFT_DISPATCH_VERSION,
};
pub use entry::{EntryPayload, LogEntry};
pub use error::{ErrorClass, RaftError};
pub use protocol::{
    decode_message, encode_append_entries_vectored, encode_message_to_bytes,
    encode_message_vectored, encode_message_vectored_with_group, AppendEntries,
    AppendEntriesResp, EntryHeader, InboundRaftMessage, InstallSnapshot, InstallSnapshotResp,
    RaftMessage, RequestVote, RequestVoteResp, KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP,
    KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP,
    KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};
pub use state::Role;
pub use state::{HardState, SnapshotMeta, SoftState};
pub use traits::{Clock, NoopStateMachine, RaftStorage, RaftTransport, StateMachine};
pub use types::{ClusterId, GroupId, LeaderHint, LogIndex, PeerId, Term};
pub use validation::validate_node_config;
