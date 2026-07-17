//! arbitro-raft — a Raft consensus engine.
//!
//! # Panic policy (B13)
//!
//! This library's contract is **zero panics reachable from network input**:
//!
//! * Every inbound frame is length- and shape-validated by the wire codec
//!   before any typed view is taken; malformed or hostile bytes surface as a
//!   [`RaftError`] classified [`ErrorClass::BadFrame`] and are dropped by the
//!   run loop — they never panic and never terminate the node (B7/B9).
//! * The `unwrap`/`expect` calls that remain in `src/` are internal invariant
//!   postconditions (length-checked slice conversions, views validated at
//!   construction, slots populated by the same function that reads them).
//!   Each site carries a targeted `#[allow]` with its justification; the
//!   crate-level `warn(clippy::unwrap_used, clippy::expect_used)` lint below
//!   (with `allow-unwrap-in-tests` in `clippy.toml`) makes any NEW unwrap or
//!   expect in non-test code a visible clippy warning so it cannot land
//!   unreviewed. This ratchets to `deny` together with the CI clippy job's
//!   planned `-D warnings` tightening (see `.github/workflows/ci.yml`, J1).
//! * `debug_assert!` is used only for engine-internal invariants whose inputs
//!   are derived from local state, never raw peer bytes. Audited instance:
//!   `RaftNode::set_last_applied` asserts `idx <= commit_index`; its callers
//!   are the apply loop (bounded by `commit_index` in its loop condition) and
//!   the snapshot paths, which raise `commit_index` to the snapshot boundary
//!   before calling it (`src/api/node/snapshot_install.rs`). These asserts
//!   compile out in release builds.
//!
//! **Panic strategy**: as a library this crate sets no `panic=` strategy and
//! works under both `unwind` and `abort`. It installs no `catch_unwind`
//! boundary: a panic is by definition a bug, and [`ArbitroRaft::run`] /
//! [`ArbitroRaft::run_once`] must NOT be relied upon to catch or survive one —
//! a panic propagates to the caller's task/runtime (under tokio, it aborts
//! that task and surfaces via the `JoinHandle`). Embedders that need
//! crash-containment should supervise the task running the node, not expect
//! the engine to absorb panics.

// B13: panic-policy enforcement — see the crate docs above. Test code is
// exempt via `allow-unwrap-in-tests`/`allow-expect-in-tests` in clippy.toml.
#![warn(clippy::unwrap_used, clippy::expect_used)]

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
    RaftMessage, RequestVote, RequestVoteResp, TimeoutNow, KIND_APPEND_ENTRIES,
    KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT,
    KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP, KIND_TIMEOUT_NOW,
    RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};
pub use state::Role;
pub use state::{HardState, SnapshotMeta, SoftState};
pub use traits::{Clock, NoopStateMachine, RaftStorage, RaftTransport, StateMachine};
pub use types::{ClusterId, GroupId, LeaderHint, LogIndex, PeerId, Term};
pub use validation::validate_node_config;
