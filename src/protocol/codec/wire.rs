use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// ── Protocol constants ────────────────────────────────────────────────────────

pub const RAFT_MAGIC: u32 = 0x5241_4654;
pub const RAFT_VERSION: u8 = 0x01;
pub const RAFT_FRAME_HEADER_SIZE: usize = std::mem::size_of::<RaftFrameHeader>();

/// Maximum size of a single wire frame, in bytes. The receiver's `inbound_buf`
/// is sized to exactly this, so any frame larger than it can never be received
/// (P1-4). Client payloads are validated against [`MAX_ENTRY_PAYLOAD`] at
/// propose time so an over-large entry is rejected up front instead of
/// committing on the leader and then silently failing to replicate.
pub const MAX_FRAME_SIZE: usize = 64 * 1024;

/// Maximum size of a single client payload (one log entry), in bytes. Leaves
/// [`MAX_FRAME_SIZE`] minus the frame header, the `AppendEntries` body, and the
/// per-entry header — a conservative fixed margin so a lone entry always fits a
/// peer's frame buffer with room to spare.
pub const MAX_ENTRY_PAYLOAD: usize = MAX_FRAME_SIZE - 512;

pub const KIND_REQUEST_VOTE: u8 = 1;
pub const KIND_REQUEST_VOTE_RESP: u8 = 2;
pub const KIND_APPEND_ENTRIES: u8 = 3;
pub const KIND_APPEND_ENTRIES_RESP: u8 = 4;
pub const KIND_INSTALL_SNAPSHOT: u8 = 5;
pub const KIND_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const KIND_CUSTOM: u8 = 7;
pub const KIND_CUSTOM_RESPONSE: u8 = 8;
pub const KIND_APPEND_ENTRIES_SEEDED: u8 = 9;
pub const KIND_PRE_VOTE: u8 = 10;
pub const KIND_PRE_VOTE_RESP: u8 = 11;
pub const KIND_TIMEOUT_NOW: u8 = 12;

// ── Wire structs ──────────────────────────────────────────────────────────────

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub(crate) struct RaftFrameHeader {
    pub(crate) magic: U32,    // [0..4]
    pub(crate) version: u8,   // [4]
    pub(crate) kind: u8,      // [5]
    pub(crate) flags: U16,    // [6..8]
    pub(crate) from: U64,     // [8..16]
    pub(crate) body_len: U32, // [16..20]
    pub(crate) reserved: U32, // [20..24]
    pub(crate) group_id: U64, // [24..32] — was _pad; GroupId(0) = default single-group
}

const _: () = assert!(RAFT_FRAME_HEADER_SIZE == 32);

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct RequestVote {
    pub term: U64,
    pub candidate_id: U64,
    pub last_log_index: U64,
    pub last_log_term: U64,
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct RequestVoteResp {
    pub term: U64,
    pub vote_granted: u8,
    pub _pad: [u8; 7],
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct AppendEntries {
    pub term: U64,
    pub leader_id: U64,
    pub prev_log_index: U64,
    pub prev_log_term: U64,
    pub leader_commit: U64,
    pub entry_count: U32,
    /// Reserved word, doubling as the A11 ReadIndex probe token: a leader
    /// confirming its quorum for a linearizable read stamps its current
    /// probe sequence here; the follower echoes it verbatim into
    /// `AppendEntriesResp._pad[0..4]`. `0` = no probe (the historical
    /// padding value), which a confirmation round never counts — so frames
    /// from older senders degrade SAFE (reads fail, never go stale).
    /// The field keeps its `_pad` name for wire/API compatibility.
    pub _pad: U32,
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct EntryHeader {
    pub term: U64,
    pub index: U64,
    pub payload_len: U32,
    pub _pad: U32,
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct AppendEntriesResp {
    pub term: U64,
    pub match_index: U64,
    pub success: u8,
    /// Reserved bytes; `[0..4]` echo the request's `AppendEntries._pad`
    /// (little-endian) — the A11 ReadIndex probe token. This is what lets a
    /// leader distinguish an ack generated AFTER its confirmation round began
    /// from a stale ack that was already buffered/in flight: only echoes
    /// matching the round's freshly-bumped sequence count toward the read
    /// quorum. All-zero (older peers) never matches a live round — safe.
    /// The field keeps its `_pad` name for wire/API compatibility.
    pub _pad: [u8; 7],
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct InstallSnapshot {
    pub term: U64,
    pub leader_id: U64,
    pub last_included_index: U64,
    pub last_included_term: U64,
    pub offset: U64,
    pub chunk_len: U32,
    pub done: u8,
    pub _pad: [u8; 3],
}

#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct InstallSnapshotResp {
    pub term: U64,
    pub next_offset: U64,
    pub accepted: u8,
    pub _pad: [u8; 7],
}

/// Leadership transfer (Raft §4.2.3): the leader tells a fully-caught-up
/// target to start an election IMMEDIATELY — bypassing both the election
/// timeout and the pre-vote phase (the transfer is leader-sanctioned, so
/// pre-vote's disruption guard does not apply).
///
/// Body is 16 bytes after the standard 32-byte [`RaftFrameHeader`]:
///
/// | Bytes     | Field       | Notes                                      |
/// |-----------|-------------|--------------------------------------------|
/// | `[0..8]`  | `term`      | Sender's current term (`u64` LE). A target |
/// |           |             | ignores the sanction if this is stale.     |
/// | `[8..16]` | `leader_id` | The transferring leader's id (`u64` LE).   |
#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct TimeoutNow {
    pub term: U64,
    pub leader_id: U64,
}

// wire.rs now only contains pure zerocopy-mapped structs
