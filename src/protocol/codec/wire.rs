use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned};

use crate::{PeerId, RaftError};

// ── Protocol constants ────────────────────────────────────────────────────────

pub const RAFT_MAGIC: u32 = 0x5241_4654;
pub const RAFT_VERSION: u8 = 0x01;
pub const RAFT_FRAME_HEADER_SIZE: usize = std::mem::size_of::<RaftFrameHeader>();

pub const KIND_REQUEST_VOTE: u8 = 1;
pub const KIND_REQUEST_VOTE_RESP: u8 = 2;
pub const KIND_APPEND_ENTRIES: u8 = 3;
pub const KIND_APPEND_ENTRIES_RESP: u8 = 4;
pub const KIND_INSTALL_SNAPSHOT: u8 = 5;
pub const KIND_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const KIND_CUSTOM: u8 = 7;
pub const KIND_CUSTOM_RESPONSE: u8 = 8;

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
    pub(crate) _pad: U64,     // [24..32] — aligns header to 32 bytes (power-of-2, half cache line)
}

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

// wire.rs now only contains pure zerocopy-mapped structs
