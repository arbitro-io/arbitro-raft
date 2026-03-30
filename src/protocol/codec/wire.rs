use bytes::Bytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref};

use crate::{PeerId, RaftError};

// ── Protocol constants ────────────────────────────────────────────────────────

pub const RAFT_MAGIC:            u32   = 0x5241_4654;
pub const RAFT_VERSION:          u8    = 0x01;
pub const RAFT_FRAME_HEADER_SIZE: usize = std::mem::size_of::<RaftFrameHeader>();

pub const KIND_REQUEST_VOTE:         u8 = 1;
pub const KIND_REQUEST_VOTE_RESP:    u8 = 2;
pub const KIND_APPEND_ENTRIES:       u8 = 3;
pub const KIND_APPEND_ENTRIES_RESP:  u8 = 4;
pub const KIND_INSTALL_SNAPSHOT:     u8 = 5;
pub const KIND_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const KIND_CUSTOM:               u8 = 7;
pub const KIND_CUSTOM_RESPONSE:      u8 = 8;

// ── Wire structs ──────────────────────────────────────────────────────────────

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RaftFrameHeader {
    pub(crate) magic:    U32,     // [0..4]
    pub(crate) version:  u8,      // [4]
    pub(crate) kind:     u8,      // [5]
    pub(crate) flags:    U16,     // [6..8]
    pub(crate) from:     U64,     // [8..16]
    pub(crate) body_len: U32,     // [16..20]
    pub(crate) reserved: U32,     // [20..24]
    pub(crate) _pad:     U64,     // [24..32] — aligns header to 32 bytes (power-of-2, half cache line)
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RequestVoteBody {
    pub(crate) term:           U64,
    pub(crate) candidate_id:   U64,
    pub(crate) last_log_index: U64,
    pub(crate) last_log_term:  U64,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RequestVoteRespBody {
    pub(crate) term:        U64,
    pub(crate) vote_granted: u8,
    pub(crate) _pad:        [u8; 7],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct AppendEntriesBody {
    pub(crate) term:           U64,
    pub(crate) leader_id:      U64,
    pub(crate) prev_log_index: U64,
    pub(crate) prev_log_term:  U64,
    pub(crate) leader_commit:  U64,
    pub(crate) entry_count:    U32,
    pub(crate) _pad:           U32,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct EntryHeader {
    pub(crate) term:        U64,
    pub(crate) index:       U64,
    pub(crate) payload_len: U32,
    pub(crate) _pad:        U32,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct AppendEntriesRespBody {
    pub(crate) term:        U64,
    pub(crate) match_index: U64,
    pub(crate) success:     u8,
    pub(crate) _pad:        [u8; 7],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct InstallSnapshotBody {
    pub(crate) term:                 U64,
    pub(crate) leader_id:            U64,
    pub(crate) last_included_index:  U64,
    pub(crate) last_included_term:   U64,
    pub(crate) offset:               U64,
    pub(crate) chunk_len:            U32,
    pub(crate) done:                 u8,
    pub(crate) _pad:                 [u8; 3],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct InstallSnapshotRespBody {
    pub(crate) term:        U64,
    pub(crate) next_offset: U64,
    pub(crate) accepted:    u8,
    pub(crate) _pad:        [u8; 7],
}

// ── RaftFrameView ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RaftFrameView {
    pub(crate) frame: Bytes,
}

impl RaftFrameHeader {
    #[inline]
    pub(crate) fn ref_from_prefix(buf: &[u8]) -> Option<(&Self, &[u8])> {
        Ref::<_, Self>::from_prefix(buf)
            .ok()
            .map(|(r, rest)| (Ref::into_ref(r), rest))
    }
}

impl RaftFrameView {
    #[inline]
    pub fn from(&self) -> PeerId {
        let (header, _) = RaftFrameHeader::ref_from_prefix(&self.frame).unwrap();
        PeerId(header.from.get())
    }

    #[inline]
    pub fn kind(&self) -> u8 {
        let (header, _) = RaftFrameHeader::ref_from_prefix(&self.frame).unwrap();
        header.kind
    }

    #[inline]
    pub fn body(&self) -> &[u8] { &self.frame[RAFT_FRAME_HEADER_SIZE..] }

    #[inline]
    pub fn frame(&self) -> &Bytes { &self.frame }

    #[inline]
    pub fn body_offset(&self) -> usize { RAFT_FRAME_HEADER_SIZE }

    #[inline]
    pub fn body_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.body()[offset..offset + 8].try_into().unwrap())
    }

    #[inline]
    pub fn body_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.body()[offset..offset + 4].try_into().unwrap())
    }

    #[inline]
    pub fn body_byte(&self, offset: usize) -> u8 { self.body()[offset] }
}

// ── EntryHeaderView ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct EntryHeaderView<'a> {
    body:          &'a [u8],
    header_offset: usize,
    payload_start: usize,
    payload_end:   usize,
}

impl<'a> EntryHeaderView<'a> {
    pub fn parse(body: &'a [u8], header_offset: usize) -> Result<Self, RaftError> {
        let (header, _) = parse_prefix::<EntryHeader>(&body[header_offset..], "append entry header")?;
        let payload_start = header_offset + std::mem::size_of::<EntryHeader>();
        let payload_end   = payload_start
            .checked_add(header.payload_len.get() as usize)
            .ok_or_else(|| RaftError::Protocol("entry payload overflow".into()))?;
        if payload_end > body.len() {
            return Err(RaftError::Protocol("short append entry payload".into()));
        }
        Ok(Self { body, header_offset, payload_start, payload_end })
    }

    #[inline]
    fn header(&self) -> &'a EntryHeader {
        let (header, _) = Ref::<_, EntryHeader>::from_prefix(&self.body[self.header_offset..])
            .ok()
            .map(|(r, rest)| (Ref::into_ref(r), rest))
            .unwrap();
        header
    }

    #[inline]
    pub fn term(&self) -> u64 { self.header().term.get() }

    #[inline]
    pub fn index(&self) -> u64 { self.header().index.get() }

    #[inline]
    pub fn payload_start(&self) -> usize { self.payload_start }

    #[inline]
    pub fn payload_end(&self) -> usize { self.payload_end }

    #[inline]
    pub fn header_offset(&self) -> usize { self.header_offset }
}

// ── Shared parse helper ───────────────────────────────────────────────────────

/// Zero-copy prefix parse — returns `(&T, rest)` or a `Protocol` error.
pub(crate) fn parse_prefix<'a, T>(buf: &'a [u8], name: &str) -> Result<(&'a T, &'a [u8]), RaftError>
where
    T: FromBytes + KnownLayout + Immutable,
{
    Ref::<_, T>::from_prefix(buf)
        .ok()
        .map(|(r, rest)| (Ref::into_ref(r), rest))
        .ok_or_else(|| RaftError::Protocol(format!("short {}", name)))
}
