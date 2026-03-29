use bytes::Bytes;

use crate::{
    InstallSnapshot, InstallSnapshotResp, LogIndex, PeerId, RaftError, SnapshotChunk, SnapshotMeta,
    Term,
};
use crate::protocol::codec::wire::RaftFrameView;

// ── InstallSnapshotView ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InstallSnapshotView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl InstallSnapshotView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self { Self { from, frame } }

    #[inline] pub fn from(&self) -> PeerId      { self.from }
    #[inline] pub fn term(&self) -> Term        { Term(self.frame.body_u64(0)) }
    #[inline] pub fn leader_id(&self) -> PeerId { PeerId(self.frame.body_u64(8)) }
    #[inline] pub fn offset(&self) -> u64       { self.frame.body_u64(32) }
    #[inline] pub fn chunk_len(&self) -> usize  { self.frame.body_u32(40) as usize }
    #[inline] pub fn done(&self) -> bool        { self.frame.body_byte(44) != 0 }

    #[inline]
    pub fn meta(&self) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(self.frame.body_u64(16)),
            last_included_term:  Term(self.frame.body_u64(24)),
        }
    }

    #[inline]
    pub fn chunk_bytes(&self) -> Bytes {
        self.frame.frame().slice(
            (self.frame.body_offset() + 48)
                ..(self.frame.body_offset() + 48 + self.chunk_len()),
        )
    }

    pub fn to_owned(&self) -> InstallSnapshot {
        InstallSnapshot {
            term:      self.term(),
            leader_id: self.leader_id(),
            meta:      self.meta(),
            chunk:     SnapshotChunk {
                offset: self.offset(),
                bytes:  self.chunk_bytes(),
                done:   self.done(),
            },
        }
    }
}

// ── InstallSnapshotRespView ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InstallSnapshotRespView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl InstallSnapshotRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self { Self { from, frame } }

    #[inline] pub fn from(&self) -> PeerId        { self.from }
    #[inline] pub fn term(&self) -> Term          { Term(self.frame.body_u64(0)) }
    #[inline] pub fn next_offset(&self) -> u64    { self.frame.body_u64(8) }
    #[inline] pub fn accepted(&self) -> bool      { self.frame.body_byte(16) != 0 }

    pub fn to_owned(&self) -> InstallSnapshotResp {
        InstallSnapshotResp {
            term:        self.term(),
            accepted:    self.accepted(),
            next_offset: self.next_offset(),
        }
    }
}

// Silence unused import
#[allow(unused_imports)]
use RaftError as _;
