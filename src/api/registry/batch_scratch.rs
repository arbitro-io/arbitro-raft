//! Reusable scratch buffers for the batched multi-Raft heartbeat tick.
//!
//! `RaftGroupRegistry::tick_heartbeats` runs on a hot periodic path — once
//! per heartbeat interval, regardless of how many Raft groups this node
//! hosts. Without a place to keep buffers between ticks, every tick paid
//! for a fresh `HashMap` plus several `Vec`s just to bucket per-group
//! frames by destination peer. [`BatchScratch`] lives on the registry and
//! is cleared (never reallocated, once warmed up) at the start of each
//! tick instead.
//!
//! No new dependency is introduced here: `PeerId` is a `u64` newtype, but
//! `rustc-hash` is not a workspace dependency, so this uses the standard
//! `HashMap`. The reuse win comes from clearing and refilling the same
//! map/vecs every tick rather than allocating new ones.

use std::collections::HashMap;

use crate::protocol::codec::wire::RAFT_FRAME_HEADER_SIZE;
use crate::protocol::AppendEntries;
use crate::{GroupId, PeerId};

/// One (group, from, body) frame destined for a specific peer, produced
/// while building the batched heartbeat wire for each leader group.
pub(crate) struct FrameOut {
    pub(crate) group_id: GroupId,
    pub(crate) from: PeerId,
    pub(crate) ae: AppendEntries,
}

/// Scratch buffers reused across every `tick_heartbeats` call.
///
/// Callers must `clear()` the fields they use at the top of each tick;
/// capacity built up over previous ticks is retained across calls.
///
/// A per-peer `slices: Vec<&[u8]>` buffer deliberately does NOT live here:
/// its elements borrow from `header_bufs`/`ae_bodies` for the duration of
/// a single vectored send, and keeping that borrow alive across ticks
/// would require unsafe lifetime laundering. It stays a per-tick local in
/// the call site instead.
#[derive(Default)]
pub(crate) struct BatchScratch {
    /// Frames bucketed by destination peer, rebuilt fresh each tick from
    /// every leader group's `build_heartbeat_wire` output.
    pub(crate) per_peer: HashMap<PeerId, Vec<FrameOut>>,
    /// Owned header bytes for the peer currently being sent to. Reused
    /// across peers within a tick and across ticks.
    pub(crate) header_bufs: Vec<[u8; RAFT_FRAME_HEADER_SIZE]>,
    /// Owned `AppendEntries` bodies for the peer currently being sent to.
    pub(crate) ae_bodies: Vec<AppendEntries>,
}
