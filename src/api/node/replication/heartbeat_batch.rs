//! Multi-Raft batched heartbeat fan-out.
//!
//! In single-group mode `send_heartbeat_once` emits one AppendEntries-with-
//! zero-entries frame per peer per tick. Once a physical node hosts N Raft
//! groups the naive path multiplies that to `N * peers` frames per tick, and
//! more importantly `N` distinct `send_vectored` syscalls per peer per tick.
//!
//! [`send_batched_heartbeats`] coalesces the fan-out at the socket-write
//! layer: it produces one on-wire AppendEntries frame per (peer, group), but
//! groups all frames destined for the same peer into a single
//! [`RaftTransport::send_vectored`] call. The OS sees ONE syscall / one TCP
//! segment per peer per tick regardless of group count, while each frame
//! stays wire-compatible with the existing per-group codec so the receiving
//! [`RaftGroupRegistry`] can still demultiplex by the `group_id` field
//! stamped into the frame header.
//!
//! **Threading contract (Sprint 1):** this function is only safe to call from
//! a single-threaded tick that owns exclusive access to every `RaftNode` in
//! `groups`. It does not (and cannot) coordinate leadership transitions
//! happening concurrently in another task. Leadership is rechecked per group
//! while building wires and non-leader groups are skipped silently, but the
//! caller must not race this function with the group's own handler loop.
//!
//! [`RaftGroupRegistry`]: crate::api::registry::RaftGroupRegistry

use std::sync::Arc;

use tracing::warn;
use zerocopy::IntoBytes;

use crate::api::registry::{BatchScratch, FrameOut};
use crate::api::RaftNode;
use crate::protocol::codec::wire::{
    RaftFrameHeader, KIND_APPEND_ENTRIES, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};
use crate::{GroupId, PeerId, RaftError, RaftStorage, RaftTransport};

/// Send heartbeats for every group in `groups` to every peer, coalescing
/// per-peer sends into a single vectored write.
///
/// Only groups where `node.is_leader()` are considered (followers do not
/// heartbeat). All frames stamp `group_id` into the frame header so the
/// receiving [`RaftGroupRegistry`] can demultiplex by group.
///
/// One `transport.send_vectored` call is issued per unique peer per
/// invocation — NOT per (peer, group).
///
/// Returns the total number of frames enqueued (equals the sum, over each
/// group that led, of the number of non-self peers in that group).
///
/// `scratch` holds the reusable per-tick buffers (see [`BatchScratch`]) —
/// callers own one instance (typically on [`RaftGroupRegistry`]) and pass
/// it in by mutable reference so this hot periodic path does not allocate
/// a fresh `HashMap`/`Vec`s every tick. `expected_leader_groups` sizes the
/// initial per-peer `Vec` on first insert (a cheap hint, not a hard cap —
/// e.g. the registry's group count).
///
/// # Contract
///
/// Sprint 1 assumes single-threaded caller ownership of every node in
/// `groups`. See the module docs for the full threading contract.
///
/// [`RaftGroupRegistry`]: crate::api::registry::RaftGroupRegistry
pub async fn send_batched_heartbeats<'a, S, T, I>(
    groups: I,
    scratch: &mut BatchScratch,
    transport: &Arc<T>,
    expected_leader_groups: usize,
) -> Result<usize, RaftError>
where
    S: RaftStorage + 'a,
    T: RaftTransport + 'a,
    I: IntoIterator<Item = (GroupId, &'a mut RaftNode<S, T>)>,
{
    // ── Phase 1: build wires, bucket per destination peer ────────────────
    scratch.per_peer.clear();
    let mut group_errors = 0usize;

    for (gid, node) in groups {
        if !node.is_leader() {
            continue;
        }
        let from = node.node_id();

        // Snapshot the peer list into the node's own scratch buffer (the
        // same buffer send_heartbeat_once reuses) before the mutable
        // borrow needed by build_heartbeat_wire. Skip self. Read directly
        // off `node.config.peers` (not through a method) so this borrows
        // only that field, leaving `node.scratch_peers` free to be
        // borrowed mutably at the same time.
        node.scratch_peers.clear();
        // A13: learners receive batched heartbeats like followers.
        node.scratch_peers.extend(
            node.config
                .peers
                .iter()
                .chain(node.config.learners.iter())
                .copied()
                .filter(|p| *p != from),
        );

        for i in 0..node.scratch_peers.len() {
            let peer = node.scratch_peers[i];
            // build_heartbeat_wire rechecks is_leader defensively; a group
            // that stepped down mid-iteration returns None and is skipped.
            // A storage error building this group's wire must not abort
            // heartbeats for other groups or other peers of this group —
            // log and move on instead of propagating.
            match node.build_heartbeat_wire(peer) {
                Ok(Some(ae)) => {
                    scratch
                        .per_peer
                        .entry(peer)
                        .or_insert_with(|| Vec::with_capacity(expected_leader_groups))
                        .push(FrameOut {
                            group_id: gid,
                            from,
                            ae,
                        });
                }
                Ok(None) => {
                    // Not the leader anymore for this group — stop
                    // emitting frames for it entirely (subsequent peers
                    // would also skip, but the check is cheap).
                    break;
                }
                Err(e) => {
                    group_errors += 1;
                    warn!(
                        group_id = gid.0,
                        peer = peer.0,
                        error = %e,
                        "failed to build heartbeat wire for group; skipping peer"
                    );
                    continue;
                }
            }
        }
    }

    // ── Phase 2: one coalesced send_vectored per peer ────────────────────
    let mut total = 0usize;
    let mut peer_errors = 0usize;
    for (peer, frames) in scratch.per_peer.iter() {
        // Owned storage for headers (32 bytes each) and AppendEntries bodies
        // (48 bytes each), reused across peers/ticks via `scratch`. Both are
        // held stable in this scope so the borrows stored in `slices`
        // remain valid across the transport await.
        scratch.header_bufs.clear();
        scratch.ae_bodies.clear();

        let body_len = std::mem::size_of::<crate::protocol::AppendEntries>() as u32;
        for f in frames {
            let mut hbuf = [0u8; RAFT_FRAME_HEADER_SIZE];
            write_frame_header(&mut hbuf, f.from, f.group_id, KIND_APPEND_ENTRIES, body_len);
            scratch.header_bufs.push(hbuf);
            scratch.ae_bodies.push(f.ae);
        }

        // Build the final slice list borrowing from scratch.header_bufs +
        // scratch.ae_bodies (both stable in this scope). Two slices per
        // frame: header, body. This Vec is not itself kept in `scratch`:
        // its elements borrow from the two buffers above for the duration
        // of this send only, so caching it across ticks would require
        // storing a live borrow across an await point on the next tick —
        // not sound without unsafe lifetime laundering.
        let mut slices: Vec<&[u8]> = Vec::with_capacity(frames.len() * 2);
        for i in 0..frames.len() {
            slices.push(&scratch.header_bufs[i][..]);
            slices.push(scratch.ae_bodies[i].as_bytes());
        }

        // A single peer's transport error must not skip the remaining
        // peers — same tolerant semantics as send_heartbeat_once, which
        // swallows individual send failures via `send_message`.
        match transport.send_vectored(*peer, &slices).await {
            Ok(()) => total += frames.len(),
            Err(e) => {
                peer_errors += 1;
                warn!(peer = peer.0, error = %e, "failed to send batched heartbeat to peer");
            }
        }
    }

    if group_errors > 0 || peer_errors > 0 {
        warn!(
            group_errors,
            peer_errors, total, "batched heartbeats completed with partial failures"
        );
    }

    Ok(total)
}

/// Write a 32-byte Raft frame header directly into `buf`.
///
/// Mirrors the layout produced by `encode_message_vectored_with_group`
/// without pulling in the vectored-encoder machinery — the batched
/// heartbeat path only ever emits AppendEntries frames with zero entries,
/// so the general path's payload-splicing logic is unnecessary.
///
/// Layout (little-endian for multi-byte fields):
///
/// ```text
/// [0..4]   magic:    u32 = RAFT_MAGIC
/// [4]      version:  u8  = RAFT_VERSION
/// [5]      kind:     u8  = KIND_APPEND_ENTRIES
/// [6..8]   flags:    u16 = 0
/// [8..16]  from:     u64 = leader PeerId
/// [16..20] body_len: u32 = 48 (sizeof AppendEntries)
/// [20..24] reserved: u32 = 0
/// [24..32] group_id: u64 = GroupId
/// ```
#[inline]
fn write_frame_header(
    buf: &mut [u8; RAFT_FRAME_HEADER_SIZE],
    from: PeerId,
    group_id: GroupId,
    kind: u8,
    body_len: u32,
) {
    // Prefer the typed zerocopy view to keep this in lock-step with the
    // wire struct definition — any future field reshuffle stays a compile
    // error rather than a silent wire-format drift.
    use zerocopy::Ref;
    // B13: the buffer is exactly RAFT_FRAME_HEADER_SIZE by the parameter type;
    // the zerocopy prefix take cannot fail.
    #[allow(clippy::expect_used)]
    let (h_ref, _) = Ref::<_, RaftFrameHeader>::from_prefix(&mut buf[..])
        .expect("32-byte buffer aligns to RaftFrameHeader by construction");
    let header = Ref::into_mut(h_ref);
    header.magic.set(RAFT_MAGIC);
    header.version = RAFT_VERSION;
    header.kind = kind;
    header.flags.set(0);
    header.from.set(from.0);
    header.body_len.set(body_len);
    header.reserved.set(0);
    header.group_id.set(group_id.0);
}
