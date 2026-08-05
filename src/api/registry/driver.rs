//! Multi-Raft per-core run-driver (H7).
//!
//! One [`MultiRaftDriver`] runs N Raft groups share-nothing on one core/task:
//! it receives each inbound frame from the shared transport, routes it to the
//! owning group by `group_id` (O(1), one multiplicative hash), steps that
//! group, and applies its committed entries — with zero allocation and zero
//! locks on the route→step path.
//!
//! # Memory diet (H6)
//!
//! The driver owns ONE set of per-core scratch buffers ([`CoreScratch`],
//! ~33 MiB). A group borrows them only while it is being stepped: an RAII
//! [`ScratchLend`] `mem::swap`s the shared buffers into the node's scratch
//! fields before every node operation and its `Drop` swaps them back out
//! (three pointer-triple swaps each way, no copy, no allocation). On
//! [`add_group`](MultiRaftDriver::add_group)
//! the node's own MB-class buffers (`scratch_payload` 16 MiB,
//! `scratch_outbound` 1 MiB, `scratch_quorum_buf` 64 KiB) are freed, so an
//! IDLE group's footprint is only its KB-class capacity docks and maps —
//! roughly 100 KiB (entry dock 32 KiB + iovec dock 32 KiB + payload-ref dock
//! 16 KiB + index/peer vecs + small maps). Total big-buffer memory is
//! O(cores), not O(groups). [`remove_group`](MultiRaftDriver::remove_group)
//! restores the standard buffers so the returned node works standalone.
//!
//! # Inbound validation (H9)
//!
//! A frame whose `group_id` names no group in this driver is counted
//! ([`unknown_group_frames`](MultiRaftDriver::unknown_group_frames)) and
//! dropped before decode — it is never routed to another group and never
//! panics.
//!
//! # Cancellation
//!
//! Every lend of the shared scratch to a node is held by an RAII guard
//! ([`ScratchLend`]): construction swaps the buffers into the node, `Drop`
//! swaps them back out. Because the guard lives in the async fn's frame, a
//! `run_once` / `propose` / `campaign` future dropped at any `.await` inside
//! the lend window (e.g. cancelled by `select!` or a timeout) still returns
//! the buffers to the driver — the node is left with its empty diet vecs and
//! `group_idle_scratch_bytes` stays 0. Cancellation costs no memory; it can
//! only abandon the in-flight protocol step, which Raft already tolerates.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::transport::multiplex::{frame_group, MultiplexDemux, MultiplexedTransport};
use crate::{
    GroupId, LogIndex, NodeConfig, RaftError, RaftNode, RaftStorage, RaftTransport, StateMachine,
};

use super::GroupIdMap;

/// Matches `RaftNode::new`'s `scratch_payload` (storage-read scratch).
const PAYLOAD_SCRATCH: usize = 16 * 1024 * 1024;
/// Matches `RaftNode::new`'s `scratch_outbound` (outbound encode scratch).
const OUTBOUND_SCRATCH: usize = 1024 * 1024;
/// Matches `RaftNode::new`'s `scratch_quorum_buf`.
const QUORUM_SCRATCH: usize = 64 * 1024;
/// Max frames dispatched per `run_once` burst before yielding to timers.
const BURST_FRAMES: usize = 128;

/// The per-core shared scratch a group borrows while it is stepped (H6).
pub(crate) struct CoreScratch {
    /// Recv buffer for the driver's own demux and for node-internal recv
    /// (election/quorum waits). Sized to the wire contract.
    inbound_buf: Box<[u8]>,
    /// Lent as the active node's `scratch_payload`.
    payload_buf: Vec<u8>,
    /// Lent as the active node's `scratch_outbound`.
    outbound_buf: Vec<u8>,
    /// Lent as the active node's `scratch_quorum_buf`.
    quorum_buf: Vec<u8>,
    /// `RaftStorage::entry_at` scratch for the driver's apply loop.
    apply_buf: Vec<u8>,
}

impl CoreScratch {
    fn new() -> Self {
        Self {
            inbound_buf: vec![0u8; crate::protocol::codec::wire::MAX_FRAME_SIZE].into_boxed_slice(),
            payload_buf: vec![0u8; PAYLOAD_SCRATCH],
            outbound_buf: vec![0u8; OUTBOUND_SCRATCH],
            quorum_buf: vec![0u8; QUORUM_SCRATCH],
            apply_buf: vec![0u8; PAYLOAD_SCRATCH],
        }
    }

    /// Begin one RAII lend of the three big buffers to `node`: swaps them in
    /// now, and the returned [`ScratchLend`] swaps them back on `Drop` — on
    /// the normal path AND when the enclosing future is cancelled at an
    /// `.await`. Also hands back the driver-side `inbound_buf` / `apply_buf`
    /// (disjoint fields, still usable during the lend). Three pointer-triple
    /// swaps each way, zero copy, zero allocation.
    #[inline]
    fn lend<'a, S, T>(
        &'a mut self,
        node: &'a mut RaftNode<S, T>,
    ) -> (ScratchLend<'a, S, T>, &'a mut [u8], &'a mut Vec<u8>) {
        std::mem::swap(&mut node.scratch_payload, &mut self.payload_buf);
        std::mem::swap(&mut node.scratch_outbound, &mut self.outbound_buf);
        std::mem::swap(&mut node.scratch_quorum_buf, &mut self.quorum_buf);
        (
            ScratchLend {
                node,
                payload: &mut self.payload_buf,
                outbound: &mut self.outbound_buf,
                quorum: &mut self.quorum_buf,
            },
            &mut self.inbound_buf,
            &mut self.apply_buf,
        )
    }

    /// Defense-in-depth: re-provision any big buffer that is undersized so no
    /// group ever runs with a short scratch. With the [`ScratchLend`] guard
    /// returning buffers even on cancellation this is a three-branch no-op in
    /// steady state.
    fn ensure_resident(&mut self) {
        if self.payload_buf.len() < PAYLOAD_SCRATCH {
            self.payload_buf = vec![0u8; PAYLOAD_SCRATCH];
        }
        if self.outbound_buf.capacity() < OUTBOUND_SCRATCH {
            self.outbound_buf = vec![0u8; OUTBOUND_SCRATCH];
        }
        if self.quorum_buf.len() < QUORUM_SCRATCH {
            self.quorum_buf = vec![0u8; QUORUM_SCRATCH];
        }
    }
}

/// RAII lend window (H6): while alive, `node` holds the driver's MB-class
/// buffers; `Drop` swaps them back into the driver's [`CoreScratch`] fields.
/// The guard lives inside the async fn frame, so dropping the future at an
/// `.await` (cancellation) runs this `Drop` too — the buffers can never be
/// stranded on a node. Mirrors the `BufferGuard` pattern used for
/// `scratch_quorum_buf` inside the node itself. Safe owned-`Vec` swaps only.
struct ScratchLend<'a, S, T> {
    node: &'a mut RaftNode<S, T>,
    payload: &'a mut Vec<u8>,
    outbound: &'a mut Vec<u8>,
    quorum: &'a mut Vec<u8>,
}

impl<S, T> Drop for ScratchLend<'_, S, T> {
    #[inline]
    fn drop(&mut self) {
        std::mem::swap(&mut self.node.scratch_payload, self.payload);
        std::mem::swap(&mut self.node.scratch_outbound, self.outbound);
        std::mem::swap(&mut self.node.scratch_quorum_buf, self.quorum);
    }
}

/// Splitmix64 finalizer for election jitter (same mixer as `ArbitroRaft`).
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

struct GroupTimers {
    next_election_at: Instant,
    next_heartbeat_at: Instant,
    /// §4.2.3 TimeoutNow received — next election tick skips pre-vote.
    forced_campaign: bool,
    rng: u64,
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
}

impl GroupTimers {
    fn new(gid: GroupId, node_id: u64, timing: crate::TimingConfig) -> Self {
        let mut t = Self {
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            forced_campaign: false,
            rng: mix64(gid.0 ^ node_id.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
            heartbeat_ms: timing.heartbeat_ms.max(1),
            election_min_ms: timing.election_min_ms.max(1),
            election_max_ms: timing.election_max_ms.max(timing.election_min_ms.max(1)),
        };
        t.reset_election();
        t.reset_heartbeat();
        t
    }

    fn reset_heartbeat(&mut self) {
        self.next_heartbeat_at = Instant::now() + Duration::from_millis(self.heartbeat_ms);
    }

    fn reset_election(&mut self) {
        let span = self.election_max_ms - self.election_min_ms + 1;
        self.rng = mix64(self.rng);
        let jitter = self.rng % span;
        self.next_election_at =
            Instant::now() + Duration::from_millis(self.election_min_ms + jitter);
    }
}

/// Per-core driver for N share-nothing Raft groups over one transport.
///
/// See the module docs for the H6/H7/H9 contracts. All groups' outbound
/// frames are stamped with their `group_id` by a demux-backed
/// [`MultiplexedTransport`]; inbound frames are routed by the driver (or,
/// while a group internally awaits its own quorum traffic, parked per-group
/// by the cooperative demux and drained here on the next tick).
pub struct MultiRaftDriver<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    registry: super::RaftGroupRegistry<S, MultiplexedTransport<T>, SM>,
    demux: MultiplexDemux<T>,
    timers: GroupIdMap<GroupTimers>,
    scratch: CoreScratch,
    /// H9: frames the DRIVER routed to a group id it does not host.
    unknown_group_frames: u64,
    /// Reused per-tick list of groups whose timer fired.
    due_scratch: Vec<GroupId>,
}

impl<S, T, SM> MultiRaftDriver<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            registry: super::RaftGroupRegistry::new(),
            demux: MultiplexDemux::new(inner),
            timers: GroupIdMap::default(),
            scratch: CoreScratch::new(),
            unknown_group_frames: 0,
            due_scratch: Vec::new(),
        }
    }

    /// Create a group on this driver: builds the node over a demux-backed
    /// group transport and puts it on the shared-scratch memory diet (its
    /// MB-class buffers are freed; it borrows the driver's while stepped).
    pub fn add_group(
        &mut self,
        gid: GroupId,
        config: NodeConfig,
        storage: S,
        sm: SM,
    ) -> Result<(), RaftError> {
        if self.registry.get(gid).is_some() {
            return Err(RaftError::Protocol(format!("duplicate group id {}", gid.0)));
        }
        let timing = config.timing;
        let node_id = config.node_id.0;
        let transport = self.demux.transport(gid);
        let mut node = match RaftNode::new(config, storage, transport) {
            Ok(node) => node,
            Err(e) => {
                self.demux.unregister(gid);
                return Err(e);
            }
        };
        // H6: shed the per-group big buffers — the driver's CoreScratch is
        // lent for the duration of every operation instead.
        node.scratch_payload = Vec::new();
        node.scratch_outbound = Vec::new();
        node.scratch_quorum_buf = Vec::new();
        // Atomic add: if the registry rejects the node, roll back the demux
        // inbox registered above so no orphan inbox parks frames for a group
        // that was never added.
        if let Err(e) = self.registry.insert(gid, node, sm) {
            self.demux.unregister(gid);
            return Err(e);
        }
        self.timers
            .insert(gid, GroupTimers::new(gid, node_id, timing));
        Ok(())
    }

    /// Remove a group, restoring its standalone scratch buffers so the
    /// returned node functions outside the driver.
    pub fn remove_group(
        &mut self,
        gid: GroupId,
    ) -> Option<(RaftNode<S, MultiplexedTransport<T>>, SM)> {
        let (mut node, sm) = self.registry.remove(gid)?;
        node.scratch_payload = vec![0u8; PAYLOAD_SCRATCH];
        node.scratch_outbound = vec![0u8; OUTBOUND_SCRATCH];
        node.scratch_quorum_buf = vec![0u8; QUORUM_SCRATCH];
        self.demux.unregister(gid);
        self.timers.remove(&gid);
        Some((node, sm))
    }

    #[inline]
    pub fn group(&self, gid: GroupId) -> Option<&RaftNode<S, MultiplexedTransport<T>>> {
        self.registry.get(gid)
    }

    #[inline]
    pub fn state_machine(&self, gid: GroupId) -> Option<&SM> {
        self.registry.state_machine(gid)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    /// H9: inbound frames whose `group_id` named no group hosted here
    /// (driver-routed + demux-parked), counted and dropped pre-decode.
    pub fn unknown_group_frames(&self) -> u64 {
        self.unknown_group_frames + self.demux.unknown_group_frames()
    }

    /// H6 observability: bytes of MB-class scratch a group holds while IDLE.
    /// Zero for every group on the diet (the whole point).
    pub fn group_idle_scratch_bytes(&self, gid: GroupId) -> Option<usize> {
        self.registry.get(gid).map(|node| {
            node.scratch_payload.capacity()
                + node.scratch_outbound.capacity()
                + node.scratch_quorum_buf.capacity()
        })
    }

    /// Run one real election for `gid` (operator-driven; skips pre-vote).
    pub async fn campaign(&mut self, gid: GroupId) -> Result<bool, RaftError> {
        self.scratch.ensure_resident();
        let Some(entry) = self.registry.entry_mut(gid) else {
            return Err(RaftError::Protocol(format!("no group {} in driver", gid.0)));
        };
        let (guard, inbound_buf, _) = self.scratch.lend(&mut entry.node);
        let res = guard.node.campaign_once(inbound_buf).await;
        let hb = match &res {
            Ok(true) => guard.node.send_heartbeat_once().await,
            _ => Ok(()),
        };
        drop(guard);
        if let Some(t) = self.timers.get_mut(&gid) {
            t.reset_election();
            t.reset_heartbeat();
        }
        let elected = res?;
        hb?;
        Ok(elected)
    }

    /// Propose one payload on `gid` (leader-only), wait for commit, and apply
    /// committed entries to the group's state machine before returning.
    pub async fn propose(&mut self, gid: GroupId, payload: &[u8]) -> Result<LogIndex, RaftError> {
        if payload.len() > crate::protocol::codec::wire::MAX_ENTRY_PAYLOAD {
            return Err(RaftError::InvalidPayload(
                "payload exceeds MAX_ENTRY_PAYLOAD and would not fit a peer's frame buffer",
            ));
        }
        if payload.first().copied() == Some(crate::api::node::membership::CONFIG_CHANGE_MAGIC) {
            return Err(RaftError::InvalidPayload(
                "payload first byte collides with reserved config-change magic (0xC0)",
            ));
        }
        self.scratch.ensure_resident();
        let Some(entry) = self.registry.entry_mut(gid) else {
            return Err(RaftError::Protocol(format!("no group {} in driver", gid.0)));
        };
        let (guard, _, apply_buf) = self.scratch.lend(&mut entry.node);
        let res = guard.node.propose_once(payload).await;
        let apply = if res.is_ok() {
            Self::apply_group(&mut *guard.node, &mut entry.sm, apply_buf)
        } else {
            Ok(())
        };
        drop(guard);
        let idx = res?;
        apply?;
        Ok(idx)
    }

    /// One driver tick: drain parked frames, burst-drain the transport, wait
    /// up to `max_wait` when idle, then service due heartbeat/election
    /// timers. Returns the number of frames dispatched.
    pub async fn run_once(&mut self, max_wait: Duration) -> Result<usize, RaftError> {
        self.scratch.ensure_resident();
        let mut processed = 0usize;

        // 1. Frames parked by the cooperative demux while some group awaited
        //    its own quorum traffic. Lock-free no-op when nothing is parked.
        while processed < BURST_FRAMES {
            match self.demux.pop_parked_into(&mut self.scratch.inbound_buf)? {
                Some((gid, n)) => {
                    self.dispatch_frame(GroupId(gid), n).await?;
                    processed += 1;
                }
                None => break,
            }
        }

        // 2. Burst-drain the shared transport — THE hot path: recv into the
        //    resident buffer, read group_id from the fixed header offset,
        //    one O(1) map lookup, step. No allocation, no lock.
        while processed < BURST_FRAMES {
            match self
                .demux
                .inner()
                .recv_frame_timeout(Duration::ZERO, &mut self.scratch.inbound_buf)
                .await
            {
                Ok(Some(n)) => {
                    self.route_raw(n).await?;
                    processed += 1;
                }
                Ok(None) => break,
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "driver: tolerating non-fatal transport recv error");
                    break;
                }
            }
        }

        // 3. Idle — wait for one frame, bounded by max_wait and the earliest
        //    timer deadline.
        if processed == 0 && !max_wait.is_zero() {
            let now = Instant::now();
            let mut wait = max_wait;
            for t in self.timers.values() {
                wait = wait
                    .min(t.next_heartbeat_at.saturating_duration_since(now))
                    .min(t.next_election_at.saturating_duration_since(now));
            }
            if !wait.is_zero() {
                match self
                    .demux
                    .inner()
                    .recv_frame_timeout(wait, &mut self.scratch.inbound_buf)
                    .await
                {
                    Ok(Some(n)) => {
                        self.route_raw(n).await?;
                        processed += 1;
                    }
                    Ok(None) => {}
                    Err(e) if e.is_fatal() => return Err(e),
                    Err(e) => {
                        tracing::warn!(error = %e, "driver: tolerating non-fatal transport recv error");
                    }
                }
            }
        }

        // 4. Timers.
        self.tick_timers().await?;
        Ok(processed)
    }

    /// Route a raw frame already sitting in `scratch.inbound_buf[..n]`.
    #[inline]
    async fn route_raw(&mut self, n: usize) -> Result<(), RaftError> {
        match frame_group(&self.scratch.inbound_buf[..n]) {
            Some(gid) => self.dispatch_frame(GroupId(gid), n).await,
            None => {
                self.unknown_group_frames += 1;
                Ok(())
            }
        }
    }

    /// Route→step→apply for one frame in `scratch.inbound_buf[..n]`.
    async fn dispatch_frame(&mut self, gid: GroupId, n: usize) -> Result<(), RaftError> {
        // H9: a group_id this driver does not host is counted and dropped
        // before decode — never mis-routed, never a panic.
        let Some(entry) = self.registry.entry_mut(gid) else {
            self.unknown_group_frames += 1;
            return Ok(());
        };
        let (guard, inbound_buf, apply_buf) = self.scratch.lend(&mut entry.node);

        let step: Result<(), RaftError> = match crate::decode_message(&inbound_buf[..n]) {
            Ok(inbound) => match guard.node.handle_inbound(inbound).await {
                Ok(()) => Ok(()),
                Err(e) if e.is_fatal() => Err(e),
                Err(e) => {
                    tracing::warn!(
                        group_id = gid.0,
                        error = %e,
                        "driver: dropping inbound frame after non-fatal handler error"
                    );
                    guard.node.metrics.inc_frames_dropped_nonfatal();
                    Ok(())
                }
            },
            Err(e) => {
                tracing::warn!(
                    group_id = gid.0,
                    len = n,
                    error = %e,
                    "driver: dropping undecodable inbound frame"
                );
                guard.node.metrics.inc_frames_dropped_nonfatal();
                Ok(())
            }
        };

        let post: Result<(), RaftError> = if step.is_ok() {
            let adv = if guard.node.is_leader() {
                guard.node.try_advance_commit_index()
            } else {
                Ok(())
            };
            match adv {
                Ok(()) => Self::apply_group(&mut *guard.node, &mut entry.sm, apply_buf),
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        };

        let forced = guard.node.take_forced_campaign();
        drop(guard);

        if let Some(t) = self.timers.get_mut(&gid) {
            // A frame from the leader is a liveness signal — push the
            // election deadline out, exactly like the single-group loop.
            t.reset_election();
            if forced {
                // §4.2.3 TimeoutNow: campaign immediately on the next timer
                // pass (which runs within this same run_once), skipping
                // pre-vote — the transfer is leader-sanctioned.
                t.next_election_at = Instant::now();
                t.forced_campaign = true;
            }
        }
        step?;
        post
    }

    /// Service due heartbeat (leader) and election (follower) timers.
    async fn tick_timers(&mut self) -> Result<(), RaftError> {
        let now = Instant::now();
        self.due_scratch.clear();
        for (gid, t) in self.timers.iter() {
            if now >= t.next_heartbeat_at || now >= t.next_election_at {
                self.due_scratch.push(*gid);
            }
        }
        for i in 0..self.due_scratch.len() {
            let gid = self.due_scratch[i];
            self.tick_group(gid, now).await?;
        }
        Ok(())
    }

    async fn tick_group(&mut self, gid: GroupId, now: Instant) -> Result<(), RaftError> {
        let Some(entry) = self.registry.entry_mut(gid) else {
            return Ok(());
        };
        let Some(t) = self.timers.get_mut(&gid) else {
            return Ok(());
        };
        let is_leader = entry.node.is_leader();

        if now >= t.next_heartbeat_at {
            t.reset_heartbeat();
            if is_leader {
                if !entry.node.check_quorum_active() {
                    tracing::info!(
                        group_id = gid.0,
                        node_id = entry.node.node_id().0,
                        "driver: lost quorum contact, abdicating leadership"
                    );
                    let term = entry.node.current_term();
                    entry.node.step_down(term)?;
                } else {
                    let (guard, _, _) = self.scratch.lend(&mut entry.node);
                    let res = guard.node.send_heartbeat_once().await;
                    drop(guard);
                    match res {
                        Ok(()) => {}
                        Err(e) if e.is_fatal() => return Err(e),
                        Err(e) => {
                            tracing::warn!(group_id = gid.0, error = %e, "driver: heartbeat failed (non-fatal)");
                        }
                    }
                }
            }
        }

        if !is_leader && now >= t.next_election_at {
            let forced = std::mem::take(&mut t.forced_campaign);
            t.reset_election();
            let (guard, inbound_buf, _) = self.scratch.lend(&mut entry.node);
            let res = Self::run_election(&mut *guard.node, inbound_buf, forced).await;
            let hb = match &res {
                Ok(true) => guard.node.send_heartbeat_once().await,
                _ => Ok(()),
            };
            drop(guard);
            if let Some(t) = self.timers.get_mut(&gid) {
                t.reset_election();
                if matches!(res, Ok(true)) {
                    t.reset_heartbeat();
                }
            }
            match res {
                Ok(_) => {}
                Err(e) if e.is_fatal() => return Err(e),
                Err(RaftError::NoQuorum) => {}
                Err(e) => {
                    tracing::warn!(group_id = gid.0, error = %e, "driver: tolerating non-fatal election error");
                }
            }
            match hb {
                Ok(()) => {}
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(group_id = gid.0, error = %e, "driver: post-election heartbeat failed (non-fatal)");
                }
            }
        }
        Ok(())
    }

    /// Pre-vote gated election (forced §4.2.3 campaigns skip pre-vote).
    async fn run_election(
        node: &mut RaftNode<S, MultiplexedTransport<T>>,
        inbound_buf: &mut [u8],
        forced: bool,
    ) -> Result<bool, RaftError> {
        if !forced {
            let pre = match node.campaign_pre_vote(inbound_buf).await {
                Ok(ok) => ok,
                Err(RaftError::NoQuorum) => false,
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "driver: tolerating non-fatal pre-vote error");
                    false
                }
            };
            if !pre {
                return Ok(false);
            }
        }
        node.campaign_once(inbound_buf).await
    }

    /// Apply all committed-but-unapplied entries of one group to its state
    /// machine — the driver-side mirror of the single-group apply loop
    /// (snapshot re-anchor, membership control entries, compaction debt).
    fn apply_group(
        node: &mut RaftNode<S, MultiplexedTransport<T>>,
        sm: &mut SM,
        apply_buf: &mut [u8],
    ) -> Result<(), RaftError> {
        node.restore_state_machine_from_snapshot(sm)?;
        let commit = node.commit_index();
        let mut next = LogIndex(node.last_applied().0 + 1);
        while next <= commit {
            let payload_len = {
                match node.read_entry_payload_into(next, apply_buf)? {
                    Some(entry) => entry.payload.0.len(),
                    // Snapshot advanced the log start past this index; the
                    // restore above (or the next one) re-anchors last_applied.
                    None => break,
                }
            };
            let payload = &apply_buf[..payload_len];
            let consumed = crate::api::node::membership::apply_if_config_change(node, payload)?;
            if !consumed {
                if let Err(e) = sm.apply_at(next, payload) {
                    tracing::error!(
                        node_id = node.node_id().0,
                        index = next.0,
                        error = %e,
                        "driver: state machine apply failed; stopping group"
                    );
                    return Err(e);
                }
            }
            node.set_last_applied(next);
            node.compaction_debt_entries += 1;
            node.compaction_debt_bytes += payload_len as u64;
            next = LogIndex(next.0 + 1);
        }
        crate::api::node::log_compaction::maybe_compact(node, sm)?;
        Ok(())
    }
}
