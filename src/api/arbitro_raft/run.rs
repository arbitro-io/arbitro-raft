use std::time::{Duration, Instant};

use futures::FutureExt;

use crate::{LogIndex, RaftError, RaftStorage, RaftTransport, StateMachine};

use super::client::CommitWaiter;
use super::ArbitroRaft;

// ---------------------------------------------------------------------------
// Private event-loop implementation for ArbitroRaft.
//
// Responsibilities:
//   - run_leader_once  — drain client channel, replicate, burst-drain inbound
//   - run_follower_once — burst-drain inbound, campaign on timeout
//   - replicate_pending — form batch slices, call replicate_batch_async
//   - drain_commit_waiters — notify clients whose entries have been committed
//   - fail_commit_waiters  — notify all pending clients on step-down
// ---------------------------------------------------------------------------

impl<S, T, SM> ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    /// Apply all committed-but-not-yet-applied entries to the state
    /// machine, in strict log order.
    ///
    /// Invariants:
    ///   - `last_applied <= commit_index` at all times (Raft protocol).
    ///   - Apply is per-node local. On leader step-down, in-flight
    ///     committed entries MUST still be applied — they are durable
    ///     in the log. This helper does NOT gate on `is_leader()`.
    ///   - Apply is idempotent-per-index across restarts because
    ///     `last_applied` is volatile (reset to 0) and the state
    ///     machine is responsible for its own persistence and
    ///     reconciliation via `snapshot`/`restore`.
    ///   - Errors from `sm.apply_at` propagate — a diverging state
    ///     machine is a hard bug; the node should crash rather than
    ///     silently continue. (Resource-class errors are the one
    ///     exception: `run_once` degrades instead of dying — C8.)
    pub(super) fn apply_committed_entries(&mut self) -> Result<(), RaftError> {
        // Consume any freshly-installed snapshot before walking log entries.
        // This re-anchors `last_applied` to the snapshot boundary so the
        // loop below doesn't hit a `None` from `read_entry_payload_into`
        // for entries the snapshot has replaced.
        self.node
            .restore_state_machine_from_snapshot(&mut self.state_machine)?;
        let commit = self.node.commit_index();
        let mut next = LogIndex(self.node.last_applied().0 + 1);
        while next <= commit {
            // Split-borrow: `read_entry_payload_into` borrows
            // `&self.node.storage` immutably via `&self.node`; the
            // subsequent `self.state_machine.apply` borrows the
            // disjoint `state_machine` field mutably. We copy the
            // payload bytes out into a local slice so that the
            // read-borrow of `self.node` ends before we touch
            // `self.state_machine`.
            let payload_len = {
                let entry_opt = self
                    .node
                    .read_entry_payload_into(next, &mut self.apply_buf)?;
                match entry_opt {
                    Some(entry) => entry.payload.0.len(),
                    // Entry missing at an index <= commit_index is only
                    // possible if a snapshot install has advanced the
                    // log start beyond `last_applied`. In that case the
                    // state machine will have been restored by the
                    // snapshot handler; stop applying and let the
                    // snapshot path re-anchor `last_applied`.
                    None => break,
                }
            };
            // Reborrow the just-written payload bytes as an immutable
            // slice. Config-change entries (0xC0 magic) are consumed by
            // the membership layer; only application payloads reach the
            // user state machine.
            let payload = &self.apply_buf[..payload_len];
            let consumed_by_membership =
                crate::api::node::membership::apply_if_config_change(&mut self.node, payload)?;
            if !consumed_by_membership {
                // A6: hand the state machine the entry's committed LogIndex so
                // an externally-persistent implementation can keep its own
                // durable applied cursor (the engine's `last_applied` is
                // volatile — see the `StateMachine` trait docs). Defaults to
                // forwarding to `apply(entry)` for index-agnostic impls.
                if let Err(e) = self.state_machine.apply_at(next, payload) {
                    // A diverging state machine is a hard bug — log loudly with
                    // the offending index before the error stops the node
                    // (ERR-10 / P1-3), rather than letting it die unexplained.
                    tracing::error!(
                        node_id = self.node.node_id().0,
                        index = next.0,
                        error = %e,
                        "state machine apply failed; stopping node"
                    );
                    return Err(e);
                }
            }
            self.node.set_last_applied(next);
            // C2: applied-entry debt drives the log-compaction policy trigger
            // below. Config-change entries count too — they occupy log space.
            self.node.compaction_debt_entries += 1;
            self.node.compaction_debt_bytes += payload_len as u64;
            next = LogIndex(next.0 + 1);
        }
        // C2: policy-triggered log compaction, leader AND follower — both
        // accumulate log. Cheap when the trigger has not fired (two integer
        // compares). The horizon is conservative (clamped to every current
        // voter's match_index minus a retention margin) so a lagging follower
        // is never stranded; see `log_compaction::maybe_compact`.
        crate::api::node::log_compaction::maybe_compact(&mut self.node, &self.state_machine)?;
        Ok(())
    }

    /// Decode and handle one inbound frame of `n` bytes from `inbound_buf`.
    ///
    /// Frame-level problems — an undecodable frame, an unknown message kind, an
    /// unregistered dispatch command, or an oversized/rejected snapshot from a
    /// peer — are logged and swallowed: a single bad frame from one peer (or a
    /// version-skewed node) must never terminate the consensus loop. Only a
    /// [`ErrorClass::Fatal`](crate::ErrorClass::Fatal) error (local storage
    /// failure / corrupt log) propagates and stops the node.
    ///
    /// D3 abuse cutoff: decode errors are attributed to the frame's claimed
    /// sender (bounded to current members plus one shared "unknown" bucket).
    /// A sender that crosses `limits.inbound_decode_error_jail_threshold`
    /// decode errors inside one error window is jailed for
    /// `limits.inbound_jail_cooldown_ms`; while jailed, its frames are shed
    /// HERE, pre-decode — one header peek and a counter bump — so a hostile
    /// peer blasting garbage at line rate cannot make this loop burn a full
    /// decode + warn-log per frame.
    pub(super) async fn dispatch_inbound(&mut self, n: usize) -> Result<(), RaftError> {
        // Cheap header peek for abuse attribution — NOT trust: a D2-compliant
        // transport already dropped frames whose `from` mismatches the
        // connection's authenticated identity before they reached us.
        let claimed_from = crate::protocol::codec::decode::parse_prefix::<
            crate::protocol::codec::wire::RaftFrameHeader,
        >(&self.inbound_buf[..n], "raft frame header")
        .ok()
        .map(|(h, _)| h.from.get());
        let abuse_key = super::abuse::InboundAbuseGuard::key_for(claimed_from, self.node.peers());
        let now = Instant::now();
        if self.abuse.is_jailed(abuse_key, now) {
            self.node.metrics.inc_frames_shed_jailed();
            return Ok(());
        }

        let inbound = match crate::decode_message(&self.inbound_buf[..n]) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    len = n,
                    error = %e,
                    "dropping undecodable inbound frame"
                );
                self.node.metrics.inc_frames_dropped_nonfatal();
                if self
                    .abuse
                    .record_decode_error(abuse_key, now, &self.node.config.limits)
                {
                    self.node.metrics.inc_peers_jailed();
                    tracing::warn!(
                        node_id = self.node.node_id().0,
                        peer = abuse_key,
                        cooldown_ms = self.node.config.limits.inbound_jail_cooldown_ms,
                        "jailing peer: decode-error rate exceeded; shedding its frames pre-decode"
                    );
                }
                return Ok(());
            }
        };
        match self.node.handle_inbound(inbound).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_fatal() => Err(e),
            Err(e) => {
                if e.is_resource_exhaustion() {
                    // C8: an ENOSPC-class failure while handling a frame (e.g.
                    // a follower's append persist) — the frame was not acked,
                    // so the leader never counts this node. Survive read-only
                    // and resume acking once storage recovers.
                    self.node.metrics.inc_resource_exhausted();
                    tracing::error!(
                        node_id = self.node.node_id().0,
                        error = %e,
                        "storage resource exhaustion (disk full?) while handling \
                         inbound frame; frame dropped, nothing acked — will \
                         resume once storage recovers"
                    );
                } else {
                    tracing::warn!(
                        node_id = self.node.node_id().0,
                        error = %e,
                        "dropping inbound frame after non-fatal handler error"
                    );
                }
                self.node.metrics.inc_frames_dropped_nonfatal();
                Ok(())
            }
        }
    }

    /// Receive one inbound frame, tolerating non-fatal transport errors.
    ///
    /// A transient recv error — an oversized frame from one peer, a short read,
    /// a momentarily-broken connection — is logged and reported as "no frame
    /// this tick" (`Ok(None)`) so a single misbehaving or version-skewed peer
    /// can never terminate the consensus loop. Only a
    /// [`ErrorClass::Fatal`](crate::ErrorClass::Fatal) error (local storage /
    /// corrupt log) propagates. This is the recv-side complement to
    /// [`dispatch_inbound`](Self::dispatch_inbound) and completes P0-2.
    pub(super) async fn recv_inbound(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<usize>, RaftError> {
        match self
            .node
            .transport()
            .recv_frame_timeout(timeout, &mut self.inbound_buf)
            .await
        {
            Ok(opt) => Ok(opt),
            Err(e) if e.is_fatal() => Err(e),
            Err(e) => {
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    error = %e,
                    "tolerating non-fatal transport recv error"
                );
                self.node.metrics.inc_frames_dropped_nonfatal();
                Ok(None)
            }
        }
    }

    /// Drive an inherited joint configuration to completion (Raft §4.3).
    ///
    /// A node can become leader while a membership change is only half-applied —
    /// it appended `C_old,new` (activating the joint config) and then won an
    /// election before `C_new` committed. Nothing else re-proposes the final
    /// entry, so the transition would stall forever. The pathological case is a
    /// node *being removed* that wins mid-transition: it must finish the removal
    /// (commit `C_new`, then step down when it applies a config it is not part
    /// of) rather than sit as a permanent leader under the stale voter set.
    ///
    /// This is the smaller, protocol-level alternative to leader-transfer
    /// (§4.2.3): whoever holds leadership completes the transition. No-op when
    /// not leader or not in a joint config. Best-effort — if this node steps
    /// down or loses quorum before `C_new` commits, the next leader runs the
    /// same finalize on its own election.
    async fn finalize_joint_if_inherited(&mut self) -> Result<(), RaftError> {
        if !self.node.is_leader() {
            return Ok(());
        }
        let Some((old_peers, new_peers)) = self.node.joint_peers.clone() else {
            return Ok(());
        };
        tracing::info!(
            node_id = self.node.node_id().0,
            "inherited active joint config on election; proposing C_new to finish membership change"
        );
        use crate::api::node::membership::{ConfigChangeEntry, ConfigChangePhase};
        let final_bytes = ConfigChangeEntry {
            phase: ConfigChangePhase::Final,
            old_peers,
            new_peers,
        }
        .encode();
        match self.node.propose_once(&final_bytes).await {
            Ok(_) => {
                // Farewell commit advertisement — `C_new` committed but is
                // not yet applied locally, so the fan-out set still includes
                // the peers being removed; this heartbeat is their only
                // guaranteed chance to learn the final entry committed and
                // self-remove before we drop them from replication. Same
                // rationale as in `propose_config_change`.
                if self.node.is_leader() {
                    self.send_heartbeat_once().await?;
                }
                Ok(())
            }
            // Lost leadership / quorum before it committed — a later leader
            // will retry the same finalize. Not an error.
            Err(RaftError::NotLeader { .. }) | Err(RaftError::NoQuorum) => Ok(()),
            Err(e) if e.is_fatal() => Err(e),
            Err(e) => {
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    error = %e,
                    "joint finalize proposal failed; will retry on next leader"
                );
                Ok(())
            }
        }
    }

    pub(super) async fn run_leader_once(&mut self) -> Result<(), RaftError> {
        // §4.2.3 leadership-transfer freeze: while a TimeoutNow handoff is
        // pending, PARK client proposals — leave them queued in `client_rx`
        // (the same place they wait on a follower) instead of replicating.
        // If the transfer succeeds this node becomes a follower and they stay
        // parked; if it aborts (window expires) the next tick drains them
        // normally. Leader duties (inbound, heartbeats, check-quorum)
        // continue untouched.
        let transferring = self.node.leadership_transfer_in_progress();

        // Timer-based eviction of stalled inbound snapshot transfers (C4).
        // Cheap: the map holds at most one entry per peer and is usually empty.
        self.node.evict_stalled_snapshots();

        // 1. Drain inbound client proposals → pending_batch + pending_slots.
        //    NOTE: do NOT clear first — items may have been pushed by the idle-path select
        //    arm on the previous tick; clearing would drop slots without notifying clients.
        let limit = self.node.config.limits.append_batch_entries;
        if !transferring {
            while self.pending_batch.len() < limit {
                match self.client_rx.try_recv() {
                    Ok(p) => {
                        self.pending_batch.push(p.payload);
                        self.pending_slots.push(p.slot_id);
                    }
                    Err(_) => break,
                }
            }

            if !self.pending_batch.is_empty() {
                self.replicate_pending().await?;
            }
        }

        // 2. Burst-drain available inbound frames.
        let mut processed = 0;
        while let Some(n) = self.recv_inbound(Duration::ZERO).await? {
            self.dispatch_inbound(n).await?;
            processed += 1;
            if processed >= 128 {
                break;
            }
        }

        if processed > 0 {
            self.node.try_advance_commit_index()?;
            self.drain_commit_waiters();
            // Apply committed entries locally — safe even if we just
            // stepped down: entries already committed remain durable
            // and must be applied to keep the state machine in sync.
            self.apply_committed_entries()?;
            if !self.node.is_leader() {
                self.fail_commit_waiters();
            }
            return Ok(());
        }

        // 3. Idle — wait until next heartbeat or next inbound frame.
        let now = Instant::now();
        if now >= self.next_heartbeat_at {
            // Check-Quorum validation: leader must check if it still maintains a majority lease
            if !self.node.check_quorum_active() {
                tracing::info!(
                    node_id = self.node.node_id().0,
                    "lost quorum contact, abdicating leadership"
                );
                let term = self.node.current_term();
                self.node.step_down(term)?;
                self.fail_commit_waiters();
                return Ok(());
            }
            self.node.send_heartbeat_once().await?;
            self.reset_heartbeat_deadline();
            return Ok(());
        }

        let timeout = self.next_heartbeat_at.saturating_duration_since(now);

        // Transfer pending: wait on inbound frames only — do NOT select on
        // `client_rx`, so proposals stay parked for the handoff window.
        if transferring {
            if let Some(n) = self.recv_inbound(timeout).await? {
                self.dispatch_inbound(n).await?;
                self.node.try_advance_commit_index()?;
                self.drain_commit_waiters();
                self.apply_committed_entries()?;
                if !self.node.is_leader() {
                    self.fail_commit_waiters();
                }
            }
            return Ok(());
        }

        futures::select! {
            frame_result = self.node.transport().recv_frame_timeout(timeout, &mut self.inbound_buf).fuse() => {
                let frame = match frame_result {
                    Ok(opt) => opt,
                    Err(e) if e.is_fatal() => return Err(e),
                    Err(e) => {
                        tracing::warn!(
                            node_id = self.node.node_id().0,
                            error = %e,
                            "tolerating non-fatal transport recv error"
                        );
                        None
                    }
                };
                if let Some(n) = frame {
                    self.dispatch_inbound(n).await?;
                    self.node.try_advance_commit_index()?;
                    self.drain_commit_waiters();
                    self.apply_committed_entries()?;
                    if !self.node.is_leader() { self.fail_commit_waiters(); }
                }
                // else: timeout, heartbeat sent on next tick
                return Ok(());
            }
            // tokio's bounded `recv` is cancel-safe: losing this select race
            // never loses a proposal.
            proposal = self.client_rx.recv().fuse() => {
                if let Some(p) = proposal {
                    self.pending_batch.push(p.payload);
                    self.pending_slots.push(p.slot_id);
                    // Drain all remaining proposals in one shot — form the full batch
                    // immediately so we replicate below without waiting for the next tick.
                    while self.pending_batch.len() < limit {
                        match self.client_rx.try_recv() {
                            Ok(p2) => { self.pending_batch.push(p2.payload); self.pending_slots.push(p2.slot_id); }
                            Err(_) => break,
                        }
                    }
                }
                // Fall through — replicate the batch formed above.
            }
        }

        // Replicate batch accumulated by the proposal arm (skipped if frame arm returned).
        if !self.pending_batch.is_empty() {
            self.replicate_pending().await?;
        }

        Ok(())
    }

    /// Replicate `pending_batch`, pair results with `pending_slots` → `commit_waiters`.
    ///
    /// The slice-of-slices view is a LOCAL `Vec<&[u8]>` (allocation recycled
    /// through the node's `scratch_payload_refs` dock) whose elements borrow
    /// `self.pending_batch` — a field disjoint from `self.node`, so the
    /// borrow checker itself proves the refs stay valid across the
    /// `&mut self.node` call to `replicate_batch_async`. No `'static`
    /// laundering, no ref parked inside the node (US3).
    pub(super) async fn replicate_pending(&mut self) -> Result<(), RaftError> {
        // Form a slice of slices — one indirect per already-owned Bytes payload
        // in pending_batch (H5: refcounted, moved from the mailbox uncopied).
        let mut refs = self.node.scratch_payload_refs.take();
        refs.extend(self.pending_batch.iter().map(|p| &p[..]));

        let res = self.node.replicate_batch_async(&refs).await;
        self.node.scratch_payload_refs.put(refs);

        match res {
            Ok((first_index, _)) => {
                for (i, slot_id) in self.pending_slots.drain(..).enumerate() {
                    self.commit_waiters.push(CommitWaiter {
                        index: LogIndex(first_index.0 + i as u64),
                        slot_id,
                    });
                }
            }
            Err(e) => {
                for slot_id in self.pending_slots.drain(..) {
                    self.registry.get(slot_id).notify_error();
                }
                self.pending_batch.clear();
                return Err(e);
            }
        }
        self.pending_batch.clear();
        Ok(())
    }

    /// Run one real election (no pre-vote) and settle post-election duties:
    /// deadline resets, the first heartbeat, and §4.3 joint auto-resumption.
    /// Error handling mirrors the follower-timeout campaign path (P0-2): only
    /// Fatal-class errors propagate.
    async fn campaign_and_settle(&mut self) -> Result<(), RaftError> {
        match self.node.campaign_once(&mut self.inbound_buf).await {
            Ok(elected) => {
                self.reset_election_deadline();
                if elected {
                    self.reset_heartbeat_deadline();
                    self.node.send_heartbeat_once().await?;
                    self.reset_heartbeat_deadline();
                    // If we won while a membership change was still
                    // in its joint phase, drive it to completion so
                    // the transition can never stall (§4.3).
                    self.finalize_joint_if_inherited().await?;
                }
            }
            Err(RaftError::NoQuorum) => {
                self.reset_election_deadline();
            }
            Err(err) if err.is_fatal() => return Err(err),
            // Non-fatal error mid-election: abandon this round and
            // retry after the election timeout (P0-2).
            Err(err) => {
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    error = %err,
                    "tolerating non-fatal error during election campaign"
                );
                self.reset_election_deadline();
            }
        }
        Ok(())
    }

    pub(super) async fn run_follower_once(&mut self) -> Result<(), RaftError> {
        // Timer-based eviction of stalled inbound snapshot transfers (C4):
        // runs every tick so a leader that goes quiet mid-install cannot park
        // a multi-GiB pending buffer until the next unrelated message arrives.
        self.node.evict_stalled_snapshots();

        let mut processed = 0;
        while let Some(n) = self.recv_inbound(Duration::ZERO).await? {
            self.dispatch_inbound(n).await?;
            processed += 1;
            if processed >= 128 {
                break;
            }
        }

        if processed > 0 {
            // Followers learn about commit-index advances through
            // AppendEntries; apply any newly-committed entries before
            // returning so the state machine tracks the leader.
            self.apply_committed_entries()?;
            self.reset_election_deadline();
        }

        // §4.2.3 leadership transfer: a leader-sanctioned TimeoutNow bypasses
        // BOTH the election-timeout wait and pre-vote — campaign immediately.
        if self.node.take_forced_campaign() {
            self.campaign_and_settle().await?;
            return Ok(());
        }

        if processed > 0 {
            return Ok(());
        }

        let now = Instant::now();
        let timeout = self.next_election_at.saturating_duration_since(now);
        match self.recv_inbound(timeout).await? {
            Some(n) => {
                self.dispatch_inbound(n).await?;
                self.apply_committed_entries()?;
                self.reset_election_deadline();
                // Forced campaign (§4.2.3) — see above.
                if self.node.take_forced_campaign() {
                    self.campaign_and_settle().await?;
                    return Ok(());
                }
                if self.node.is_leader() {
                    self.reset_heartbeat_deadline();
                }
            }
            None => {
                // Pre-Vote protocol: first check if the cluster would support our candidacy
                let pre_vote_success =
                    match self.node.campaign_pre_vote(&mut self.inbound_buf).await {
                        Ok(success) => success,
                        Err(RaftError::NoQuorum) => false,
                        Err(err) if err.is_fatal() => return Err(err),
                        // A malformed/hostile frame arriving mid-campaign must not
                        // terminate the node — treat the pre-vote as failed (P0-2).
                        Err(err) => {
                            tracing::warn!(
                                node_id = self.node.node_id().0,
                                error = %err,
                                "tolerating non-fatal error during pre-vote campaign"
                            );
                            false
                        }
                    };

                if pre_vote_success {
                    // Only start a real election if the pre-vote check succeeded
                    self.campaign_and_settle().await?;
                } else {
                    self.reset_election_deadline();
                }
            }
        }

        Ok(())
    }

    /// Resolve all commit_waiters whose index ≤ current commit_index.
    ///
    /// Fast path: when the entire batch commits at once (normal case), drain
    /// in insertion order via a single pass with no swap_remove overhead.
    pub(super) fn drain_commit_waiters(&mut self) {
        let commit_index = self.node.commit_index();
        if self.commit_waiters.is_empty() {
            return;
        }

        // Fast path — full batch committed (common in bench + low-contention).
        if self
            .commit_waiters
            .last()
            .is_some_and(|w| w.index <= commit_index)
        {
            for w in self.commit_waiters.drain(..) {
                self.registry.get(w.slot_id).notify_committed(w.index);
            }
            return;
        }

        // Slow path — partial commit, swap_remove to avoid shifting.
        let mut i = 0;
        while i < self.commit_waiters.len() {
            if self.commit_waiters[i].index <= commit_index {
                let w = self.commit_waiters.swap_remove(i);
                self.registry.get(w.slot_id).notify_committed(w.index);
            } else {
                i += 1;
            }
        }
    }

    /// Fail all pending commit_waiters — called on step-down.
    pub(super) fn fail_commit_waiters(&mut self) {
        for w in self.commit_waiters.drain(..) {
            self.registry.get(w.slot_id).notify_error();
        }
    }

    /// Leader-side helper: for every peer whose `next_index` has fallen
    /// below the on-disk snapshot boundary, stream the snapshot instead
    /// of an `AppendEntries`. No-op on followers.
    ///
    /// Returns the number of peers a snapshot was actually sent to.
    pub async fn install_snapshot_to_lagging_peers(&mut self) -> Result<usize, RaftError> {
        if !self.node.is_leader() {
            return Ok(0);
        }
        // Copy the peer list out so the async call below can take
        // `&mut self.node` without aliasing the borrow of `config.peers`.
        // A13: learners are snapshot catch-up targets like followers.
        let mut peers: Vec<_> = self.node.peers().to_vec();
        peers.extend_from_slice(self.node.learners());
        let self_id = self.node.node_id();
        let mut sent = 0usize;
        for peer in peers {
            if peer == self_id {
                continue;
            }
            if self
                .node
                .maybe_install_snapshot_to_lagging_peer(peer)
                .await?
            {
                sent += 1;
            }
        }
        Ok(sent)
    }
}
