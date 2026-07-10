use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};

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
    ///   - Errors from `sm.apply` propagate — a diverging state
    ///     machine is a hard bug; the node should crash rather than
    ///     silently continue.
    pub(super) fn apply_committed_entries(&mut self) -> Result<(), RaftError> {
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
                let entry_opt =
                    self.node.read_entry_payload_into(next, &mut self.apply_buf)?;
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
            // slice, then hand them to the state machine. The buffer
            // is not touched again until the next iteration.
            let payload = &self.apply_buf[..payload_len];
            self.state_machine.apply(payload)?;
            self.node.set_last_applied(next);
            next = LogIndex(next.0 + 1);
        }
        Ok(())
    }

    pub(super) async fn run_leader_once(&mut self) -> Result<(), RaftError> {
        // 1. Drain inbound client proposals → pending_batch + pending_slots.
        //    NOTE: do NOT clear first — items may have been pushed by the idle-path select
        //    arm on the previous tick; clearing would drop slots without notifying clients.
        let limit = self.node.config.limits.append_batch_entries;
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

        // 2. Burst-drain available inbound frames.
        let mut processed = 0;
        while let Some(n) = self
            .node
            .transport()
            .recv_frame_timeout(Duration::ZERO, &mut self.inbound_buf)
            .await?
        {
            let inbound = crate::decode_message(&self.inbound_buf[..n])?;
            self.node.handle_inbound(inbound).await?;
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
        futures::select! {
            frame_result = self.node.transport().recv_frame_timeout(timeout, &mut self.inbound_buf).fuse() => {
                if let Some(n) = frame_result? {
                    let inbound = crate::decode_message(&self.inbound_buf[..n])?;
                    self.node.handle_inbound(inbound).await?;
                    self.node.try_advance_commit_index()?;
                    self.drain_commit_waiters();
                    self.apply_committed_entries()?;
                    if !self.node.is_leader() { self.fail_commit_waiters(); }
                }
                // else: timeout, heartbeat sent on next tick
                return Ok(());
            }
            proposal = self.client_rx.next() => {
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
    /// Uses a temporary `Vec<&[u8]>` built from the pre-allocated `pending_batch`
    /// entries. This allocation happens once per batch, NOT per entry.
    pub(super) async fn replicate_pending(&mut self) -> Result<(), RaftError> {
        // Form a slice of slices — one indirect per already-owned Vec<u8> in pending_batch.
        self.node.scratch_payload_refs.clear();
        for p in &self.pending_batch {
            // Safety: We temporarily transmute the lifetime of the slice to &'static [u8].
            // This is safe because we clear the scratchpad before returning, and replicate_batch_async
            // only accesses it during its synchronous asynchronous block execution.
            let slice_static = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(p.as_slice()) };
            self.node.scratch_payload_refs.push(slice_static);
        }

        // SAFETY: We temporarily erase the lifetime link between self.node and the slice passed
        // as argument to replicate_batch_async, allowing &mut self.node to be called concurrently.
        // This is safe because replicate_batch_async only reads the references synchronously
        // during its execution, and we clear the scratchpad immediately afterwards.
        let refs: &'static [&'static [u8]] = unsafe {
            std::mem::transmute::<&[&[u8]], &'static [&'static [u8]]>(
                self.node.scratch_payload_refs.as_slice(),
            )
        };
        let res = self.node.replicate_batch_async(refs).await;
        self.node.scratch_payload_refs.clear();

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

    pub(super) async fn run_follower_once(&mut self) -> Result<(), RaftError> {
        let mut processed = 0;
        while let Some(n) = self
            .node
            .transport()
            .recv_frame_timeout(Duration::ZERO, &mut self.inbound_buf)
            .await?
        {
            let inbound = crate::decode_message(&self.inbound_buf[..n])?;
            self.node.handle_inbound(inbound).await?;
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
            return Ok(());
        }

        let now = Instant::now();
        let timeout = self.next_election_at.saturating_duration_since(now);
        match self
            .node
            .transport()
            .recv_frame_timeout(timeout, &mut self.inbound_buf)
            .await?
        {
            Some(n) => {
                let inbound = crate::decode_message(&self.inbound_buf[..n])?;
                self.node.handle_inbound(inbound).await?;
                self.apply_committed_entries()?;
                self.reset_election_deadline();
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
                        Err(err) => return Err(err),
                    };

                if pre_vote_success {
                    // Only start a real election if the pre-vote check succeeded
                    match self.node.campaign_once(&mut self.inbound_buf).await {
                        Ok(elected) => {
                            self.reset_election_deadline();
                            if elected {
                                self.reset_heartbeat_deadline();
                                self.node.send_heartbeat_once().await?;
                                self.reset_heartbeat_deadline();
                            }
                        }
                        Err(RaftError::NoQuorum) => {
                            self.reset_election_deadline();
                        }
                        Err(err) => return Err(err),
                    }
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
}
