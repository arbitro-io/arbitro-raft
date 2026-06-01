use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};

use crate::{LogIndex, RaftError, RaftStorage, RaftTransport};

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

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
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
            if !self.node.is_leader() {
                self.fail_commit_waiters();
            }
            return Ok(());
        }

        // 3. Idle — wait until next heartbeat or next inbound frame.
        let now = Instant::now();
        if now >= self.next_heartbeat_at {
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
                self.reset_election_deadline();
                if self.node.is_leader() {
                    self.reset_heartbeat_deadline();
                }
            }
            None => match self.node.campaign_once(&mut self.inbound_buf).await {
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
            },
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
