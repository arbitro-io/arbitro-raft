use super::super::RaftNode;
use crate::{LogIndex, PeerId, RaftError, RaftMessage};

/// Outcome of a C3 InstallSnapshot escalation attempt for one peer.
enum SnapshotEscalation {
    /// A snapshot was streamed and accepted — the peer's progress is
    /// re-anchored past the boundary; A9's AppendEntries repair takes over
    /// the remaining tail on the next tick.
    Streamed,
    /// The install gate refused without streaming anything: the peer is in
    /// its C4 attempt-cap cooldown, no snapshot exists on disk, or the peer
    /// is above the snapshot boundary. Cheap — safe to re-check every tick.
    Refused,
    /// A streaming attempt ran and failed (peer unreachable, NACK loop,
    /// step-down mid-install). One PS7 attempt was consumed; the C4 cap +
    /// cooldown bound how often this can repeat per peer.
    Failed,
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Build the heartbeat `AppendEntries` wire for a specific peer, without
    /// sending. Returns `Ok(None)` if this node is not the leader for its
    /// group (defensive: batched callers already filter, but we double-check).
    ///
    /// Shared between the single-group [`send_heartbeat_once`] path and the
    /// multi-group batched fan-out in
    /// [`super::heartbeat_batch::send_batched_heartbeats`].
    pub(crate) fn build_heartbeat_wire(
        &mut self,
        peer: PeerId,
    ) -> Result<Option<crate::protocol::AppendEntries>, RaftError> {
        if !self.is_leader() {
            return Ok(None);
        }
        self.ensure_leader_progress_initialized()?;
        let (prev_idx, prev_term, next_index) = self.append_prev_for_peer(peer)?;
        // Parity with the pre-restructure `build_append_for_peer`: the wire
        // build also performed the backlog read (its result is unused for a
        // bare heartbeat) and propagated its error, so a peer whose backlog
        // is unreadable is skipped rather than sent a bare wire. The entries
        // land in a dock-recycled local vec instead of a laundered `self`
        // scratchpad (US3).
        let mut entries = self.scratch_entries.take();
        let read = self.storage.read_entries(
            next_index,
            LogIndex(u64::MAX),
            &mut entries,
            &mut self.scratch_payload,
        );
        self.scratch_entries.put(entries);
        read?;
        Ok(Some(crate::protocol::AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_idx.0.into(),
            prev_log_term: prev_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: 0.into(),
            // A11: current ReadIndex probe token; followers echo it in
            // their AppendEntriesResp so a confirmation round can tell
            // fresh acks from stale buffered ones.
            _pad: self.read_probe_seq.into(),
        }))
    }

    /// Best-effort bare heartbeats to every voter except `skip` (and self).
    ///
    /// OPS-2 support: called from inside a long-running snapshot install so
    /// the OTHER followers keep hearing from the leader and do not start a
    /// spurious election while the install monopolizes the call stack. Bare
    /// wires only — no backlog replication, and per-peer failures are
    /// swallowed (a heartbeat gap is recoverable; aborting the install for
    /// it is not worth it).
    pub(crate) async fn send_bare_heartbeats_except(&mut self, skip: PeerId) {
        if !self.is_leader() {
            return;
        }
        // A13: learners hear heartbeats too — they follow the leader's commit
        // index and must not be starved of contact during a long install.
        self.scratch_peers.clear();
        for peer in self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .filter(|p| *p != self.config.node_id && *p != skip)
        {
            self.scratch_peers.push(peer);
        }
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            let req = match self.build_heartbeat_wire(peer) {
                Ok(Some(req)) => req,
                // Not leader anymore, or this peer's prev-entry metadata is
                // unavailable (e.g. compacted) — skip, never abort.
                Ok(None) | Err(_) => continue,
            };
            let msg = RaftMessage::AppendEntriesVectored(&req, &[]);
            let _ = self.send_message(peer, &msg).await;
        }
    }

    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(self.not_leader_error());
        }

        self.ensure_leader_progress_initialized()?;

        // A13: learners are heartbeat/repair targets exactly like followers —
        // the A9 backlog-repair branch below is what catches a fresh learner
        // up from `match_index 0`, and the C3 snapshot escalation covers a
        // learner whose backlog was already compacted away.
        self.scratch_peers.clear();
        for peer in self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.scratch_peers.push(peer);
        }

        let last_log = self.cached_last_log.0;
        let mut sent = 0usize;
        // C3: at most ONE InstallSnapshot escalation per heartbeat tick. An
        // install is chunked and interleaves bare heartbeats to the other
        // followers (OPS-2), but it still monopolizes this call stack — a
        // second below-horizon peer simply waits for the next tick.
        let mut escalated_this_tick = false;
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];

            // If the peer is behind, replicate its backlog instead of a bare
            // heartbeat. Plain heartbeats carry no entries, so without this a
            // lagging follower — e.g. a freshly added voter, or one recovering
            // after a partition — would never catch up between client proposals
            // (A9 / PS11). `send_append_attempt` ships entries from the peer's
            // next_index (bounded by `limits.append_batch_entries` per tick);
            // the ack advances its progress on the next tick. A DIVERGED
            // follower is covered by the same hook: its reject of the bare
            // probe walks `next_index` back (`handle_append_entries_response`)
            // until this branch takes over and ships the correcting entries —
            // all without any new client traffic.
            let next_index = self.peer_progress.get(&peer).map(|p| p.next_index);
            if let Some(next_index) = next_index.filter(|n| *n <= last_log) {
                // Errors are contained to THIS peer (same policy as
                // `send_bare_heartbeats_except`): propagating would abort
                // heartbeats for every remaining follower — starving healthy
                // peers of leader contact and, worse, killing the run loop
                // (a repair-read `CorruptLog` classifies as Fatal) because
                // ONE peer's backlog is unreadable. A genuinely corrupt local
                // log still halts the node via its own critical paths
                // (append/commit/apply); a peer-repair read is not one.
                //
                // C3 escalation, tier 1 — storage truth: when the entry this
                // peer needs next no longer exists in the log (its backlog was
                // compacted away below the log start by C2 / a snapshot's
                // `truncate_before`), AppendEntries can never repair it — only
                // an InstallSnapshot can. Storage is deliberately the source
                // of truth here, NOT the log_metadata arena: after an
                // in-process compaction the arena can still answer `term_at`
                // for truncated indexes, which would let a doomed append build
                // "succeed" with a batch the follower must reject as
                // non-contiguous — stranding the peer silently.
                let backlog_compacted = matches!(
                    self.storage.entry_at(next_index, &mut self.scratch_payload),
                    Ok(None)
                );
                if backlog_compacted {
                    if !escalated_this_tick {
                        match self.escalate_snapshot_catchup(peer).await {
                            SnapshotEscalation::Streamed => {
                                escalated_this_tick = true;
                                sent += 1;
                            }
                            SnapshotEscalation::Failed => escalated_this_tick = true,
                            SnapshotEscalation::Refused => {}
                        }
                    }
                    // else: defer this peer's escalation to the next tick —
                    // one install per tick keeps the heartbeat path bounded.
                    continue;
                }
                match self.send_append_attempt(peer, 0).await {
                    Ok(Some(_)) => sent += 1,
                    Ok(None) => {}
                    Err(e) => {
                        // C3 escalation, tier 2 — repair-read failure: e.g.
                        // after a restart the arena is cold and the prev-entry
                        // term read (`term_at`) fails with CorruptLog because
                        // the prefix was compacted in a previous life. The
                        // install gate re-checks the snapshot boundary, so
                        // this can never send a spurious snapshot to a peer
                        // that AppendEntries could still repair.
                        tracing::warn!(
                            node_id = self.config.node_id.0,
                            peer = peer.0,
                            error = %e,
                            "heartbeat-driven repair failed for behind peer; checking snapshot escalation"
                        );
                        if !escalated_this_tick {
                            match self.escalate_snapshot_catchup(peer).await {
                                SnapshotEscalation::Streamed => {
                                    escalated_this_tick = true;
                                    sent += 1;
                                }
                                SnapshotEscalation::Failed => escalated_this_tick = true,
                                SnapshotEscalation::Refused => {}
                            }
                        }
                    }
                }
                continue;
            }

            // Caught-up peer (next_index > last_log): bare wire only — the
            // idle path adds zero steady-state entry traffic.
            let req = match self.build_heartbeat_wire(peer) {
                Ok(Some(req)) => req,
                // Not leader anymore — stop; nothing more to send this term.
                Ok(None) => continue,
                // Per-peer wire-build failure (e.g. prev-entry metadata
                // unavailable) must not abort heartbeats to the remaining
                // followers — same containment as the repair branch above.
                Err(e) => {
                    tracing::warn!(
                        node_id = self.config.node_id.0,
                        peer = peer.0,
                        error = %e,
                        "failed to build heartbeat wire; skipping peer this tick"
                    );
                    continue;
                }
            };
            let msg = RaftMessage::AppendEntriesVectored(&req, &[]);
            if self.send_message(peer, &msg).await {
                sent += 1;
            }
        }
        tracing::trace!(
            node_id = self.config.node_id.0,
            term = self.hard_state.current_term.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }

    /// C3: drive one InstallSnapshot catch-up attempt for a peer whose
    /// backlog fell below the compaction horizon, reusing the existing
    /// PS7-capped install path ([`maybe_install_snapshot_to_lagging_peer`]).
    ///
    /// Every error is contained to THIS peer — same policy as the
    /// AppendEntries repair branch in [`send_heartbeat_once`]: one peer's
    /// failed install must never abort heartbeats to the remaining followers
    /// or kill the leader's run loop. The C4 attempt cap + cooldown inside
    /// the install gate bound how often a persistently-failing peer can burn
    /// leader cycles.
    ///
    /// [`maybe_install_snapshot_to_lagging_peer`]: RaftNode::maybe_install_snapshot_to_lagging_peer
    async fn escalate_snapshot_catchup(&mut self, peer: PeerId) -> SnapshotEscalation {
        match self.maybe_install_snapshot_to_lagging_peer(peer).await {
            Ok(true) => {
                tracing::info!(
                    node_id = self.config.node_id.0,
                    peer = peer.0,
                    "below-horizon peer caught up via automatic InstallSnapshot"
                );
                SnapshotEscalation::Streamed
            }
            Ok(false) => {
                tracing::debug!(
                    node_id = self.config.node_id.0,
                    peer = peer.0,
                    "snapshot escalation not applicable this tick \
                     (cooldown, no snapshot on disk, or peer above boundary)"
                );
                SnapshotEscalation::Refused
            }
            Err(e) => {
                tracing::warn!(
                    node_id = self.config.node_id.0,
                    peer = peer.0,
                    error = %e,
                    "snapshot escalation for below-horizon peer failed; \
                     retrying under the C4 attempt cap"
                );
                SnapshotEscalation::Failed
            }
        }
    }
}
