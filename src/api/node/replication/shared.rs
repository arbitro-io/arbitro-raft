use std::time::Duration;

use super::super::RaftNode;
use crate::{LogIndex, PeerId, RaftError, RaftMessage, Term};
use zerocopy::IntoBytes;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Compute the `(prev_log_index, prev_log_term, next_index)` triple for
    /// an append to `peer`. Pure metadata — the actual backlog read happens
    /// at the call site with split field borrows, so the entries can borrow
    /// `scratch_payload` without any lifetime laundering (US3).
    pub(crate) fn append_prev_for_peer(
        &mut self,
        peer: PeerId,
    ) -> Result<(LogIndex, Term, LogIndex), RaftError> {
        let progress = self
            .peer_progress
            .get(&peer)
            .copied()
            .ok_or(RaftError::PeerUnknown(peer))?;
        let prev_log_index = LogIndex(progress.next_index.0.saturating_sub(1));
        let prev_log_term = self.term_at(prev_log_index)?;
        Ok((prev_log_index, prev_log_term, progress.next_index))
    }

    pub(crate) async fn send_append_attempt(
        &mut self,
        peer: PeerId,
        _attempt: u64,
    ) -> Result<Option<LogIndex>, RaftError> {
        // All `&mut self` metadata work happens BEFORE any storage view is
        // taken, so the borrows below never cross a `&mut self` reborrow.
        let (prev_idx, prev_term, next_index) = self.append_prev_for_peer(peer)?;

        // ── 1. Attempt MAGIC ZEROCOPY Path ─────────────────────────────────────
        if let Ok(Some(headers)) = self
            .storage
            .read_entry_headers(next_index, LogIndex(u64::MAX))
        {
            let limit = self.config.limits.append_batch_entries.max(1);
            let headers = if headers.len() > limit {
                &headers[..limit]
            } else {
                headers
            };

            if let Some(last_header) = headers.last() {
                let last_idx = LogIndex(last_header.index.get());
                let to_idx = LogIndex(next_index.0 + headers.len() as u64 - 1);

                // Collect payload views into a dock-recycled LOCAL vec. The
                // slices borrow `self.storage` (see the `for_each_payload`
                // contract); the borrow checker verifies them — nothing is
                // laundered to 'static or parked in `self` (US3).
                let mut payload_refs = self.scratch_payload_refs.take();
                if let Err(e) = self
                    .storage
                    .for_each_payload(next_index, to_idx, &mut |p| payload_refs.push(p))
                {
                    self.scratch_payload_refs.put(payload_refs);
                    return Err(e);
                }

                let req = crate::protocol::AppendEntries {
                    term: self.hard_state.current_term.0.into(),
                    leader_id: self.config.node_id.0.into(),
                    prev_log_index: prev_idx.0.into(),
                    prev_log_term: prev_term.0.into(),
                    leader_commit: self.soft_state.commit_index.0.into(),
                    entry_count: (headers.len() as u32).into(),
                    // A11: ReadIndex probe token (echoed by the follower).
                    _pad: self.read_probe_seq.into(),
                };

                let msg = RaftMessage::AppendEntriesSeededVectored {
                    ae: &req,
                    headers: headers.as_bytes(),
                    payloads: &payload_refs,
                };

                // The message borrows `self.storage`, so it cannot go through
                // `send_message(&mut self)`; the free function takes only the
                // disjoint fields it needs (transport, outbound buffer, iovec
                // dock) under normal split-borrow checking.
                let mut iovs = self.scratch_vectored.take();
                let sent = super::super::send_message_vectored(
                    &self.transport,
                    self.config.node_id,
                    &mut self.scratch_outbound,
                    &mut iovs,
                    peer,
                    &msg,
                )
                .await;
                self.scratch_vectored.put(iovs);
                self.scratch_payload_refs.put(payload_refs);
                return Ok(if sent { Some(last_idx) } else { None });
            }
        }

        // ── 2. Fallback to ERGONOMIC Path ──────────────────────────────────────
        // Read the backlog into a dock-recycled LOCAL vec whose entries
        // borrow `scratch_payload` — a split field borrow the compiler
        // checks, replacing the old transmute-into-`self.scratch_entries`.
        let mut entries = self.scratch_entries.take();
        if let Err(e) = self.storage.read_entries(
            next_index,
            LogIndex(u64::MAX),
            &mut entries,
            &mut self.scratch_payload,
        ) {
            self.scratch_entries.put(entries);
            return Err(e);
        }
        entries.truncate(self.config.limits.append_batch_entries.max(1));
        let last_idx = entries.last().map(|e| e.index).unwrap_or(prev_idx);

        // Create AppendEntries metadata on stack
        let req = crate::protocol::AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_idx.0.into(),
            prev_log_term: prev_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (entries.len() as u32).into(),
            // A11: ReadIndex probe token (echoed by the follower).
            _pad: self.read_probe_seq.into(),
        };

        let msg = RaftMessage::AppendEntriesVectored(&req, &entries);
        let mut iovs = self.scratch_vectored.take();
        let sent = super::super::send_message_vectored(
            &self.transport,
            self.config.node_id,
            &mut self.scratch_outbound,
            &mut iovs,
            peer,
            &msg,
        )
        .await;
        self.scratch_vectored.put(iovs);
        self.scratch_entries.put(entries);
        if sent {
            Ok(Some(last_idx))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn initialize_leader_progress(&mut self) -> Result<(), RaftError> {
        self.peer_progress.clear();
        self.last_voter_contact.clear();
        let last_index = self.cached_last_log.0;
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.peer_progress.insert(
                peer,
                super::super::progress::PeerProgress {
                    // B8 arithmetic policy: saturating at the boundary.
                    next_index: LogIndex(last_index.0.saturating_add(1)),
                    match_index: LogIndex(0),
                },
            );
            self.last_voter_contact
                .insert(peer, std::time::Instant::now());
        }
        // A13: learners get replication progress like followers, but NO
        // check-quorum contact stamp — a learner's liveness must never help
        // keep the leader's quorum lease alive (`check_quorum_active` only
        // reads voters, and `handle_inbound` only stamps voters, so the
        // omission here keeps all three sites consistent).
        for peer in self
            .config
            .learners
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.peer_progress.insert(
                peer,
                super::super::progress::PeerProgress {
                    next_index: LogIndex(last_index.0.saturating_add(1)),
                    match_index: LogIndex(0),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn ensure_leader_progress_initialized(&mut self) -> Result<(), RaftError> {
        if self.peer_progress.is_empty() {
            self.initialize_leader_progress()?;
        }
        Ok(())
    }

    pub(crate) fn term_at(&mut self, index: LogIndex) -> Result<Term, RaftError> {
        if index.0 == 0 {
            return Ok(Term(0));
        }
        // The last-log cache is authoritative for the tip. After a snapshot
        // restore/install the boundary entry itself is discarded from storage
        // (`truncate_before(boundary + 1)`) while the tip position lives on in
        // `cached_last_log` — without this fallback a leader that just
        // restored from its own snapshot could not even build a bare
        // heartbeat (prev = its own last index would read as CorruptLog).
        // This is the send-side twin of the receive-side `prev_ok`
        // snapshot-boundary fallback in `replication/handler.rs` (C3).
        if index == self.cached_last_log.0 {
            return Ok(self.cached_last_log.1);
        }
        if let Some(term) = self.log_metadata.get_term(index) {
            return Ok(term);
        }
        // §7: the snapshot boundary's term must stay answerable after the
        // boundary entry itself was discarded (restore/install truncate the
        // log through the boundary) — e.g. the very first propose after a
        // snapshot restore needs prev_log_term at the boundary (C3).
        if index == self.snapshot_boundary.0 {
            return Ok(self.snapshot_boundary.1);
        }
        // B10: term-only probe via `RaftStorage::term_at` — no payload read,
        // and no reliance on `entry_at` tolerating an undersized buffer (a
        // compliant strict-buffer storage would error and kill the node here
        // after a restart, when the metadata arena is cold).
        let term = self
            .storage
            .term_at(index)?
            .ok_or_else(|| RaftError::CorruptLog(format!("missing term at index {}", index.0)))?;
        self.log_metadata.append(index, term);
        Ok(term)
    }

    /// Whether `index` currently satisfies the commit quorum of the EFFECTIVE
    /// configuration: dual-quorum (majority-of-old AND majority-of-new) while a
    /// joint transition is active, else a simple majority of `config.peers`.
    ///
    /// Routes every arm through [`subset_quorum_index_into`] so the
    /// synchronous propose path and the async run-loop agree on exactly when an
    /// entry becomes committable — this is what closes the joint-consensus
    /// data-loss hole (a Joint entry must not be reported committed on a
    /// union-only majority). `&mut self` only for the `scratch_commit_acks`
    /// working buffer (no allocation on any path).
    #[inline]
    pub(crate) fn index_meets_commit_quorum(&mut self, index: LogIndex) -> bool {
        let last_index = self.cached_last_log.0;
        let self_id = self.config.node_id;
        match &self.joint_peers {
            Some((old_peers, new_peers)) => {
                let old_ok = subset_quorum_index_into(
                    &mut self.scratch_commit_acks,
                    &self.peer_progress,
                    self_id,
                    old_peers,
                    last_index,
                )
                .is_some_and(|q| q >= index);
                let new_ok = subset_quorum_index_into(
                    &mut self.scratch_commit_acks,
                    &self.peer_progress,
                    self_id,
                    new_peers,
                    last_index,
                )
                .is_some_and(|q| q >= index);
                old_ok && new_ok
            }
            None => subset_quorum_index_into(
                &mut self.scratch_commit_acks,
                &self.peer_progress,
                self_id,
                &self.config.peers,
                last_index,
            )
            .is_some_and(|q| q >= index),
        }
    }

    /// Stop condition for the synchronous propose-time ack gather. Keeps the
    /// non-joint hot path on the cheap `accepted >= needed` counter and only
    /// falls back to the full dual-quorum check while joint — config
    /// changes are rare, steady-state proposals are not.
    #[inline]
    pub(crate) fn propose_commit_reached(
        &mut self,
        needed: usize,
        accepted: usize,
        last_index: LogIndex,
    ) -> bool {
        if self.joint_peers.is_some() {
            self.index_meets_commit_quorum(last_index)
        } else {
            accepted >= needed
        }
    }

    /// Check whether a quorum of peers has replicated the latest entries and, if so,
    /// advance `commit_index` to the highest index confirmed by a quorum.
    ///
    /// During a joint-consensus transition (`node.joint_peers.is_some()`) the
    /// commit rule requires majority-of-`old_peers` AND majority-of-`new_peers`
    /// (Raft §4.3). The effective quorum index is the min of the two — either
    /// sub-set can veto commit progress.
    pub(crate) fn try_advance_commit_index(&mut self) -> Result<(), RaftError> {
        // A single-node cluster (self is the whole voter set) is its own
        // quorum and must commit without any peer progress. Only bail early
        // for a MULTI-node leader whose peers have not yet been initialized —
        // there we genuinely cannot confirm a quorum. (Fixes C8.)
        if self.config.peers.len() > 1 && self.peer_progress.is_empty() {
            return Ok(());
        }
        let last_index = self.cached_last_log.0;
        if last_index <= self.soft_state.commit_index {
            return Ok(());
        }

        // Every arm goes through `subset_quorum_index_into` (dup-F2) so the
        // propose-time predicate (`index_meets_commit_quorum`) and this
        // run-loop advance can never disagree on the commit rule. The gather
        // uses `scratch_commit_acks` (NOT `scratch_indexes`) so a concurrent
        // propose's return slice is never clobbered (G1). A13: the gather is
        // by VOTER identity — learners keep progress entries for replication,
        // but a learner's ack must never advance the commit index; a voter
        // with no progress entry contributes `LogIndex(0)` so a freshly-added
        // voter is never silently skipped.
        let self_id = self.config.node_id;
        let quorum_index = match &self.joint_peers {
            Some((old_peers, new_peers)) => {
                let Some(old_q) = subset_quorum_index_into(
                    &mut self.scratch_commit_acks,
                    &self.peer_progress,
                    self_id,
                    old_peers,
                    last_index,
                ) else {
                    return Ok(());
                };
                let Some(new_q) = subset_quorum_index_into(
                    &mut self.scratch_commit_acks,
                    &self.peer_progress,
                    self_id,
                    new_peers,
                    last_index,
                ) else {
                    return Ok(());
                };
                std::cmp::min(old_q, new_q)
            }
            None => {
                let Some(q) = subset_quorum_index_into(
                    &mut self.scratch_commit_acks,
                    &self.peer_progress,
                    self_id,
                    &self.config.peers,
                    last_index,
                ) else {
                    return Ok(());
                };
                q
            }
        };

        if quorum_index <= self.soft_state.commit_index {
            return Ok(());
        }
        // Safety rule: only commit if the quorum entry belongs to current_term.
        let quorum_term = if quorum_index == self.cached_last_log.0 {
            self.cached_last_log.1
        } else if let Some(term) = self.log_metadata.get_term(quorum_index) {
            term
        } else {
            // B10: term-only probe — no payload read.
            let term = self
                .storage
                .term_at(quorum_index)?
                .unwrap_or(crate::Term(0));
            if term.0 > 0 {
                self.log_metadata.append(quorum_index, term);
            }
            term
        };
        if quorum_term == self.hard_state.current_term {
            self.set_commit_index(quorum_index);
        }
        Ok(())
    }

    pub(crate) async fn drain_inbound_ready(&mut self) -> Result<(), RaftError> {
        // Reuse preallocated scratch_quorum_buf to avoid 64KB allocation in
        // the hot propose path. B5: grow with `resize` — which zero-fills any
        // newly exposed bytes — instead of `reserve` + `set_len`, which would
        // expose uninitialized heap memory if the reserve reallocated. Once
        // the buffer is at 64 KiB this is a no-op.
        if self.scratch_quorum_buf.len() < 64 * 1024 {
            self.scratch_quorum_buf.resize(64 * 1024, 0);
        }

        // Take the buffer to satisfy the borrow checker during zero-copy decode & handle
        struct BufferGuard<'a, S, T> {
            node: &'a mut RaftNode<S, T>,
            buf: Vec<u8>,
        }
        impl<S, T> Drop for BufferGuard<'_, S, T> {
            fn drop(&mut self) {
                self.node.scratch_quorum_buf = std::mem::take(&mut self.buf);
            }
        }

        let mut guard = BufferGuard {
            buf: std::mem::take(&mut self.scratch_quorum_buf),
            node: self,
        };

        // This drain runs AFTER the batch is already committed. A decode/handle
        // failure here must NOT surface as an error to the caller — that would
        // report a committed write as failed and invite a duplicate submission
        // (G5). Skip bad frames; only a Fatal-class error stops the node.
        loop {
            let recv = match guard
                .node
                .transport
                .recv_frame_timeout(Duration::ZERO, &mut guard.buf)
                .await
            {
                Ok(opt) => opt,
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "tolerating non-fatal recv while draining post-commit inbound");
                    break;
                }
            };
            let Some(n) = recv else { break };
            let inbound = match crate::decode_message(&guard.buf[..n]) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "dropping undecodable frame while draining post-commit inbound");
                    continue;
                }
            };
            guard
                .node
                .handle_inbound_tolerant(inbound, "post-commit drain")
                .await?;
        }
        Ok(())
    }
}

/// Highest log index that a strict majority of `subset` has replicated —
/// the ONE quorum-index computation (dup-F2) behind both the propose-time
/// predicate (`index_meets_commit_quorum`) and the run-loop commit advance
/// (`try_advance_commit_index`); the commit rule must never disagree
/// between them.
///
/// Counts the leader's own log tip (`last_index`) only if `self_id` appears
/// in `subset`, and each other member's `match_index` from `peer_progress`.
/// Members of `subset` with no progress entry contribute `LogIndex(0)`,
/// which prevents a newly-added voter from being silently ignored by the
/// quorum computation while it is still catching up.
///
/// `acks` is a caller-provided working buffer (cleared here, contents
/// meaningless afterward) so NO call site allocates — the node threads
/// `scratch_commit_acks` through.
#[inline]
fn subset_quorum_index_into(
    acks: &mut Vec<LogIndex>,
    peer_progress: &super::super::PeerMap<super::super::PeerProgress>,
    self_id: PeerId,
    subset: &[PeerId],
    last_index: LogIndex,
) -> Option<LogIndex> {
    if subset.is_empty() {
        return None;
    }
    acks.clear();
    for &peer in subset {
        let match_index = if peer == self_id {
            last_index
        } else {
            peer_progress
                .get(&peer)
                .map(|p| p.match_index)
                .unwrap_or(LogIndex(0))
        };
        acks.push(match_index);
    }
    // Sort descending → quorum-th largest is the safe commit point.
    acks.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let q = super::super::quorum(subset.len());
    acks.get(q.saturating_sub(1)).copied()
}
