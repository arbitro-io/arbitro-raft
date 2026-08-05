// Linearizable reads via ReadIndex (Raft §6.4, dissertation) — A11.
//
// The protocol, on the leader:
//
//   1. **Current-term commit guard** — a new leader's `commit_index` is only
//      proven complete (Leader Completeness) once at least one entry OF ITS
//      OWN TERM has committed. This crate appends NO automatic no-op at
//      election win (`campaign_once` just flips the role), so when the guard
//      is not yet satisfied `read_index` appends a control no-op entry and
//      commits it through the normal propose path. That propose IS a quorum
//      round at the current term that happened after the read began, so it
//      doubles as the confirmation step and the method returns immediately
//      with the resulting commit index.
//   2. **Record** `read_index = commit_index`.
//   3. **Quorum confirmation** — exchange a round of heartbeats and observe
//      current-term acks from a majority of the voter set (the leader counts
//      itself; during a §4.3 joint transition BOTH sides must reach majority
//      independently, mirroring the election and commit rules). Bounded by an
//      absolute deadline of one election timeout — quorum not confirmed in
//      time means this node may be a deposed leader still unaware of a higher
//      term, and the read MUST fail rather than risk staleness.
//   4. **Apply wait** — performed by the caller: the state machine reflects
//      the read point only once `last_applied >= read_index`. `RaftNode`
//      does not own the apply loop (apply is external / SM-side), so
//      [`RaftNode::read_index`] returns the confirmed index and documents the
//      obligation; [`crate::ArbitroRaft::read_index`] discharges it by
//      applying committed entries before returning.
//
// Safety argument (why a majority of current-term acks suffices): any
// competing leader must have won an election at a term T' > confirm_term,
// which requires a majority of voters to have adopted T'. A voter that
// adopted T' replies with T' (stepping this node down), never with
// confirm_term. Therefore a majority replying AT confirm_term proves no
// higher-term leader existed at the time those replies were GENERATED — so
// `commit_index` captured at step 2 covers every write linearized before the
// read... PROVIDED every counted reply was generated after the read began.
// That freshness is what the probe token provides: each confirmation round
// bumps `read_probe_seq`, stamps it into the reserved word of its heartbeats
// (`AppendEntries._pad`), and counts only acks that echo it back
// (`AppendEntriesResp._pad[0..4]`). An ack that was already buffered or in
// flight when the read began echoes an older token (or 0, from the era when
// the word was pure padding) and is never counted — the same role etcd's
// read-request context plays in its MsgHeartbeat/MsgHeartbeatResp exchange.
//
// A11: lease-read variant is future work (needs a monotonic-clock lease;
// ReadIndex is the safe default).

use std::time::{Duration, Instant};

use super::RaftNode;
use crate::{InboundRaftMessage, LogIndex, PeerId, RaftError, RaftMessage, Term};
use tracing::warn;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Linearizable read point (Raft §6.4 ReadIndex).
    ///
    /// Returns a `LogIndex` such that a state-machine read performed after
    /// applying every entry up to (at least) that index reflects every write
    /// that completed before this call — i.e. the read linearizes at the
    /// moment of quorum confirmation.
    ///
    /// **Caller contract**: the returned index is a *read point*, not an
    /// applied guarantee. Before reading the state machine the caller MUST
    /// wait until `last_applied >= read_index` (drive the apply loop). The
    /// [`crate::ArbitroRaft::read_index`] wrapper does this for you.
    ///
    /// Leader-only. `inbound_buf` is the caller-owned frame buffer (same
    /// contract as [`RaftNode::campaign_once`] /
    /// [`RaftNode::transfer_leadership`]).
    ///
    /// Errors:
    /// - [`RaftError::NotLeader`] — not the leader (redirect hint included),
    ///   a §4.2.3 transfer freeze is active (hint = incoming leader), or this
    ///   node was deposed mid-confirmation.
    /// - [`RaftError::NoQuorum`] — a majority could not be contacted within
    ///   one election timeout. A partitioned or deposed leader lands here:
    ///   the read is REFUSED, never served stale.
    ///
    /// A11: lease-read variant is future work (needs a monotonic-clock
    /// lease; ReadIndex is the safe default).
    pub async fn read_index(&mut self, inbound_buf: &mut [u8]) -> Result<LogIndex, RaftError> {
        if !self.is_leader() {
            return Err(self.not_leader_error());
        }
        // §4.2.3 transfer freeze: reads redirect exactly like proposals —
        // the incoming leader is about to own the linearization order.
        self.check_transfer_freeze()?;

        let confirm_term = self.hard_state.current_term;

        // ── 1. Current-term commit guard (§6.4 / §5.4.2) ──────────────────
        // `commit_index` is only a safe read point once an entry of THIS
        // term has committed. `try_advance_commit_index` already refuses to
        // advance commit onto prior-term entries, but a fresh leader can
        // still carry a commit index it adopted as a follower — its term
        // must be checked. A `term_at` failure (e.g. the entry was compacted
        // below the snapshot boundary after a restart) conservatively falls
        // to the no-op path, which is always correct, merely one round
        // slower.
        let commit = self.soft_state.commit_index;
        let guard_ok = commit.0 > 0 && matches!(self.term_at(commit), Ok(t) if t == confirm_term);
        if !guard_ok {
            // This crate appends NO automatic no-op at election win, so the
            // guard is satisfied here, on first demand, by committing a
            // control no-op (0xC0-reserved prefix; the apply loop consumes
            // it — it never reaches the user state machine). The propose's
            // own quorum gather is a current-term majority round that
            // happened entirely after this call began, so it doubles as the
            // §6.4 confirmation step: return the post-commit index directly.
            let noop = crate::api::node::membership::noop_entry();
            let noop_idx = self.propose_once(&noop).await?;
            debug_assert!(self.soft_state.commit_index >= noop_idx);
            return Ok(self.soft_state.commit_index);
        }

        // ── 2. Record the read point BEFORE confirmation ──────────────────
        let read_index = commit;

        // ── 3. Quorum confirmation (E4 quorum-confirmed predicate) ────────
        self.confirm_leadership_quorum(confirm_term, inbound_buf)
            .await?;

        // ── 4. Apply wait is the caller's obligation (see docs above). ────
        Ok(read_index)
    }

    /// E4-minimal internal predicate: prove this node is STILL the leader of
    /// `confirm_term` by observing current-term `AppendEntriesResp` acks from
    /// a majority of the voter set (self included) within one election
    /// timeout. Heartbeats are (re-)sent at the heartbeat cadence; inbound
    /// frames are drained and handled normally, so a higher-term frame steps
    /// this node down and fails the predicate with [`RaftError::NotLeader`].
    ///
    /// During a §4.3 joint transition the predicate requires a majority of
    /// C_old AND a majority of C_new independently — the same dual rule
    /// elections and commits obey.
    ///
    /// On deadline expiry without quorum: [`RaftError::NoQuorum`]. The
    /// caller must NOT serve the read; the check-quorum lease in the run
    /// loop handles the eventual step-down.
    ///
    /// NOTE (E4): this is deliberately the *minimal* helper ReadIndex needs.
    /// A fuller reusable quorum-confirmed abstraction (shared with
    /// check-quorum and future lease reads) remains separate work.
    pub(crate) async fn confirm_leadership_quorum(
        &mut self,
        confirm_term: Term,
        inbound_buf: &mut [u8],
    ) -> Result<(), RaftError> {
        // Freshness token: bump the probe sequence FIRST. Every heartbeat
        // (and append) built from here on carries the new value, and
        // followers echo it back in `AppendEntriesResp._pad[0..4]`. Only
        // acks echoing THIS value count — an ack that was already buffered
        // or in flight when the read began echoes an older value (or 0) and
        // is worthless as proof of still-leadership. Without this, a
        // partitioned leader could "confirm" its quorum from its own inbound
        // backlog and serve a stale read.
        self.read_probe_seq = self.read_probe_seq.wrapping_add(1);
        if self.read_probe_seq == 0 {
            self.read_probe_seq = 1; // 0 is reserved for "no probe"
        }
        let probe = self.read_probe_seq;

        // Voters that acked `confirm_term` with a fresh echo; tracked by
        // identity (not a counter) for the joint-config dual majority.
        let mut acks: Vec<PeerId> = Vec::with_capacity(self.config.peers.len());
        acks.push(self.config.node_id); // the leader counts itself
        if self.voter_majority(&acks) {
            return Ok(()); // single-node cluster: self IS the majority
        }

        // Absolute deadline — mirrors the pre-vote gather and the §4.2.3
        // catch-up loop: a peer trickling irrelevant frames must never
        // extend the window.
        let deadline = Instant::now() + self.election_timeout();
        let heartbeat = Duration::from_millis(self.config.timing.heartbeat_ms.max(1));
        let mut next_probe = Instant::now(); // first probe fires immediately

        loop {
            // Deposed (or term moved) mid-confirmation → the predicate fails.
            if !self.is_leader() || self.hard_state.current_term != confirm_term {
                return Err(self.not_leader_error());
            }
            if self.voter_majority(&acks) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                warn!(
                    node_id = self.config.node_id.0,
                    term = confirm_term.0,
                    acks = acks.len(),
                    needed = super::quorum(self.config.peers.len()),
                    "read-index quorum confirmation timed out; refusing read"
                );
                return Err(RaftError::NoQuorum);
            }

            // (Re-)probe at heartbeat cadence — resilient to lost frames.
            if now >= next_probe {
                self.send_heartbeat_once().await?;
                next_probe = now + heartbeat;
            }

            // Drain inbound until the next probe tick (or the deadline).
            let slice_deadline = deadline.min(next_probe);
            let mut msg_slots: [Option<InboundRaftMessage<'_>>; 16] = [None; 16];
            let n = self
                .drain_inbound_frames(slice_deadline, inbound_buf, &mut msg_slots)
                .await?;
            for slot in msg_slots.iter().take(n) {
                // Slots `0..n` are populated by `drain_inbound_frames`; an
                // empty one is impossible, but skipping is strictly safer
                // than panicking (B13).
                let Some(inbound) = *slot else { continue };
                let from = inbound.from;
                // Record the ack BEFORE handling: an `AppendEntriesResp`
                // carrying exactly `confirm_term` — success or reject — AND
                // echoing THIS round's probe token proves the sender still
                // recognized this leader's term at a moment after the round
                // began (the echo can only come from a heartbeat sent after
                // the bump above). Only configured voters count. Stale
                // buffered acks echo an older token and are ignored here
                // (though still handled normally below).
                if let RaftMessage::AppendEntriesResp(resp) = inbound.message {
                    let echoed = u32::from_le_bytes([
                        resp._pad[0],
                        resp._pad[1],
                        resp._pad[2],
                        resp._pad[3],
                    ]);
                    if Term(resp.term.get()) == confirm_term
                        && echoed == probe
                        && self.config.peers.contains(&from)
                        && !acks.contains(&from)
                    {
                        acks.push(from);
                    }
                }
                // Normal handling: advances peer progress, steps us down on a
                // higher term (caught by the loop-top check). A non-fatal
                // handler error drops the frame, never the confirmation.
                self.handle_inbound_tolerant(inbound, "read-index confirmation")
                    .await?;
            }
        }
    }
}
