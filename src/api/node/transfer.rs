// Leadership transfer (Raft §4.2.3).
//
// The transfer runs in two phases on the leader:
//
//   1. Catch-up — replicate to the target until `target.match_index` reaches
//      the leader's last log index, bounded by one election timeout. While
//      catching up the leader keeps its normal heartbeat duty so the rest of
//      the cluster stays quiet. If the target cannot catch up in time the
//      transfer ABORTS: the leader resumes normal operation and the caller
//      gets [`RaftError::TransferTimeout`].
//
//   2. Sanction — send [`TimeoutNow`] to the (now fully caught-up) target and
//      freeze client proposals for at most one more election timeout. The
//      target starts an election at `term + 1` immediately, bypassing BOTH
//      the election-timeout wait and the pre-vote phase: pre-vote's
//      leader-stickiness exists to stop *disruptive* elections, and a
//      leader-sanctioned transfer is by definition not disruptive (this is
//      the standard §4.2.3 behavior). The old leader steps down through the
//      existing higher-term path the moment the target's `RequestVote` at
//      `term + 1` arrives.
//
// Safety: Election Safety is never weakened. The target campaigns at a
// strictly higher term through the ordinary `campaign_once` path, so every
// vote is still persisted and log-up-to-date-checked; the old leader cannot
// act at the new term (its own term guard steps it down). Because the target
// is confirmed caught up BEFORE `TimeoutNow` is sent, every committed entry
// is in the target's log and Leader Completeness holds. Even if a client
// entry raced in after the catch-up check, it could only have committed on a
// majority whose logs then beat the target's — those voters would deny the
// vote — so a committed entry can never be lost by a transfer (the proposal
// freeze is a liveness/UX measure, not a safety requirement).

use std::time::{Duration, Instant};

use super::RaftNode;
use crate::protocol::codec::wire::TimeoutNow;
use crate::{InboundRaftMessage, LogIndex, PeerId, RaftError, RaftMessage, Term};
use tracing::{info, warn};

/// Leader-side record of an in-flight transfer: after `TimeoutNow` is sent,
/// client proposals are rejected (with a redirect hint to `target`) until the
/// target's higher term deposes us or `deadline` expires (abort → resume).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingTransfer {
    pub(crate) target: PeerId,
    pub(crate) deadline: Instant,
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Transfer leadership to `target` (Raft §4.2.3).
    ///
    /// Only the leader may initiate — otherwise [`RaftError::NotLeader`].
    /// `Ok(())` means the handoff was INITIATED: the target was confirmed
    /// fully caught up (`match_index == last_log_index`) and `TimeoutNow`
    /// was handed to the transport. The target then campaigns at `term + 1`
    /// and this node steps down when it observes that term; callers should
    /// keep driving the run loop and watch [`RaftNode::role`] /
    /// [`RaftNode::leader_id`] for the outcome. Until the outcome (or one
    /// election timeout) client proposals are rejected with a `NotLeader`
    /// redirect hint pointing at `target`.
    ///
    /// Errors:
    /// - [`RaftError::NotLeader`] — not the leader, or deposed mid-transfer.
    /// - [`RaftError::PeerUnknown`] — `target` is not in the voter set.
    /// - [`RaftError::TransferTimeout`] — the target could not catch up
    ///   within one election timeout; leadership resumed unchanged.
    ///
    /// `inbound_buf` is the caller-owned frame buffer (same contract as
    /// [`RaftNode::campaign_once`]).
    pub async fn transfer_leadership(
        &mut self,
        target: PeerId,
        inbound_buf: &mut [u8],
    ) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(self.not_leader_error());
        }
        // Transferring to ourselves is a no-op: we already lead.
        if target == self.config.node_id {
            return Ok(());
        }
        if !self.config.peers.contains(&target) {
            return Err(RaftError::PeerUnknown(target));
        }
        self.ensure_leader_progress_initialized()?;

        let transfer_term = self.hard_state.current_term;
        let deadline = Instant::now() + self.election_timeout();
        let heartbeat = Duration::from_millis(self.config.timing.heartbeat_ms.max(1));
        let mut next_probe = Instant::now();
        // Resend to the target as soon as its next_index moves, without
        // waiting a full heartbeat interval — but never re-send the same
        // window twice back-to-back while its ack is still in flight.
        let mut last_probed_next = LogIndex(u64::MAX);

        info!(
            node_id = self.config.node_id.0,
            target = target.0,
            term = transfer_term.0,
            "leadership transfer: catching target up"
        );

        // ── Phase 1: catch the target up to our last log index ────────────
        loop {
            // Deposed (or term moved) while catching up → abort as NotLeader.
            if !self.is_leader() || self.hard_state.current_term != transfer_term {
                return Err(self.not_leader_error());
            }
            let last_log = self.cached_last_log.0;
            let caught_up = self
                .peer_progress
                .get(&target)
                .is_some_and(|p| p.match_index >= last_log);
            if caught_up {
                break;
            }

            let now = Instant::now();
            if now >= deadline {
                // ABORT — resume normal leadership untouched.
                warn!(
                    node_id = self.config.node_id.0,
                    target = target.0,
                    "leadership transfer aborted: target did not catch up within election timeout"
                );
                return Err(RaftError::TransferTimeout(target));
            }

            // Heartbeat cadence: replicates the target's backlog (the
            // behind-peer branch of `send_heartbeat_once` ships entries, not
            // a bare heartbeat) AND keeps every other follower's election
            // timer fed so the transfer window cannot spark a spurious
            // campaign elsewhere.
            if now >= next_probe {
                self.send_heartbeat_once().await?;
                next_probe = now + heartbeat;
                last_probed_next = self
                    .peer_progress
                    .get(&target)
                    .map(|p| p.next_index)
                    .unwrap_or(LogIndex(0));
            } else {
                // Fast path between heartbeats: if the target's ack advanced
                // its window, ship the next batch immediately.
                let target_next = self
                    .peer_progress
                    .get(&target)
                    .map(|p| p.next_index)
                    .unwrap_or(LogIndex(0));
                if target_next != last_probed_next {
                    self.send_append_attempt(target, 0).await?;
                    last_probed_next = target_next;
                }
            }

            // Drain inbound until the next probe tick (or the deadline) —
            // acks from the target advance `match_index` via the normal
            // response handler; any higher-term frame steps us down and the
            // loop-top check aborts.
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
                self.handle_inbound_tolerant(inbound, "leadership transfer")
                    .await?;
            }
        }

        // ── Phase 2: target is fully caught up — sanction its election ────
        let msg = TimeoutNow {
            term: transfer_term.0.into(),
            leader_id: self.config.node_id.0.into(),
        };
        if !self
            .send_message(target, &RaftMessage::TimeoutNow(&msg))
            .await
        {
            // Transport refused the frame — nothing was sanctioned, leadership
            // is untouched; surface it so the caller can retry.
            return Err(RaftError::Transport(
                "leadership transfer: failed to send TimeoutNow to target".into(),
            ));
        }

        // Freeze proposals until the target's term deposes us or the window
        // expires (abort → resume). §4.2.3: the prior leader stops accepting
        // client requests so the target cannot fall behind again mid-handoff.
        self.pending_transfer = Some(PendingTransfer {
            target,
            deadline: Instant::now() + self.election_timeout(),
        });
        info!(
            node_id = self.config.node_id.0,
            target = target.0,
            term = transfer_term.0,
            "leadership transfer: target caught up, TimeoutNow sent"
        );
        Ok(())
    }

    /// §4.2.3 transfer-freeze guard (dup-F3): while a leadership handoff is
    /// in flight, new client work (proposals, reads) is rejected with a
    /// redirect hint at the INCOMING leader — the transfer target — so the
    /// sanctioned node cannot fall behind again mid-handoff.
    #[inline]
    pub(crate) fn check_transfer_freeze(&mut self) -> Result<(), RaftError> {
        if self.leadership_transfer_in_progress() {
            return Err(RaftError::NotLeader {
                leader_hint: self.pending_transfer.map(|t| crate::LeaderHint {
                    leader_id: t.target,
                }),
            });
        }
        Ok(())
    }

    /// True while a §4.2.3 handoff is awaiting resolution. Expired windows are
    /// cleared lazily here (abort semantics: the target never campaigned, so
    /// the leader resumes accepting proposals).
    pub(crate) fn leadership_transfer_in_progress(&mut self) -> bool {
        match self.pending_transfer {
            Some(t) if Instant::now() < t.deadline => true,
            Some(_) => {
                self.pending_transfer = None;
                false
            }
            None => false,
        }
    }

    /// Target-side handler for [`TimeoutNow`]: arm an immediate, pre-vote-free
    /// campaign. The campaign itself runs from the run loop (which owns the
    /// frame buffer) via [`RaftNode::take_forced_campaign`].
    pub(crate) async fn handle_timeout_now(
        &mut self,
        from: PeerId,
        msg: &TimeoutNow,
    ) -> Result<(), RaftError> {
        let msg_term = Term(msg.term.get());

        // Stale sanction from a deposed leader — ignore.
        if msg_term.0 < self.hard_state.current_term.0 {
            return Ok(());
        }
        if msg_term.0 > self.hard_state.current_term.0 {
            // Adopt the newer term first (standard higher-term rule); the
            // forced campaign below then bumps strictly past it.
            self.step_down(msg_term)?;
        }
        // Already leading this term — nothing to do.
        if self.is_leader() {
            return Ok(());
        }
        // Self-membership guard (§4.2.2): a non-voter never campaigns, even
        // when sanctioned. `campaign_once` re-checks, but don't even arm.
        if !self.config.peers.contains(&self.config.node_id) {
            return Ok(());
        }
        // At an equal term the sanction must come from the leader we follow
        // (or from an unknown leader right after adopting the term). Any
        // other same-term peer has no authority to force an election.
        if msg_term == self.hard_state.current_term {
            if let Some(leader) = self.soft_state.leader_id {
                if leader != from {
                    return Ok(());
                }
            }
        }

        info!(
            node_id = self.config.node_id.0,
            from = from.0,
            term = msg_term.0,
            "received TimeoutNow; arming immediate leader-sanctioned campaign"
        );
        self.forced_campaign_term = Some(self.hard_state.current_term);
        Ok(())
    }

    /// Consume the armed [`TimeoutNow`] sanction, if still valid. Returns
    /// `true` when the run loop should campaign IMMEDIATELY (no election
    /// timeout, no pre-vote). The sanction is dropped if the term moved on
    /// since it was armed — a campaign at `stale_term + 1` could re-disrupt a
    /// cluster that already elected past it.
    pub(crate) fn take_forced_campaign(&mut self) -> bool {
        match self.forced_campaign_term.take() {
            Some(t) => {
                t == self.hard_state.current_term
                    && !self.is_leader()
                    && self.config.peers.contains(&self.config.node_id)
            }
            None => false,
        }
    }
}
