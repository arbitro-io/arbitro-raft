use crate::api::node::progress::AppendAdvance;
use crate::RaftNode;
use crate::{AppendEntriesResp, InboundRaftMessage, LogIndex, PeerId, RaftError, RaftMessage};
use std::time::{Duration, Instant};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Route an `AppendEntriesResp` during quorum wait: advance replication state for pending
    /// peers or forward to the normal response handler for peers not in `scratch_pending`.
    pub(super) async fn process_append_resp(
        &mut self,
        from: PeerId,
        resp: &AppendEntriesResp,
        last_index: LogIndex,
        accepted: &mut usize,
    ) -> Result<(), RaftError> {
        if let Some(state) = self.scratch_pending.get(&from).copied() {
            if let AppendAdvance::Completed = self
                .advance_append_replication(from, last_index, state, resp)
                .await?
            {
                self.scratch_pending.remove(&from);
                // A13: only a VOTER's completed ack moves the commit counter.
                // Learners are part of the initial fan-out (they replicate
                // like followers, and `advance_append_replication` above has
                // already advanced their progress), but their ack must never
                // count toward `needed` — in a 3-voter + 1-learner cluster an
                // entry acked by the leader and the learner alone must NOT
                // commit.
                if self.config.peers.contains(&from) {
                    *accepted += 1;
                }
            }
        } else {
            self.handle_append_entries_response(from, resp).await?;
        }
        Ok(())
    }

    /// Block until a quorum has acknowledged `last_index` or the absolute
    /// deadline (`now + timeout`) expires (B11): a peer stream that keeps
    /// delivering non-quorum frames must not extend the gather window forever.
    /// Returns the number of accepted acknowledgements (leader self-ack = 1 on entry).
    pub(super) async fn gather_quorum_acks(
        &mut self,
        needed: usize,
        last_index: LogIndex,
        timeout: Duration,
    ) -> Result<usize, RaftError> {
        let mut accepted = 1usize;
        // B11: absolute deadline, mirroring the pre-vote gather in
        // `election.rs`. Without it a slow/hostile peer that trickles
        // irrelevant frames resets the per-recv timeout indefinitely.
        let deadline = Instant::now() + timeout;

        // Use pre-allocated quorum buffer. B5: grow with `resize` — which
        // zero-fills any newly exposed bytes — instead of `reserve` +
        // `set_len`, which would expose uninitialized heap memory if the
        // reserve reallocated. Once the buffer is at 64 KiB this is a no-op.
        if self.scratch_quorum_buf.len() < 64 * 1024 {
            self.scratch_quorum_buf.resize(64 * 1024, 0);
        }

        // RAII Guard to ensure the buffer is returned to self even on error/panic.
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

        while !guard
            .node
            .propose_commit_reached(needed, accepted, last_index)
            && !guard.node.scratch_pending.is_empty()
        {
            // Burst-drain: consume all immediately-available frames before yielding.
            loop {
                let n = match guard
                    .node
                    .transport
                    .recv_frame_timeout(Duration::ZERO, &mut guard.buf)
                    .await
                {
                    Ok(Some(n)) => n,
                    Ok(None) => break,
                    Err(e) if e.is_fatal() => return Err(e),
                    Err(e) => {
                        tracing::warn!(error = %e, "tolerating non-fatal recv during quorum gather");
                        break;
                    }
                };
                // A bad frame from one peer must NOT fail this propose: its entry
                // is already appended and may still commit, so a spurious error
                // here would invite a duplicate submission (the pre-commit
                // sibling of the G5 post-commit drain). Skip it and keep gathering.
                if let Err(e) = guard
                    .node
                    .gather_dispatch_frame(&guard.buf[..n], last_index, &mut accepted)
                    .await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    tracing::warn!(error = %e, "dropping frame after non-fatal error during quorum gather");
                }
                if guard
                    .node
                    .propose_commit_reached(needed, accepted, last_index)
                    || guard.node.scratch_pending.is_empty()
                    // B11: a flood of immediately-available non-quorum frames
                    // must not keep this burst loop spinning past the deadline.
                    || Instant::now() >= deadline
                {
                    break;
                }
            }
            if guard
                .node
                .propose_commit_reached(needed, accepted, last_index)
                || guard.node.scratch_pending.is_empty()
            {
                break;
            }

            // B11: bound the blocking wait by the remaining budget, not a
            // fresh full `timeout` per recv. On expiry we fall out with the
            // acks gathered so far; the caller sees commit not reached and
            // surfaces `NoQuorum` — never a hang, never a false commit.
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            // Blocking wait: yield only when the queue is actually empty.
            let n = match guard
                .node
                .transport
                .recv_frame_timeout(remaining, &mut guard.buf)
                .await
            {
                Ok(Some(n)) => n,
                Ok(None) => break,
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "tolerating non-fatal recv during quorum gather");
                    break;
                }
            };
            if let Err(e) = guard
                .node
                .gather_dispatch_frame(&guard.buf[..n], last_index, &mut accepted)
                .await
            {
                if e.is_fatal() {
                    return Err(e);
                }
                tracing::warn!(error = %e, "dropping frame after non-fatal error during quorum gather");
            }
        }

        Ok(accepted)
    }

    /// Decode one gathered frame and route it: advance replication for a pending
    /// peer's `AppendEntriesResp`, otherwise hand it to the normal inbound
    /// handler. A malformed frame surfaces as a (non-fatal) decode `RaftError`
    /// that the gather loop drops rather than aborting the propose.
    async fn gather_dispatch_frame(
        &mut self,
        buf: &[u8],
        last_index: LogIndex,
        accepted: &mut usize,
    ) -> Result<(), RaftError> {
        let inbound = crate::decode_message(buf)?;
        let from = inbound.from;
        let group_id = inbound.group_id;
        match inbound.message {
            RaftMessage::AppendEntriesResp(resp) => {
                self.process_append_resp(from, resp, last_index, accepted)
                    .await?;
            }
            message => {
                self.handle_inbound(InboundRaftMessage { from, group_id, message })
                    .await?;
            }
        }
        Ok(())
    }
}
