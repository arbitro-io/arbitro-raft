use crate::api::node::progress::AppendAdvance;
use crate::RaftNode;
use crate::{AppendEntriesResp, InboundRaftMessage, LogIndex, PeerId, RaftError, RaftMessage};
use std::time::Duration;

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
                *accepted += 1;
            }
        } else {
            self.handle_append_entries_response(from, resp).await?;
        }
        Ok(())
    }

    /// Block until a quorum has acknowledged `last_index` or `timeout` expires without progress.
    /// Returns the number of accepted acknowledgements (leader self-ack = 1 on entry).
    pub(super) async fn gather_quorum_acks(
        &mut self,
        needed: usize,
        last_index: LogIndex,
        timeout: Duration,
    ) -> Result<usize, RaftError> {
        let mut accepted = 1usize;

        // Use pre-allocated quorum buffer
        self.scratch_quorum_buf.clear();
        if self.scratch_quorum_buf.capacity() < 64 * 1024 {
            self.scratch_quorum_buf.reserve(64 * 1024);
        }
        unsafe { self.scratch_quorum_buf.set_len(64 * 1024) };

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

        while accepted < needed && !guard.node.scratch_pending.is_empty() {
            // Burst-drain: consume all immediately-available frames before yielding.
            while let Some(n) = guard
                .node
                .transport
                .recv_frame_timeout(Duration::ZERO, &mut guard.buf)
                .await?
            {
                let inbound = crate::decode_message(&guard.buf[..n])?;
                let from = inbound.from;
                let group_id = inbound.group_id;
                match inbound.message {
                    RaftMessage::AppendEntriesResp(resp) => {
                        guard
                            .node
                            .process_append_resp(from, resp, last_index, &mut accepted)
                            .await?;
                    }
                    message => {
                        guard
                            .node
                            .handle_inbound(InboundRaftMessage { from, group_id, message })
                            .await?;
                    }
                }
                if accepted >= needed || guard.node.scratch_pending.is_empty() {
                    break;
                }
            }
            if accepted >= needed || guard.node.scratch_pending.is_empty() {
                break;
            }

            // Blocking wait: yield only when the queue is actually empty.
            if let Some(n) = guard
                .node
                .transport
                .recv_frame_timeout(timeout, &mut guard.buf)
                .await?
            {
                let inbound = crate::decode_message(&guard.buf[..n])?;
                let from = inbound.from;
                let group_id = inbound.group_id;
                match inbound.message {
                    RaftMessage::AppendEntriesResp(resp) => {
                        guard
                            .node
                            .process_append_resp(from, resp, last_index, &mut accepted)
                            .await?;
                    }
                    message => {
                        guard
                            .node
                            .handle_inbound(InboundRaftMessage { from, group_id, message })
                            .await?;
                    }
                }
            } else {
                break;
            }
        }

        Ok(accepted)
    }
}
