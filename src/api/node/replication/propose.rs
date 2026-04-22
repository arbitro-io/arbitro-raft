use std::time::Duration;

use super::super::progress::{AppendAdvance, AppendAttemptState};
use super::super::RaftNode;
use crate::{
    protocol::codec::{encode_message_to_bytes, encode_message_vectored},
    AppendEntries, AppendEntriesResp, EntryPayload, InboundRaftMessage, LogEntry, LogIndex, PeerId,
    RaftError, RaftMessage,
};

/// Decide whether the fan-out should use `writev` (vectored) or a single
/// contiguous `Bytes` frame.
///
/// Thresholds derived from `encode_tcp_bench`:
/// - Vectored wins once each entry is ≥ 1 memory page (4 KiB) — the kernel's
///   `copy_from_iter` has page-aligned fast paths and the userspace memcpy
///   that contiguous pays starts thrashing cache.
/// - Below 64 KiB total the iovec-overhead of writev eats the win.
/// - `IOV_MAX` on Linux is 1024 — we stay well below that to leave margin
///   for kernels that cap lower under pressure.
///
/// Contiguous still wins for small-entry batches (commands, heartbeats):
/// one memcpy + O(1) `Bytes::clone()` per peer beats many small iovecs.
/// Tunable knobs for the encoding path decision. Resolved once from env
/// on first access and cached — zero overhead on the hot path after that.
///
/// | Variable                          | Default       | Role                                                  |
/// |-----------------------------------|---------------|-------------------------------------------------------|
/// | `ARBITRO_RAFT_FORCE_CONTIGUOUS`   | unset         | If set (any value), always pick contiguous encode.    |
/// | `ARBITRO_RAFT_FORCE_VECTORED`     | unset         | If set (any value), always pick vectored encode.      |
/// | `ARBITRO_RAFT_VEC_IOV_MAX`        | 4096          | Upper cap on iovec count before falling back to cont. |
/// | `ARBITRO_RAFT_VEC_MIN_ENTRY`     | 4096  (1 pg)   | Minimum per-entry payload size for vectored to win.   |
/// | `ARBITRO_RAFT_VEC_MIN_TOTAL`      | 65536 (64 KiB)| Minimum total payload bytes to amortize writev cost.  |
///
/// `FORCE_*` takes precedence over the size heuristic. Setting both force
/// vars simultaneously is undefined — contiguous wins (first check).
struct VecTuning {
    force: Option<bool>,
    iov_max: usize,
    min_entry: usize,
    min_total: usize,
}

fn parse_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

fn vec_tuning() -> &'static VecTuning {
    use std::sync::OnceLock;
    static CACHE: OnceLock<VecTuning> = OnceLock::new();
    CACHE.get_or_init(|| {
        let force = if std::env::var_os("ARBITRO_RAFT_FORCE_CONTIGUOUS").is_some() {
            Some(false)
        } else if std::env::var_os("ARBITRO_RAFT_FORCE_VECTORED").is_some() {
            Some(true)
        } else {
            None
        };
        VecTuning {
            force,
            iov_max:   parse_usize("ARBITRO_RAFT_VEC_IOV_MAX",   4096),
            min_entry: parse_usize("ARBITRO_RAFT_VEC_MIN_ENTRY", 4096),
            min_total: parse_usize("ARBITRO_RAFT_VEC_MIN_TOTAL", 64 * 1024),
        }
    })
}

#[inline]
fn should_use_vectored(entries: &[LogEntry<'_>]) -> bool {
    let t = vec_tuning();
    if let Some(forced) = t.force {
        return forced;
    }

    // Per-entry: entry header + payload = 2 iovecs. Plus frame header + AE body + slack.
    let n_iovs = entries.len() * 2 + 4;
    if n_iovs > t.iov_max {
        return false;
    }
    let mut total = 0usize;
    for e in entries {
        let len = e.payload.0.len();
        if len < t.min_entry {
            // Any sub-threshold entry flips us back to contiguous — the iovec overhead
            // of that single small slice is enough to erase the win on the others.
            return false;
        }
        total += len;
    }
    total >= t.min_total
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Append `payloads` to local log, populating `scratch_entries` and `scratch_indexes`.
    /// Returns the last appended `LogIndex`.
    fn append_propose_entries(&mut self, payloads: &[&[u8]]) -> Result<LogIndex, RaftError> {
        let last_log_index = self.cached_last_log.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            let entry = LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload),
            };
            // Safety: scratch_entries in the struct is Vec<LogEntry<'static>>.
            // Transmute to 'static for preallocated storage. Safe because we clear it after use.
            let entry_static =
                unsafe { std::mem::transmute::<LogEntry<'_>, LogEntry<'static>>(entry) };
            self.scratch_entries.push(entry_static);
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        let last_index = *self.scratch_indexes.last().unwrap();

        // Safety: ensure storage sees the entries with a valid ephemeral lifetime
        let entries_ref = unsafe {
            std::mem::transmute::<&[LogEntry<'static>], &[LogEntry<'_>]>(&self.scratch_entries)
        };
        self.storage.append_entries(entries_ref)?;

        self.cached_last_log = (last_index, self.hard_state.current_term);
        Ok(last_index)
    }

    /// Collect remote peers into `scratch_peers`, send one `AppendEntries` attempt to each,
    /// and populate `scratch_pending` with peers that received the frame.
    /// Collect remote peers into `scratch_peers`, send one `AppendEntries` attempt to each
    /// in parallel (fan-out), and populate `scratch_pending` with peers that received the frame.
    async fn send_initial_appends(&mut self) -> Result<(), RaftError> {
        self.scratch_peers.clear();
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.scratch_peers.push(peer);
        }

        if self.scratch_peers.is_empty() {
            return Ok(());
        }

        // 1. Prepare the message once
        let (last_log_index, _) = self.storage.last_log_position()?;
        // Entries were already appended to local log and are in scratch_entries
        let entries_ref = unsafe {
            std::mem::transmute::<&[LogEntry<'static>], &[LogEntry<'_>]>(
                self.scratch_entries.as_slice(),
            )
        };

        let prev_log_index = LogIndex(last_log_index.0 - entries_ref.len() as u64);
        let prev_log_term = self.term_at(prev_log_index)?;

        let req = AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_log_index.0.into(),
            prev_log_term: prev_log_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (entries_ref.len() as u32).into(),
            _pad: 0.into(),
        };
        let msg = RaftMessage::AppendEntriesVectored(&req, entries_ref);

        let sent_last_index = last_log_index;
        self.scratch_pending.clear();

        // Hybrid encoding strategy. See `should_use_vectored` — sub-page entries
        // or small total bytes take the contiguous path (1 memcpy + O(1) Bytes
        // clones to N peers). Large page-aligned batches take the vectored path
        // (zero-copy payload slices, one writev per peer).
        if should_use_vectored(entries_ref) {
            // --- Vectored fan-out ---
            self.scratch_vectored.clear();
            // SAFETY: scratch_vectored is stored as Vec<(*const u8, usize)> but
            // the encoder and transport treat it as Vec<&[u8]>. Same pattern as
            // `send_message`. Cleared before we return, so no borrow escapes.
            let iovs: &mut Vec<&[u8]> = unsafe {
                std::mem::transmute::<&mut Vec<(*const u8, usize)>, &mut Vec<&[u8]>>(
                    &mut self.scratch_vectored,
                )
            };
            encode_message_vectored(
                self.config.node_id,
                &msg,
                &mut self.scratch_outbound,
                iovs,
            )?;

            // Shared slice view — all peers send the exact same bytes.
            let slices: &[&[u8]] = iovs.as_slice();
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                sends.push(async move { (peer, transport.send_vectored(peer, slices).await) });
            }

            let results = futures::future::join_all(sends).await;
            for (peer, res) in results {
                if res.is_ok() {
                    self.scratch_pending.insert(
                        peer,
                        AppendAttemptState {
                            attempts: 1,
                            sent_last_index,
                        },
                    );
                }
            }

            // Release the 'static transmute borrow before we return.
            self.scratch_vectored.clear();
        } else {
            // --- Contiguous fan-out ---
            let frame = encode_message_to_bytes(self.config.node_id, &msg)?;
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for i in 0..self.scratch_peers.len() {
                let peer = self.scratch_peers[i];
                let f = frame.clone(); // O(1) refcount clone
                sends.push(async move { (peer, transport.send_frame_owned(peer, f).await) });
            }

            let results = futures::future::join_all(sends).await;
            for (peer, res) in results {
                if res.is_ok() {
                    self.scratch_pending.insert(
                        peer,
                        AppendAttemptState {
                            attempts: 1,
                            sent_last_index,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    /// Route an `AppendEntriesResp` during quorum wait: advance replication state for pending
    /// peers or forward to the normal response handler for peers not in `scratch_pending`.
    async fn process_append_resp(
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
    async fn gather_quorum_acks(
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
                            .handle_inbound(InboundRaftMessage { from, message })
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
                            .handle_inbound(InboundRaftMessage { from, message })
                            .await?;
                    }
                }
            } else {
                break;
            }
        }

        Ok(accepted)
    }

    pub async fn propose_once(&mut self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        let scratch = [payload];
        let indexes = self.propose_batch_once(&scratch).await?;
        Ok(*indexes
            .first()
            .ok_or(RaftError::Protocol("propose failed to return index".into()))?)
    }

    pub async fn propose_batch_once(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<&[LogIndex], RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok(&[]);
        }
        // Progress must be initialized BEFORE append so that next_index covers
        // the entries we are about to write.
        self.ensure_leader_progress_initialized()?;
        let last_index = self.append_propose_entries(payloads)?;
        self.send_initial_appends().await?;

        let needed = super::super::quorum(self.config.peers.len());
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2);
        let accepted = self.gather_quorum_acks(needed, last_index, timeout).await?;

        if accepted < needed {
            return Err(RaftError::NoQuorum);
        }
        self.soft_state.commit_index = last_index;
        self.drain_inbound_ready().await?;
        Ok(&self.scratch_indexes)
    }

    /// Fire-and-forget batch replication.
    pub async fn replicate_batch_async(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<(LogIndex, usize), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok((LogIndex(0), 0));
        }
        self.ensure_leader_progress_initialized()?;

        let (prev_log_index, prev_log_term) = self.cached_last_log;
        let first_index = LogIndex(prev_log_index.0 + 1);

        self.scratch_entries.clear();
        let mut next_raw = first_index.0;
        for payload in payloads {
            let entry = LogEntry {
                term: self.hard_state.current_term,
                index: LogIndex(next_raw),
                payload: EntryPayload(payload),
            };
            // Safety: transmute to 'static for scratchpad storage.
            let entry_static =
                unsafe { std::mem::transmute::<LogEntry<'_>, LogEntry<'static>>(entry) };
            self.scratch_entries.push(entry_static);
            next_raw += 1;
        }

        // Safety: storage call
        let entries_ref = unsafe {
            std::mem::transmute::<&[LogEntry<'static>], &[LogEntry<'_>]>(&self.scratch_entries)
        };
        self.storage.append_entries(entries_ref)?;

        if let Some(last) = self.scratch_entries.last() {
            self.cached_last_log = (last.index, last.term);
        }

        // --- Parallel Fan-out Encoding ---
        let req = AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_log_index.0.into(),
            prev_log_term: prev_log_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (entries_ref.len() as u32).into(),
            _pad: 0.into(),
        };
        let msg = RaftMessage::AppendEntriesVectored(&req, entries_ref);

        // Collect peers once so the hot branches below don't repeat the filter.
        self.scratch_peers.clear();
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.scratch_peers.push(peer);
        }

        if should_use_vectored(entries_ref) {
            // --- Vectored fan-out (bulk replication path) ---
            self.scratch_vectored.clear();
            // SAFETY: see `send_initial_appends` — scratch_vectored is cleared
            // before returning, no borrow escapes self.
            let iovs: &mut Vec<&[u8]> = unsafe {
                std::mem::transmute::<&mut Vec<(*const u8, usize)>, &mut Vec<&[u8]>>(
                    &mut self.scratch_vectored,
                )
            };
            encode_message_vectored(
                self.config.node_id,
                &msg,
                &mut self.scratch_outbound,
                iovs,
            )?;

            let slices: &[&[u8]] = iovs.as_slice();
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                sends.push(async move { transport.send_vectored(peer, slices).await });
            }

            let results = futures::future::join_all(sends).await;
            for res in results {
                if let Err(e) = res {
                    tracing::error!(error = %e, "parallel fan-out send failed");
                }
            }

            self.scratch_vectored.clear();
        } else {
            // --- Contiguous fan-out (control-plane / small-entry path) ---
            let frame = encode_message_to_bytes(self.config.node_id, &msg)?;
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                let f = frame.clone();
                sends.push(async move { transport.send_frame_owned(peer, f).await });
            }

            let results = futures::future::join_all(sends).await;
            for res in results {
                if let Err(e) = res {
                    tracing::error!(error = %e, "parallel fan-out send failed");
                }
            }
        }

        self.scratch_entries.clear();

        Ok((first_index, payloads.len()))
    }
}
