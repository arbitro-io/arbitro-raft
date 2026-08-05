use crate::api::node::progress::AppendAttemptState;
use crate::RaftNode;
use crate::{
    protocol::codec::{encode_message_to_bytes, encode_message_vectored},
    EntryPayload, LogEntry, LogIndex, RaftError, RaftMessage,
};

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
            iov_max: parse_usize("ARBITRO_RAFT_VEC_IOV_MAX", 4096),
            min_entry: parse_usize("ARBITRO_RAFT_VEC_MIN_ENTRY", 4096),
            min_total: parse_usize("ARBITRO_RAFT_VEC_MIN_TOTAL", 64 * 1024),
        }
    })
}

#[inline]
pub(super) fn should_use_vectored(entries: &[LogEntry<'_>]) -> bool {
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
    /// Build the propose batch into `entries` (a dock-recycled local vec
    /// owned by the caller — see `propose_batch_once`) and append it to
    /// storage. The entries borrow `payloads` directly; the borrow checker
    /// verifies the lifetime, so no 'static laundering is needed (US3).
    pub(super) fn append_propose_entries<'a>(
        &mut self,
        payloads: &[&'a [u8]],
        entries: &mut Vec<LogEntry<'a>>,
    ) -> Result<LogIndex, RaftError> {
        let last_log_index = self.cached_last_log.0;
        self.scratch_indexes.clear();
        // B8 arithmetic policy: saturate at index boundaries (never wrap), use
        // checked math where an out-of-range value means broken protocol state.
        let mut next_raw = last_log_index.0.saturating_add(1);
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            entries.push(LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        // B7: an empty batch must not panic on `.last()` — reject it as an
        // invalid payload. (`propose_batch_once` guards this today, but this
        // path must stay panic-free on its own.)
        let Some(&last_index) = self.scratch_indexes.last() else {
            return Err(RaftError::InvalidPayload("empty payload batch"));
        };

        self.storage.append_entries(entries)?;
        for entry in entries.iter() {
            self.log_metadata.append(entry.index, entry.term);
        }

        self.cached_last_log = (last_index, self.hard_state.current_term);
        Ok(last_index)
    }

    /// Fan the freshly-appended batch out to every peer. `entries` is the
    /// same caller-owned vec `append_propose_entries` filled.
    pub(super) async fn send_initial_appends(
        &mut self,
        entries: &[LogEntry<'_>],
    ) -> Result<(), RaftError> {
        // A13: fan out to voters AND learners — learners replicate like
        // followers; their acks are excluded on the counting side
        // (`process_append_resp` / `try_advance_commit_index`).
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

        if self.scratch_peers.is_empty() {
            return Ok(());
        }

        // 1. Prepare the message once
        let (last_log_index, _) = self.storage.last_log_position()?;
        // Entries were already appended to the local log by
        // `append_propose_entries` and arrive here as a caller-owned slice.
        let entries_ref = entries;

        // B8 / P4: checked, not unchecked — `last_log_position()` reporting a
        // tip smaller than the batch we just appended means the storage's log
        // accounting is broken. Wrapping here would fabricate a near-u64::MAX
        // prev_log_index on the wire; surface it as log corruption instead.
        let prev_log_index = LogIndex(
            last_log_index
                .0
                .checked_sub(entries_ref.len() as u64)
                .ok_or_else(|| {
                    RaftError::CorruptLog(
                        "last_log_position smaller than just-appended batch \
                         (prev_log_index underflow)"
                            .into(),
                    )
                })?,
        );
        let prev_log_term = self.term_at(prev_log_index)?;

        let req = crate::protocol::AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_log_index.0.into(),
            prev_log_term: prev_log_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (entries_ref.len() as u32).into(),
            // A11: ReadIndex probe token (echoed by the follower).
            _pad: self.read_probe_seq.into(),
        };
        let msg = RaftMessage::AppendEntriesVectored(&req, entries_ref);

        let sent_last_index = last_log_index;
        self.scratch_pending.clear();

        if should_use_vectored(entries_ref) {
            // --- Vectored fan-out ---
            // Dock-recycled LOCAL iovec list: every slice it holds borrows
            // either `scratch_outbound` (headers) or the caller's payloads —
            // lifetimes the compiler verifies (US3/US5).
            let mut iovs = self.scratch_vectored.take();
            if let Err(e) = encode_message_vectored(
                self.config.node_id,
                &msg,
                &mut self.scratch_outbound,
                &mut iovs,
            ) {
                self.scratch_vectored.put(iovs);
                return Err(e);
            }

            // Shared slice view — all peers send the exact same bytes.
            let slices: &[&[u8]] = iovs.as_slice();
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                sends.push(async move { (peer, transport.send_vectored(peer, slices).await) });
            }

            let results = futures::future::join_all(sends).await;
            for (peer, res) in results {
                if let Ok(()) = res {
                    self.scratch_pending.insert(
                        peer,
                        AppendAttemptState {
                            attempts: 1,
                            sent_last_index,
                        },
                    );
                }
            }

            self.scratch_vectored.put(iovs);
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
                if let Ok(()) = res {
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
}
