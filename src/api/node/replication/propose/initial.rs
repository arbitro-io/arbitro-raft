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
    pub(super) fn append_propose_entries(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<LogIndex, RaftError> {
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
        for entry in entries_ref {
            self.log_metadata.append(entry.index, entry.term);
        }

        self.cached_last_log = (last_index, self.hard_state.current_term);
        Ok(last_index)
    }

    pub(super) async fn send_initial_appends(&mut self) -> Result<(), RaftError> {
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

        let req = crate::protocol::AppendEntries {
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
            encode_message_vectored(self.config.node_id, &msg, &mut self.scratch_outbound, iovs)?;

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
