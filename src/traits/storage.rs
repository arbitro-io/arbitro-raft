use crate::protocol::codec::wire::EntryHeader;
use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

/// Persistent state backing a Raft node.
///
/// # Durability contract (P1-1)
///
/// Raft's safety proof assumes the *stable storage* below is truly durable: a
/// value is on disk **before** the write call returns. Two writes are on the
/// critical safety path:
///
/// * [`save_hard_state`](RaftStorage::save_hard_state) — persists
///   `current_term` and `voted_for`. It MUST be durable (fsync'd, or an
///   equivalent barrier) before returning `Ok`. The node calls it before
///   granting a vote or advancing its term; if it returned `Ok` while the value
///   was still only in the page cache, a crash + restart could resurrect a node
///   that "forgot" it already voted in a term — a **double vote → two leaders
///   in one term → committed-entry divergence**.
/// * [`append_entries`](RaftStorage::append_entries) — a leader that counts a
///   follower's ack toward a commit quorum assumes the follower has the entry
///   durably. An implementation that acks before the entry is durable narrows
///   the crash window in which a committed entry can be lost.
///
/// A purely in-memory implementation trivially satisfies "durable before
/// return" *within a process* but loses everything on restart; use it only for
/// tests or caches, never where crash-recovery matters. Implementations are
/// free to batch/group-commit fsyncs across calls as long as no call returns
/// `Ok` before its own value is durable.
///
/// # Integrity contract (I1 / API2)
///
/// The engine trusts every byte a storage returns: a read that silently hands
/// back corrupted data (bit rot, a torn write surfaced as a clean read, a
/// truncated file) does not fail Raft — it *diverges* it, because the corrupt
/// entry is replicated, committed, and applied as if it were the real one.
/// Storages therefore SHOULD checksum what they persist (per-entry / per-frame
/// CRCs for the log and WAL, a whole-payload checksum for snapshots) and
/// **detect corruption on read, surfacing it as an `Err`** — never returning
/// bytes that fail verification. The engine classifies storage read errors on
/// its critical paths as fatal and halts the node, which is the correct
/// outcome: a loud crash is recoverable (restore from snapshot / peers),
/// silent divergence is not. An in-memory test storage may skip checksums;
/// anything that touches a disk should not.
///
/// # Buffer and truncation semantics
///
/// * [`entry_at`](RaftStorage::entry_at) / [`read_entries`](RaftStorage::read_entries)
///   receive a caller-owned `payload_buf`. An implementation is free to return
///   an error when the buffer is smaller than the payload it needs to write —
///   it MUST NOT silently truncate the payload. Callers that only need an
///   entry's term must use [`term_at`](RaftStorage::term_at) (B10), never an
///   undersized `entry_at` read.
/// * [`truncate_suffix`](RaftStorage::truncate_suffix)`(from)` deletes every
///   entry with `index >= from` — `from` itself is removed (inclusive).
/// * [`truncate_before`](RaftStorage::truncate_before)`(up_to)` deletes every
///   entry with `index < up_to` — `up_to` itself is retained (exclusive).
pub trait RaftStorage: Send + Sync + 'static {
    /// Load the durable `HardState` (term + vote). Called once in
    /// `RaftNode::new`; returning stale state here is the restart-side of the
    /// double-vote hazard described in the trait docs.
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    /// Persist `HardState` durably before returning `Ok` — see the trait-level
    /// durability contract.
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    /// Append entries to the log durably before returning `Ok` — see the
    /// trait-level durability contract.
    fn append_entries(&self, entries: &[LogEntry<'_>]) -> Result<(), RaftError>;

    /// OPTIMIZED PATH: Appends entries from a pre-serialized header block and payload refs.
    /// Default implementation returns an error as it's optional for high-speed storages.
    fn append_entries_seeded(
        &self,
        _headers: &[EntryHeader],
        _payloads: &[&[u8]],
    ) -> Result<(), RaftError> {
        Err(RaftError::Storage("Seeded append not implemented".into()))
    }

    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError>;

    /// MAGIC ZEROCOPY PATH: Returns a contiguous slice of entry headers as bytes in O(1).
    /// Used by the leader to perform zero-allocation replication.
    fn read_entry_headers(
        &self,
        _from: LogIndex,
        _to: LogIndex,
    ) -> Result<Option<&[EntryHeader]>, RaftError> {
        Ok(None)
    }

    /// OPTIMIZED PATH: Iterates over payloads to avoid LogEntry construction.
    ///
    /// The visited slices are tied to `&'a self`: implementations MUST yield
    /// views into storage-owned memory that stay valid for as long as the
    /// storage borrow lives — the engine collects them and hands them to
    /// vectored I/O after the visit completes (the zerocopy replication
    /// path, paired with [`read_entry_headers`](Self::read_entry_headers),
    /// which carries the same borrow contract). An implementation that can
    /// only produce transient/per-call buffers must leave this unimplemented
    /// so the engine falls back to [`read_entries`](Self::read_entries).
    fn for_each_payload<'a>(
        &'a self,
        _from: LogIndex,
        _to: LogIndex,
        _f: &mut dyn FnMut(&'a [u8]),
    ) -> Result<(), RaftError> {
        Err(RaftError::Storage(
            "for_each_payload not implemented".into(),
        ))
    }

    /// Retrieves a specific entry from the log by its index.
    /// Uses `payload_buf` to store the payload data.
    ///
    /// Returns `Ok(None)` when the entry is not in the log (never written, or
    /// compacted away below the snapshot boundary). If `payload_buf` is
    /// smaller than the entry's payload the implementation should return an
    /// error — it MUST NOT silently truncate (see the trait-level buffer
    /// contract). Term-only callers must use [`term_at`](Self::term_at).
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError>;

    /// Term of the log entry at `index`, or `Ok(None)` if the entry is not in
    /// the log (never written, or compacted away below the snapshot
    /// boundary).
    ///
    /// This is the term-only probe the engine uses for §5.3 consistency
    /// checks — prev-entry term, conflict scans, snapshot-boundary checks —
    /// where the payload is irrelevant (B10/PS4). Storages that can answer
    /// terms without touching payload bytes (a metadata arena, a separate
    /// header index) should override this with that O(1) lookup.
    ///
    /// # Default-impl caveat
    ///
    /// The default routes through [`entry_at`](Self::entry_at) with a small
    /// (512-byte) stack buffer, so it inherits `entry_at`'s buffer contract:
    /// an implementation that errors when `payload_buf` is smaller than the
    /// entry's payload makes the default fail for entries whose payload
    /// exceeds 512 bytes. Such strict-buffer implementations MUST override
    /// `term_at` (the override is trivial — return the stored term).
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, RaftError> {
        let mut buf = [0u8; 512];
        Ok(self.entry_at(index, &mut buf)?.map(|e| e.term))
    }

    /// Returns the highest index in the log, and its corresponding term.
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError>;

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError>;

    /// Delete every log entry with index STRICTLY LESS THAN `up_to`.
    ///
    /// Called after a snapshot has been saved via `save_snapshot`. The default
    /// implementation is a no-op — storages that want log compaction must
    /// override it. It is NEVER an error to leave entries in place; the log
    /// simply grows unbounded (existing behavior).
    fn truncate_before(&self, _up_to: LogIndex) -> Result<(), RaftError> {
        Ok(())
    }

    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError>;
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError>;

    /// Cheap metadata-only probe of the current on-disk snapshot.
    ///
    /// Returns the [`SnapshotMeta`] that [`load_snapshot`](Self::load_snapshot)
    /// would return, WITHOUT the payload bytes. The engine calls this on
    /// every apply batch to decide whether a state-machine restore is
    /// warranted at all (C5/P4), so it sits on a hot path: **override it for
    /// O(1)** — the default derives the meta by loading the FULL snapshot and
    /// dropping the bytes, which is correct but defeats the purpose of the
    /// probe once snapshots are large.
    // C5: the server's `FileRaftStorage` should override this cheaply — it
    // already keeps the current snapshot meta in RAM (C6); tracked as a
    // server-side task. The default here keeps the trait addition
    // non-breaking for existing implementations.
    fn load_snapshot_meta(&self) -> Result<Option<SnapshotMeta>, RaftError> {
        Ok(self.load_snapshot()?.map(|(meta, _bytes)| meta))
    }
}
