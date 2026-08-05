//! C1 — storage fault-injection harness (test-only, reusable).
//!
//! `FaultStorage` wraps the standard in-memory test storage (the corrected
//! `split_at_mut` buffer pattern from the B1 restructure — no aliasing, Miri
//! clean) and injects the fault classes the C-D1 durability contract must be
//! proven against:
//!
//! * **fail-nth** — the N-th call to a given [`FaultOp`] returns
//!   `Err(RaftError::Storage(..))` (one-shot; per-op call counters).
//! * **torn write** — the next append persists only a prefix of the batch,
//!   then reports `Ok` or `Err` per [`TornReport`] (one-shot).
//! * **short read** — `read_entries` silently returns only the first
//!   `max_entries` of the requested range (sticky until cleared).
//!
//! This is the seam C9 (ack-after-persist) and the F5 DST harness build on.
//! Include from an integration test with:
//! `#[path = "support/fault_storage.rs"] mod fault_storage;`

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    BootstrapPeer, ClusterId, EntryHeader, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta, Term,
    TimingConfig,
};

// ---------------------------------------------------------------------------
// Fault plan
// ---------------------------------------------------------------------------

/// Storage operations that can be fault-injected.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum FaultOp {
    SaveHardState,
    /// Covers BOTH append arms (`append_entries` and `append_entries_seeded`)
    /// — they share one counter so a fault plan hits whichever arm the engine
    /// takes, mirroring the B12 parity contract.
    AppendEntries,
    ReadEntries,
    EntryAt,
    LastLogPosition,
}

/// Sticky fault class for BOTH append arms (C8): every append fails with
/// this error until cleared via `set_sticky_append_fault(None)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StickyAppendFault {
    /// ENOSPC-class resource exhaustion: `RaftError::Io(StorageFull)` — the
    /// signal the C8 degradation path classifies as `ErrorClass::Resource`.
    /// Nothing is persisted while active; clearing models freed disk space.
    DiskFull,
    /// Corruption: `RaftError::CorruptLog` — must stay Fatal (C8 must never
    /// mask corruption).
    Corrupt,
}

/// What a torn append reports to the caller after persisting only a prefix.
#[derive(Clone, Copy, Debug)]
pub enum TornReport {
    /// The storage LIES: prefix persisted, full batch acknowledged. Models a
    /// disk/page-cache that violates the durability contract.
    Ok,
    /// The storage tells the truth: prefix persisted, error surfaced. Models
    /// a power-cut mid-batch with honest error reporting.
    Err,
}

#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Default)]
struct FaultState {
    /// Per-op call counters (every call counts, faulted or not).
    calls: HashMap<FaultOp, u64>,
    /// Per-op 1-based call number that fails (one-shot: consumed on trigger).
    fail_on: HashMap<FaultOp, u64>,
    /// One-shot torn-write plan for the next append.
    torn: Option<(usize, TornReport)>,
    /// Sticky short-read cap for `read_entries`.
    short_read_max: Option<usize>,
    /// Sticky append fault (C8): every append fails until cleared.
    sticky_append: Option<StickyAppendFault>,
}

#[derive(Default)]
struct Inner {
    hard_state: Mutex<Option<HardState>>,
    entries: Mutex<Vec<StoredEntry>>,
    faults: Mutex<FaultState>,
}

/// Fault-injecting in-memory [`RaftStorage`]. `Clone` shares the same durable
/// state and fault plan (so a test can keep a handle while the node owns one,
/// and a "restarted" node can be built over the same surviving bytes).
#[derive(Clone, Default)]
pub struct FaultStorage {
    inner: Arc<Inner>,
}

impl FaultStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make the `nth` (1-based) call to `op` fail with `RaftError::Storage`.
    /// One-shot: the target is consumed when it triggers.
    pub fn fail_nth(&self, op: FaultOp, nth: u64) {
        assert!(nth >= 1, "fail_nth is 1-based");
        self.inner.faults.lock().unwrap().fail_on.insert(op, nth);
    }

    /// The NEXT append persists only the first `keep` entries of its batch,
    /// then reports `Ok` or `Err` per `report`. One-shot.
    pub fn torn_append(&self, keep: usize, report: TornReport) {
        self.inner.faults.lock().unwrap().torn = Some((keep, report));
    }

    /// Every `read_entries` call returns at most the first `max_entries` of
    /// the requested range — silently, with a consistent byte count (sticky).
    pub fn short_read(&self, max_entries: usize) {
        self.inner.faults.lock().unwrap().short_read_max = Some(max_entries);
    }

    /// Sticky append fault (C8): while set, EVERY append (both arms) fails
    /// with the given class — `DiskFull` (`Io(StorageFull)`, the Resource
    /// signal) or `Corrupt` (`CorruptLog`, stays Fatal). Pass `None` to
    /// clear (models freed disk space); nothing is persisted while active.
    pub fn set_sticky_append_fault(&self, fault: Option<StickyAppendFault>) {
        self.inner.faults.lock().unwrap().sticky_append = fault;
    }

    fn sticky_append_error(&self) -> Option<RaftError> {
        match self.inner.faults.lock().unwrap().sticky_append {
            None => None,
            Some(StickyAppendFault::DiskFull) => Some(RaftError::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "injected: no space left on device (ENOSPC)",
            ))),
            Some(StickyAppendFault::Corrupt) => Some(RaftError::CorruptLog(
                "injected: log corruption on append".into(),
            )),
        }
    }

    /// Number of calls made to `op` so far (faulted calls included).
    pub fn calls(&self, op: FaultOp) -> u64 {
        *self
            .inner
            .faults
            .lock()
            .unwrap()
            .calls
            .get(&op)
            .unwrap_or(&0)
    }

    /// Pre-seed the durable hard state (as if persisted in a previous life).
    pub fn set_hard_state(&self, hs: HardState) {
        *self.inner.hard_state.lock().unwrap() = Some(hs);
    }

    /// Pre-seed durable log entries: `(term, index, payload)` triples.
    pub fn seed_entries(&self, entries: &[(u64, u64, &[u8])]) {
        let mut stored = self.inner.entries.lock().unwrap();
        for &(term, index, payload) in entries {
            stored.push(StoredEntry {
                term: Term(term),
                index: LogIndex(index),
                payload: payload.to_vec(),
            });
        }
    }

    /// The durable truth: every entry the storage actually holds.
    pub fn durable_entries(&self) -> Vec<(u64, u64, Vec<u8>)> {
        self.inner
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|e| (e.term.0, e.index.0, e.payload.clone()))
            .collect()
    }

    /// The durable hard state (None = never persisted).
    pub fn durable_hard_state(&self) -> Option<HardState> {
        self.inner.hard_state.lock().unwrap().clone()
    }

    /// Count a call to `op` and fail it if the (one-shot) plan says so.
    fn tick(&self, op: FaultOp) -> Result<(), RaftError> {
        let mut faults = self.inner.faults.lock().unwrap();
        let n = faults.calls.entry(op).or_insert(0);
        *n += 1;
        let n = *n;
        if faults.fail_on.get(&op) == Some(&n) {
            faults.fail_on.remove(&op);
            return Err(RaftError::Storage(format!(
                "injected fault: {op:?} call #{n}"
            )));
        }
        Ok(())
    }

    /// Take the one-shot torn plan, if armed.
    fn take_torn(&self) -> Option<(usize, TornReport)> {
        self.inner.faults.lock().unwrap().torn.take()
    }

    fn short_read_cap(&self) -> Option<usize> {
        self.inner.faults.lock().unwrap().short_read_max
    }
}

impl RaftStorage for FaultStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self
            .inner
            .hard_state
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default())
    }

    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        self.tick(FaultOp::SaveHardState)?;
        *self.inner.hard_state.lock().unwrap() = Some(state.clone());
        Ok(())
    }

    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        self.tick(FaultOp::AppendEntries)?;
        if let Some(err) = self.sticky_append_error() {
            return Err(err);
        }
        let torn = self.take_torn();
        let keep = torn.map(|(k, _)| k).unwrap_or(new_entries.len());
        let mut entries = self.inner.entries.lock().unwrap();
        for e in new_entries.iter().take(keep) {
            entries.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
            });
        }
        match torn {
            Some((_, TornReport::Err)) => Err(RaftError::Storage(
                "injected torn write: prefix persisted, batch failed".into(),
            )),
            _ => Ok(()),
        }
    }

    fn append_entries_seeded(
        &self,
        headers: &[EntryHeader],
        payloads: &[&[u8]],
    ) -> Result<(), RaftError> {
        self.tick(FaultOp::AppendEntries)?;
        if let Some(err) = self.sticky_append_error() {
            return Err(err);
        }
        let torn = self.take_torn();
        let keep = torn.map(|(k, _)| k).unwrap_or(headers.len());
        let mut entries = self.inner.entries.lock().unwrap();
        for (h, p) in headers.iter().zip(payloads.iter()).take(keep) {
            entries.push(StoredEntry {
                term: Term(h.term.get()),
                index: LogIndex(h.index.get()),
                payload: p.to_vec(),
            });
        }
        match torn {
            Some((_, TornReport::Err)) => Err(RaftError::Storage(
                "injected torn write: prefix persisted, batch failed".into(),
            )),
            _ => Ok(()),
        }
    }

    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        self.tick(FaultOp::ReadEntries)?;
        let cap = self.short_read_cap().unwrap_or(usize::MAX);
        let entries = self.inner.entries.lock().unwrap();
        let mut buf = payload_buf;
        let mut written = 0;
        let mut pushed = 0usize;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                if pushed >= cap {
                    // Short read: silently stop after `cap` entries — the
                    // returned prefix is still internally consistent.
                    break;
                }
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                // Split the front chunk off the remaining buffer so the shared
                // ref pushed into `out` is never invalidated by a later write
                // through `buf` — no transmute, Stacked Borrows (Miri) clean.
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                buf = rest;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                written += len;
                pushed += 1;
            }
        }
        Ok(written)
    }

    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        self.tick(FaultOp::EntryAt)?;
        let entries = self.inner.entries.lock().unwrap();
        if let Some(e) = entries.iter().rev().find(|e| e.index == index) {
            if payload_buf.len() < e.payload.len() {
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[..e.payload.len()].copy_from_slice(&e.payload);
            let payload: &'a [u8] = &payload_buf[..e.payload.len()];
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(payload),
            }))
        } else {
            Ok(None)
        }
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        self.tick(FaultOp::LastLogPosition)?;
        Ok(self
            .inner
            .entries
            .lock()
            .unwrap()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.inner
            .entries
            .lock()
            .unwrap()
            .retain(|e| e.index < from);
        Ok(())
    }

    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Capture transport — records every outbound frame; inbound is silent.
// ---------------------------------------------------------------------------

/// Transport that records every outbound frame (vectored writes concatenated
/// exactly as a stream socket would see them) and never delivers inbound
/// frames (`recv_frame_timeout` reports "nothing arrived"). Lets a test
/// assert exactly which acks/grants/appends a node put on the wire.
#[derive(Clone, Default)]
pub struct CaptureTransport {
    sent: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl CaptureTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every frame sent so far, as `(peer, frame_bytes)`.
    pub fn sent(&self) -> Vec<(u64, Vec<u8>)> {
        self.sent.lock().unwrap().clone()
    }

    /// Frames sent to one peer.
    pub fn sent_to(&self, peer: u64) -> Vec<Vec<u8>> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| *p == peer)
            .map(|(_, f)| f.clone())
            .collect()
    }

    /// Forget everything captured so far (e.g. after a setup phase).
    pub fn clear(&self) {
        self.sent.lock().unwrap().clear();
    }
}

impl RaftTransport for CaptureTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.sent.lock().unwrap().push((peer.0, frame));
        async move { Ok(()) }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.sent.lock().unwrap().push((peer.0, frame.to_vec()));
        async move { Ok(()) }
    }

    fn recv_frame(
        &self,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move {
            Err(RaftError::Transport(
                "no inbound in capture transport".into(),
            ))
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move {
            // Honor the timeout so gather loops age out instead of spinning.
            if !timeout.is_zero() {
                tokio::time::sleep(timeout).await;
            }
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Standard 3-node-style test config (mirrors the other integration suites).
pub fn make_config(node_id: u64, peers: &[u64]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9300 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_max_ms: 300,
        },
        limits: LimitsConfig::default(),
    }
}

/// One 24-byte wire `EntryHeader` (term, index, payload_len, pad) as bytes.
pub fn entry_header_bytes(term: u64, index: u64, payload_len: u32) -> Vec<u8> {
    let mut h = Vec::with_capacity(24);
    h.extend_from_slice(&term.to_le_bytes());
    h.extend_from_slice(&index.to_le_bytes());
    h.extend_from_slice(&payload_len.to_le_bytes());
    h.extend_from_slice(&0u32.to_le_bytes());
    h
}

/// Contiguous AppendEntries entry block: `[header, payload]` per entry.
pub fn contiguous_entry_block(entries: &[(u64, u64, &[u8])]) -> Vec<u8> {
    let mut block = Vec::new();
    for &(term, index, payload) in entries {
        block.extend_from_slice(&entry_header_bytes(term, index, payload.len() as u32));
        block.extend_from_slice(payload);
    }
    block
}
