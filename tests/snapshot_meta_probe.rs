//! C5 / P4 — the apply path must probe snapshot METADATA, never the payload.
//!
//! Before C5, `apply_committed_entries` → `restore_state_machine_from_snapshot`
//! called `storage.load_snapshot()` (the FULL snapshot bytes) on every apply
//! batch, before the cheap `last_included_index <= last_applied` bail could
//! run — so once any snapshot existed, every apply batch paid O(snapshot)
//! I/O. With C5 the bail check uses the new `load_snapshot_meta()` probe and
//! the full payload is only read when a restore is actually warranted.
//!
//! The harness mirrors `tests/log_compaction.rs`: a manually-driven leader
//! over an auto-acking transport, with a storage that counts full snapshot
//! loads separately from meta probes.
//!
//! Also covers the I1 trait-default contracts: `load_snapshot_meta()` derives
//! from `load_snapshot()`, and `term_at()` derives from `entry_at()` (B10).

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    decode_message, encode_message_to_bytes, AppendEntriesResp, ArbitroRaft, BootstrapPeer,
    ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId,
    RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport, SnapshotMeta, StateMachine, Term,
    TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingSM — observable state machine.
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct CountingSM {
    state_bytes: Arc<Mutex<Vec<u8>>>,
    restored: Arc<AtomicUsize>,
}

impl StateMachine for CountingSM {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.state_bytes.lock().unwrap().extend_from_slice(entry);
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(self.state_bytes.lock().unwrap().clone())
    }
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        self.restored.fetch_add(1, Ordering::SeqCst);
        *self.state_bytes.lock().unwrap() = snapshot.to_vec();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CountingStorage — in-memory storage that counts full snapshot loads
// separately from meta-only probes. Overrides BOTH `load_snapshot` and
// `load_snapshot_meta` so the test can prove which one the engine used.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct CountingStorage {
    hard_state: Arc<Mutex<Option<HardState>>>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
    snapshot: Arc<Mutex<Option<(SnapshotMeta, Vec<u8>)>>>,
    full_snapshot_loads: Arc<AtomicUsize>,
    meta_snapshot_loads: Arc<AtomicUsize>,
}

impl CountingStorage {
    fn full_loads(&self) -> usize {
        self.full_snapshot_loads.load(Ordering::SeqCst)
    }
    fn meta_loads(&self) -> usize {
        self.meta_snapshot_loads.load(Ordering::SeqCst)
    }
    fn snapshot_meta(&self) -> Option<SnapshotMeta> {
        self.snapshot
            .lock()
            .unwrap()
            .as_ref()
            .map(|(m, _)| m.clone())
    }
}

impl RaftStorage for CountingStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone().unwrap_or_default())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        for e in new_entries {
            entries.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
            });
        }
        Ok(())
    }
    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let entries = self.entries.lock().unwrap();
        let mut buf = payload_buf;
        let mut written = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                buf = rest;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                written += len;
            }
        }
        Ok(written)
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
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
        Ok(self
            .entries
            .lock()
            .unwrap()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn truncate_before(&self, up_to: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index >= up_to);
        Ok(())
    }
    fn save_snapshot(&self, meta: &SnapshotMeta, bytes: &[u8]) -> Result<(), RaftError> {
        *self.snapshot.lock().unwrap() = Some((meta.clone(), bytes.to_vec()));
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        self.full_snapshot_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.lock().unwrap().clone())
    }
    fn load_snapshot_meta(&self) -> Result<Option<SnapshotMeta>, RaftError> {
        self.meta_snapshot_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot_meta())
    }
}

// ---------------------------------------------------------------------------
// AckTransport — auto-acks AppendEntries from `ack_peers` (same shape as
// tests/log_compaction.rs).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AckTransport {
    inbox: Arc<Mutex<std::collections::VecDeque<Vec<u8>>>>,
    ack_peers: Arc<Mutex<HashSet<PeerId>>>,
}

impl AckTransport {
    fn new(ack_peers: &[u64]) -> Self {
        Self {
            inbox: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            ack_peers: Arc::new(Mutex::new(ack_peers.iter().copied().map(PeerId).collect())),
        }
    }

    fn on_send(&self, peer: PeerId, frame: Vec<u8>) {
        if self.ack_peers.lock().unwrap().contains(&peer) {
            if let Ok(inbound) = decode_message(&frame) {
                let ae_opt = match inbound.message {
                    RaftMessage::AppendEntries(ae, _) => Some(*ae),
                    RaftMessage::AppendEntriesSeeded { ae, .. } => Some(*ae),
                    _ => None,
                };
                if let Some(ae) = ae_opt {
                    let match_index = ae.prev_log_index.get() + u64::from(ae.entry_count.get());
                    let resp = AppendEntriesResp {
                        term: ae.term,
                        match_index: match_index.into(),
                        success: 1,
                        _pad: [0; 7],
                    };
                    let bytes =
                        encode_message_to_bytes(peer, &RaftMessage::AppendEntriesResp(&resp))
                            .unwrap();
                    self.inbox.lock().unwrap().push_back(bytes.to_vec());
                }
            }
        }
    }
}

impl RaftTransport for AckTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.on_send(peer, frame);
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.on_send(peer, frame.to_vec());
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let inbox = self.inbox.clone();
        async move {
            let frame = inbox.lock().unwrap().pop_front();
            match frame {
                Some(data) => {
                    out[..data.len()].copy_from_slice(&data);
                    Ok(data.len())
                }
                None => Err(RaftError::Transport("inbox empty".into())),
            }
        }
    }
    fn recv_frame_timeout(
        &self,
        _timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let inbox = self.inbox.clone();
        async move {
            let frame = inbox.lock().unwrap().pop_front();
            match frame {
                Some(data) => {
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn config_3node(node_id: u64, limits: LimitsConfig) -> NodeConfig {
    let peers_id = [1u64, 2, 3];
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers_id.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers_id
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9400 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 10,
            election_min_ms: 800,
            election_max_ms: 1200,
        },
        limits,
    }
}

async fn pump_until<S, T, SM, F>(
    raft: &mut ArbitroRaft<S, T, SM>,
    mut cond: F,
    ticks: usize,
) -> bool
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
    F: FnMut() -> bool,
{
    for _ in 0..ticks {
        if cond() {
            return true;
        }
        raft.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    cond()
}

// ---------------------------------------------------------------------------
// Test 1 — steady-state apply batches never load the full snapshot bytes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn steady_state_apply_probes_meta_not_full_snapshot() {
    let storage = CountingStorage::default();
    let transport = AckTransport::new(&[2, 3]); // both followers ack
    let limits = LimitsConfig {
        compaction_threshold_entries: 8,
        compaction_threshold_bytes: 0,
        compaction_min_retain: 4,
        ..LimitsConfig::default()
    };
    let mut node = RaftNode::new(config_3node(1, limits), storage.clone(), transport).unwrap();
    node.become_leader_for_benchmark(Term(2));
    let sm = CountingSM::default();
    let mut leader = ArbitroRaft::new(node, sm.clone());

    // Phase 1 — build up a snapshot: 20 committed entries trip the C2
    // compaction trigger, persisting a snapshot at index 20.
    for i in 0..20u8 {
        leader.propose_once(&[i]).await.unwrap();
    }
    let compacted = pump_until(
        &mut leader,
        || storage.snapshot_meta().map(|m| m.last_included_index.0) == Some(20),
        200,
    )
    .await;
    assert!(compacted, "compaction never persisted the snapshot");
    assert_eq!(leader.node().last_applied(), LogIndex(20));

    // Phase 2 — STEADY STATE: a snapshot exists on disk and is fully applied.
    // Every subsequent apply batch runs the restore-idempotence check; C5
    // requires that check to use the meta probe, never the full payload.
    let full_baseline = storage.full_loads();
    let meta_baseline = storage.meta_loads();

    for i in 20..30u8 {
        leader.propose_once(&[i]).await.unwrap();
    }
    let applied = pump_until(&mut leader, || leader_applied(&sm) >= 30, 200).await;
    assert!(applied, "steady-state entries were not applied");

    assert!(
        storage.meta_loads() > meta_baseline,
        "the apply path must probe the snapshot meta on apply batches \
         (meta probes: {} -> {})",
        meta_baseline,
        storage.meta_loads(),
    );
    assert_eq!(
        storage.full_loads(),
        full_baseline,
        "steady-state apply batches must NOT load the full snapshot bytes",
    );
    // No restore may have happened — the snapshot was already applied.
    assert_eq!(sm.restored.load(Ordering::SeqCst), 0);

    // Stronger: across the WHOLE test (boot + compaction + 30 applies) the
    // full snapshot payload was never needed — every consumer of the
    // boundary (RaftNode::new, maybe_compact, the restore bail) is
    // meta-only now.
    assert_eq!(
        storage.full_loads(),
        0,
        "no code path in this scenario needs the snapshot payload",
    );
}

/// SM-observed apply progress (state_bytes grows by one byte per entry).
fn leader_applied(sm: &CountingSM) -> usize {
    sm.state_bytes.lock().unwrap().len()
}

// ---------------------------------------------------------------------------
// Test 2 — I1 default impls: `load_snapshot_meta` derives from
// `load_snapshot`; `term_at` derives from `entry_at` (B10).
// ---------------------------------------------------------------------------

/// Minimal storage that does NOT override the new defaulted methods —
/// proving the trait additions are non-breaking and the defaults correct.
#[derive(Clone, Default)]
struct DefaultOnlyStorage {
    entries: Arc<Mutex<Vec<StoredEntry>>>,
    snapshot: Arc<Mutex<Option<(SnapshotMeta, Vec<u8>)>>>,
}

impl RaftStorage for DefaultOnlyStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(HardState::default())
    }
    fn save_hard_state(&self, _state: &HardState) -> Result<(), RaftError> {
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        for e in new_entries {
            entries.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
            });
        }
        Ok(())
    }
    fn read_entries<'a>(
        &self,
        _from: LogIndex,
        _to: LogIndex,
        _out: &mut Vec<LogEntry<'a>>,
        _payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        Ok(0)
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
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
        Ok((LogIndex(0), Term(0)))
    }
    fn truncate_suffix(&self, _from: LogIndex) -> Result<(), RaftError> {
        Ok(())
    }
    fn save_snapshot(&self, meta: &SnapshotMeta, bytes: &[u8]) -> Result<(), RaftError> {
        *self.snapshot.lock().unwrap() = Some((meta.clone(), bytes.to_vec()));
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(self.snapshot.lock().unwrap().clone())
    }
    // NOTE: no `load_snapshot_meta` / `term_at` overrides — defaults in use.
}

#[test]
fn trait_defaults_derive_meta_and_term() {
    let storage = DefaultOnlyStorage::default();

    // No snapshot, empty log — both defaults answer None.
    assert!(storage.load_snapshot_meta().unwrap().is_none());
    assert!(storage.term_at(LogIndex(7)).unwrap().is_none());

    // Seed an entry and a snapshot.
    storage
        .append_entries(&[LogEntry {
            term: Term(3),
            index: LogIndex(7),
            payload: EntryPayload(b"payload"),
        }])
        .unwrap();
    let meta = SnapshotMeta {
        last_included_index: LogIndex(5),
        last_included_term: Term(2),
    };
    storage.save_snapshot(&meta, b"snapshot-bytes").unwrap();

    // Default `load_snapshot_meta` = meta of `load_snapshot`, bytes dropped.
    assert_eq!(storage.load_snapshot_meta().unwrap(), Some(meta));

    // Default `term_at` = term via `entry_at` (payload fits the 512 B buffer).
    assert_eq!(storage.term_at(LogIndex(7)).unwrap(), Some(Term(3)));
    assert!(storage.term_at(LogIndex(8)).unwrap().is_none());
}
