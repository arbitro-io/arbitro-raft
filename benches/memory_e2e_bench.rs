// ARBITRO RAFT — IN-MEMORY CONSENSUS BENCHMARK
//
// Measures pure consensus overhead: no real network, no disk IO.
// Transport simulates 2 real follower state machines (prev_log check,
// conflict truncation, entry append) — equivalent to OpenRaft's MemLogStore.
//
// Design principles vs the previous version:
//   1. Uses propose_once / propose_batch_once — real backpressure, blocks until quorum.
//      The old version used unbounded_send + commit_index polling, which had no backpressure
//      and made all client-count scenarios collapse into the same flat throughput.
//   2. Leader is created once per benchmark run (inside iter_custom), not per sample.
//   3. No yield_now() spin loop — propose_once returns on commit, no polling needed.
//   4. Batch scenarios simulate N concurrent clients by proposing N entries in one round.
//      propose_batch_once(vec![p; N]) ≡ N clients hitting client_write simultaneously.
//   5. Empty-payload baseline for direct comparison with openraft's minimal benchmark.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{decode_message_view, encode_message};
use arbitro_raft::{
    AppendEntriesResp, ArbitroRaft, BootstrapPeer, ClusterId, HardState, LogEntry, LogIndex,
    NodeConfig, PeerId, RaftError, RaftMessage, RaftStorage, RaftTransport, SnapshotMeta, Term,
};
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};

// ---------------------------------------------------------------------------
// MemStorage — sorted by index, binary-search for reads
//
// Entries are always appended in order so the Vec is always sorted.
// read_entries and entry_at use partition_point (binary search) to avoid
// O(N) scans that blow up when the log grows across iterations.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MemStorage {
    entries:    Arc<Mutex<Vec<LogEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            entries:    Arc::new(Mutex::new(Vec::new())),
            hard_state: Arc::new(Mutex::new(HardState {
                current_term: Term(1),
                voted_for:    None,
            })),
        }
    }
}

impl RaftStorage for MemStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = state.clone();
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry]) -> Result<(), RaftError> {
        self.entries.lock().unwrap().extend_from_slice(new_entries);
        Ok(())
    }
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError> {
        let entries = self.entries.lock().unwrap();
        // Binary search for the start of [from, to) — entries are sorted by index.
        let start = entries.partition_point(|e| e.index < from);
        let end   = entries.partition_point(|e| e.index < to);
        out.extend_from_slice(&entries[start..end]);
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        let cut = entries.partition_point(|e| e.index < from);
        entries.truncate(cut);
        Ok(())
    }
    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries.last().map(|e| (e.index, e.term)).unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        let entries = self.entries.lock().unwrap();
        // Binary search — entries sorted by index.
        let pos = entries.partition_point(|e| e.index <= index);
        Ok(entries.get(pos.wrapping_sub(1)).filter(|e| e.index == index).cloned())
    }
}

// ---------------------------------------------------------------------------
// PeerState — real follower log for one simulated peer.
//
// Stores (index, term) pairs only — no payload — to avoid O(N*payload_size)
// memory growth across iterations while still running real Raft follower logic:
//   1. prev_log consistency check (rejects mismatched term at prev_index)
//   2. conflict detection + suffix truncation
//   3. entry append in order
//   4. correct match_index in the response
//
// This is equivalent to OpenRaft's MemLogStore for protocol-overhead purposes.
// ---------------------------------------------------------------------------

struct PeerState {
    log: Vec<(LogIndex, Term)>,
}

impl PeerState {
    fn new() -> Self {
        Self { log: Vec::new() }
    }

    fn last_position(&self) -> (LogIndex, Term) {
        self.log.last().copied().unwrap_or((LogIndex(0), Term(0)))
    }

    fn term_at(&self, idx: LogIndex) -> Option<Term> {
        let pos = self.log.partition_point(|&(i, _)| i < idx);
        self.log.get(pos).filter(|&&(i, _)| i == idx).map(|&(_, t)| t)
    }

    /// Run follower AppendEntries logic. Returns (success, match_index).
    fn process_append(
        &mut self,
        ae: &arbitro_raft::AppendEntriesView,
    ) -> (bool, LogIndex) {
        let prev_idx  = ae.prev_log_index();
        let prev_term = ae.prev_log_term();

        // Step 1: prev_log consistency check.
        if prev_idx.0 > 0 {
            match self.term_at(prev_idx) {
                None    => return (false, self.last_position().0),
                Some(t) if t != prev_term => return (false, self.last_position().0),
                _       => {}
            }
        }

        // Step 2: append entries, truncating conflicts.
        if let Ok(iter) = ae.entries() {
            for ev in iter {
                let idx  = ev.index();
                let term = ev.term();
                let pos  = self.log.partition_point(|&(i, _)| i < idx);
                if let Some(&(ei, et)) = self.log.get(pos) {
                    if ei == idx {
                        if et == term { continue; }
                        self.log.truncate(pos); // conflict — drop suffix
                    }
                }
                self.log.push((idx, term));
            }
        }

        (true, self.last_position().0)
    }
}

// ---------------------------------------------------------------------------
// MemTransport — 2 real follower state machines that process AppendEntries.
//
// Uses std::sync::Mutex (not tokio::sync::Mutex) for the receiver.
// recv_frame_timeout(Duration::ZERO) is the hot path — it never awaits,
// so async mutex overhead is pure waste. std::sync::Mutex + try_lock is
// a single atomic CAS with no scheduler involvement.
//
// peers[0] = PeerId(2), peers[1] = PeerId(3).
// ---------------------------------------------------------------------------

struct MemTransport {
    tx:    UnboundedSender<Bytes>,
    rx:    Mutex<UnboundedReceiver<Bytes>>,
    peers: [Mutex<PeerState>; 2],
}

impl MemTransport {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();
        Self {
            tx,
            rx: Mutex::new(rx),
            peers: [Mutex::new(PeerState::new()), Mutex::new(PeerState::new())],
        }
    }

    fn peer_state(&self, peer: PeerId) -> Option<&Mutex<PeerState>> {
        match peer.0 {
            2 => Some(&self.peers[0]),
            3 => Some(&self.peers[1]),
            _ => None,
        }
    }
}

#[async_trait]
impl RaftTransport for MemTransport {
    async fn send_frame(&self, peer: PeerId, frame: Bytes) -> Result<(), RaftError> {
        let inbound = decode_message_view(frame)?;
        if let arbitro_raft::RaftMessageView::AppendEntries(ae) = &inbound.message {
            let term = ae.term();

            let (success, match_index) = match self.peer_state(peer) {
                Some(state) => state.lock().unwrap().process_append(ae),
                None        => (false, LogIndex(0)),
            };

            let resp = RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term,
                success,
                match_index,
            });
            let _ = self.tx.unbounded_send(encode_message(peer, &resp)?);
        }
        Ok(())
    }

    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        // Blocking recv — only used outside the hot path (e.g. follower idle wait).
        // futures channel doesn't have a sync blocking recv, so we spin with yield.
        loop {
            if let Ok(frame) = self.rx.lock().unwrap().try_recv() {
                return Ok(frame);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn recv_frame_timeout(&self, timeout: Duration) -> Result<Option<Bytes>, RaftError> {
        if timeout.is_zero() {
            // Hot path: single try_recv, no await, no scheduler touch.
            return Ok(self.rx.lock().unwrap().try_recv().ok());
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(frame) = self.rx.lock().unwrap().try_recv() {
                return Ok(Some(frame));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_config(node_id: PeerId) -> NodeConfig {
    NodeConfig {
        node_id,
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
        bootstrap_peers: vec![
            BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
            BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
            BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
        ],
        ..Default::default()
    }
}

fn make_leader() -> ArbitroRaft<MemStorage, MemTransport> {
    let storage   = MemStorage::new();
    let transport = MemTransport::new();
    let mut node  = arbitro_raft::RaftNode::new(make_config(PeerId(1)), storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    ArbitroRaft::new(node)
}

fn make_runtime(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// 1. Latency benchmark — single propose_once, persistent leader across iters
//
//    Reports the average round-trip latency for one entry to reach quorum.
//    Empty payload = pure Raft overhead (matches openraft baseline).
//    1 KB payload = realistic application entry size.
// ---------------------------------------------------------------------------

fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_latency");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    let rt = make_runtime(4);

    // Empty payload — direct comparison with openraft "1 client / single write" latency
    group.throughput(Throughput::Elements(1));
    group.bench_function("propose_once/empty", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut raft = make_leader();
            let start = Instant::now();
            for _ in 0..iters {
                raft.propose_once(Bytes::new()).await.unwrap();
            }
            start.elapsed()
        });
    });

    // 1 KB payload — common application workload
    group.bench_function("propose_once/1kb", |b| {
        let payload = Bytes::from(vec![0xAA; 1024]);
        b.to_async(&rt).iter_custom(move |iters| {
            let p = payload.clone();
            async move {
                let mut raft = make_leader();
                let start = Instant::now();
                for _ in 0..iters {
                    raft.propose_once(p.clone()).await.unwrap();
                }
                start.elapsed()
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Batch throughput — propose_batch_once(N entries per round)
//
//    propose_batch_once(vec![p; N]) is equivalent to N concurrent clients all
//    having their entries batched into a single AppendEntries round by the leader.
//    This is how openraft achieves throughput scaling: more clients → larger batches.
//
//    Throughput = N / time_per_call → reported as elem/s (ops/s).
// ---------------------------------------------------------------------------

fn bench_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_batch_throughput");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime(4);

    // Empty payload for fair comparison with openraft
    for &batch in &[1u64, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("empty/clients_{batch}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut raft = make_leader();
                let batch_payload = vec![Bytes::new(); batch as usize];
                let start = Instant::now();
                for _ in 0..iters {
                    raft.propose_batch_once(batch_payload.clone()).await.unwrap();
                }
                start.elapsed()
            });
        });
    }

    // 1 KB payload — realistic workload at key batch sizes
    for &batch in &[1u64, 4096] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("1kb/clients_{batch}"), |b| {
            let p = Bytes::from(vec![0xAA; 1024]);
            b.to_async(&rt).iter_custom(move |iters| {
                let p = p.clone();
                async move {
                    let mut raft = make_leader();
                    let batch_payload = vec![p.clone(); batch as usize];
                    let start = Instant::now();
                    for _ in 0..iters {
                        raft.propose_batch_once(batch_payload.clone()).await.unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Batch write throughput — propose_batch_once with batch_size > 1
//
//    Simulates the openraft "batch=4" scenario: each logical client sends
//    a batch of 4 entries per call. Total entries per round = clients * batch.
// ---------------------------------------------------------------------------

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_batch_write");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime(4);

    // clients=4096, batch_size=4 → 16384 entries per round
    let total_entries: u64 = 4096 * 4;
    group.throughput(Throughput::Elements(total_entries));
    group.bench_function("empty/clients_4096_batch_4", |b| {
        b.to_async(&rt).iter_custom(move |iters| async move {
            let mut raft = make_leader();
            let batch_payload = vec![Bytes::new(); total_entries as usize];
            let start = Instant::now();
            for _ in 0..iters {
                raft.propose_batch_once(batch_payload.clone()).await.unwrap();
            }
            start.elapsed()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Concurrent writes — N real tokio tasks each calling ClientHandle::write()
//
//    This is the closest equivalent to OpenRaft's benchmark:
//      - Raft event loop runs in a dedicated tokio task via raft.run()
//      - N independent tasks call client_handle.write() concurrently
//      - The leader naturally batches writes that arrive simultaneously
//      - Each write blocks until its entry reaches quorum (real backpressure)
//
//    Throughput = N * iters / elapsed → reported as elem/s (1 elem = 1 write).
// ---------------------------------------------------------------------------

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_concurrent_writes");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    // More workers to handle N concurrent client tasks + 1 raft task.
    let rt = make_runtime(16);

    for &num_clients in &[1usize, 4, 16, 64, 256, 1024] {
        group.throughput(Throughput::Elements(num_clients as u64));
        group.bench_function(format!("empty/clients_{num_clients}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut raft  = make_leader();
                let handle    = raft.client_handle();

                // Raft event loop in its own dedicated task.
                let raft_task = tokio::spawn(async move { let _ = raft.run().await; });

                let start = Instant::now();

                // N concurrent clients each submitting `iters` writes.
                let client_tasks: Vec<_> = (0..num_clients)
                    .map(|_| {
                        let h = handle.clone();
                        tokio::spawn(async move {
                            for _ in 0..iters {
                                h.write(Bytes::new()).await.unwrap();
                            }
                        })
                    })
                    .collect();

                for t in client_tasks { t.await.unwrap(); }

                let elapsed = start.elapsed();
                raft_task.abort();
                elapsed
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_latency, bench_batch_throughput, bench_batch_write, bench_concurrent_writes);
criterion_main!(benches);
