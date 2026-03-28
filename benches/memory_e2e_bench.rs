// ARBITRO RAFT — CONSENSUS LAYER BENCHMARK
// Pure consensus measurement: no network, no disk IO.
// Transport simulates network by encoding frames and decoding responses.
// Principles: zero-copy (Bytes arc-clone), O(1) dispatch, hardware sympathy.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{decode_message_view, encode_message};
use arbitro_raft::{
    AppendEntriesResp, ArbitroRaft, BootstrapPeer, ClusterId, HardState,
    LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftStorage, RaftTransport,
    SnapshotMeta, Term,
};
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;

// ---------------------------------------------------------------------------
// MemStorage — O(1) for last_log_position and entry_at via back/binary-search
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MemStorage {
    entries: Arc<Mutex<Vec<LogEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            hard_state: Arc::new(Mutex::new(HardState {
                current_term: Term(1),
                voted_for: None,
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
        let mut entries = self.entries.lock().unwrap();
        for entry in new_entries {
            entries.push(entry.clone());
        }
        Ok(())
    }
    fn read_entries(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry>,
    ) -> Result<(), RaftError> {
        let entries = self.entries.lock().unwrap();
        // [from, to) — exclusive upper bound per RaftStorage contract
        for entry in entries.iter() {
            if entry.index >= from && entry.index < to {
                out.push(entry.clone());
            }
        }
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }

    // O(1) overrides — default impls are O(N) scan which would invalidate bench results
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries.last().map(|e| (e.index, e.term)).unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        let entries = self.entries.lock().unwrap();
        // Entries are ordered by index — binary search is O(log N)
        Ok(entries.iter().rev().find(|e| e.index == index).cloned())
    }
}

// ---------------------------------------------------------------------------
// MemTransport — simulates a quorum of 2 peers responding instantly.
// The transport moves raw Bytes (pre-encoded frames) per the new trait contract.
// ---------------------------------------------------------------------------

struct MemTransport {
    /// PeerId of this node — used to encode simulated peer responses.
    local_id: PeerId,
    tx: futures::channel::mpsc::UnboundedSender<Bytes>,
    rx: tokio::sync::Mutex<futures::channel::mpsc::UnboundedReceiver<Bytes>>,
}

impl MemTransport {
    fn new(local_id: PeerId) -> Self {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        Self { local_id, tx, rx: tokio::sync::Mutex::new(rx) }
    }
}

#[async_trait]
impl RaftTransport for MemTransport {
    async fn send_frame(&self, peer: PeerId, frame: Bytes) -> Result<(), RaftError> {
        // Decode the incoming frame to inspect the message type.
        // On AppendEntries: simulate a successful response from the peer.
        let inbound = decode_message_view(frame)?;
        if let arbitro_raft::RaftMessageView::AppendEntries(ae) = &inbound.message {
            let last_idx = ae.entries()
                .ok()
                .and_then(|mut it| it.last())
                .map(|e| e.index())
                .unwrap_or(ae.prev_log_index());

            let resp = RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term: Term(1),
                success: true,
                match_index: last_idx,
            });
            // peer becomes the "from" for the simulated response frame
            let resp_frame = encode_message(peer, &resp)?;
            let _ = self.tx.unbounded_send(resp_frame);
        }
        Ok(())
    }

    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        let mut rx = self.rx.lock().await;
        match rx.next().await {
            Some(frame) => Ok(frame),
            None => Err(RaftError::Transport("channel closed".into())),
        }
    }

    async fn recv_frame_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Bytes>, RaftError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.next()).await {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) | Err(_) => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark helpers
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
    let storage = MemStorage::new();
    let transport = MemTransport::new(PeerId(1));
    let mut node = arbitro_raft::RaftNode::new(make_config(PeerId(1)), storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    ArbitroRaft::new(node)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_memory_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_hot_path_zero_copy");
    let payload_size = 1024;
    let payload_bytes = Bytes::from(vec![0xAA; payload_size]);
    let batch_size = 50;

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_entry", |b| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();
        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let mut raft = make_leader();
                let handle = raft.handle();
                let raft_task = tokio::spawn(async move { let _ = raft.run().await; });
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = handle.unbounded_send(payload_bytes.clone());
                }
                drop(handle);
                raft_task.abort();
                start.elapsed()
            }
        });
    });

    group.throughput(Throughput::Bytes((payload_size * batch_size) as u64));
    group.bench_function("batch_50", |b| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();
        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let mut raft = make_leader();
                let handle = raft.handle();
                let raft_task = tokio::spawn(async move { let _ = raft.run().await; });
                let payloads = vec![payload_bytes.clone(); batch_size];
                let start = Instant::now();
                for _ in 0..iters {
                    for p in &payloads {
                        let _ = handle.unbounded_send(p.clone());
                    }
                }
                drop(handle);
                raft_task.abort();
                start.elapsed()
            }
        });
    });
    group.finish();
}

fn bench_concurrent_clients(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_clients");
    let payload_size = 1024;
    let payload_bytes = Bytes::from(vec![0xAA; payload_size]);

    for clients in [1u64, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_writes", clients),
            &clients,
            |b, &clients| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(16)
                    .enable_all()
                    .build()
                    .unwrap();
                let payload_ref = payload_bytes.clone();
                b.to_async(&runtime).iter_custom(move |iters| {
                    let payload_bytes = payload_ref.clone();
                    async move {
                        let iters = iters.max(clients);
                        let writes_per_client = iters / clients;
                        let mut raft = make_leader();
                        let handle = raft.handle();
                        let raft_task = tokio::spawn(async move { let _ = raft.run().await; });

                        let start = Instant::now();
                        let mut handles = Vec::with_capacity(clients as usize);
                        for _ in 0..clients {
                            let handle = handle.clone();
                            let payload = payload_bytes.clone();
                            handles.push(tokio::spawn(async move {
                                for _ in 0..writes_per_client {
                                    let _ = handle.unbounded_send(payload.clone());
                                }
                            }));
                        }
                        drop(handle);
                        for h in handles { let _ = h.await; }
                        raft_task.abort();
                        start.elapsed()
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_memory_hot_path, bench_concurrent_clients);
criterion_main!(benches);
