// ARBITRO RAFT - CONSENSUS LAYER BENCHMARK
// Pure Consensus Measurement (Raft only, No Network/Disk IO)
// --------------------------------------------------------
// Principles: Zero-Allocation on Hot Path, Zero-Copy (shallow clone)
// Hardware Sympathy: O(1) Frame Dispatch, Pipelined Reactor
// --------------------------------------------------------

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{decode_message_view, encode_message_into};
use arbitro_raft::{
    AppendEntriesResp, ArbitroRaft, BootstrapPeer, ClusterId, HardState, InboundRaftMessageView,
    LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftStorage,
    RaftTransport, SnapshotMeta, Term,
};
use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;

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
                commit_index: LogIndex(0),
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
        for entry in entries.iter() {
            if entry.index >= from && entry.index <= to {
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
    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> { Ok(None) }
}

struct MemTransport {
    local_id: PeerId,
    tx: futures::channel::mpsc::UnboundedSender<InboundRaftMessageView>,
    rx: tokio::sync::Mutex<futures::channel::mpsc::UnboundedReceiver<InboundRaftMessageView>>,
    encode_pool: tokio::sync::Mutex<BytesMut>,
}

impl MemTransport {
    fn new(local_id: PeerId) -> Self {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        Self {
            local_id,
            tx,
            rx: tokio::sync::Mutex::new(rx),
            encode_pool: tokio::sync::Mutex::new(BytesMut::with_capacity(1024 * 1024)),
        }
    }
}

#[async_trait::async_trait]
impl RaftTransport for MemTransport {
    async fn send(&self, peer: PeerId, msg: RaftMessage) -> Result<(), RaftError> {
        let mut pool = self.encode_pool.lock().await;
        pool.clear();
        encode_message_into(self.local_id, &msg, &mut pool)?;
        let _encoded_bytes = pool.split().freeze();

        if let RaftMessage::AppendEntries(app) = msg {
            let next_index = if let Some(last) = app.entries().ok().and_then(|mut x| x.last()) {
                last.index()
            } else {
                app.prev_log_index()
            };

            let resp = RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term: Term(1),
                success: true,
                match_index: next_index,
            });

            let mut resp_pool = BytesMut::with_capacity(128);
            encode_message_into(peer, &resp, &mut resp_pool)?;
            let decoded = decode_message_view(resp_pool.freeze())?;
            let _ = self.tx.unbounded_send(decoded);
        }
        Ok(())
    }

    async fn recv(&self) -> Result<InboundRaftMessageView, RaftError> {
        let mut rx = self.rx.lock().await;
        match rx.next().await {
            Some(msg) => Ok(msg),
            None => Err(RaftError::Transport("channel closed".into())),
        }
    }

    async fn recv_timeout(&self, timeout: Duration) -> Result<Option<InboundRaftMessageView>, RaftError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.next()).await {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }
}

fn bench_memory_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_hot_path_zero_copy");
    let payload_size = 1024;
    let payload_bytes = Bytes::from(vec![0xAA; payload_size]);
    let batch_size = 50;

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_entry", |b| {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(16).enable_all().build().unwrap();
        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let config = NodeConfig {
                    node_id: PeerId(1), cluster_id: ClusterId(1),
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    bootstrap_peers: vec![
                        BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    ..Default::default()
                };
                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                let mut node = node;
                node.become_leader_for_benchmark(Term(1));
                
                let mut raft = ArbitroRaft::new(node);
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
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(16).enable_all().build().unwrap();
        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let config = NodeConfig {
                    node_id: PeerId(1), cluster_id: ClusterId(1),
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    bootstrap_peers: vec![
                        BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    ..Default::default()
                };
                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                let mut node = node;
                node.become_leader_for_benchmark(Term(1));
                
                let mut raft = ArbitroRaft::new(node);
                let handle = raft.handle();
                let raft_task = tokio::spawn(async move { let _ = raft.run().await; });

                let payloads = vec![payload_bytes.clone(); batch_size as usize];
                let start = Instant::now();
                for _ in 0..iters {
                    for p in &payloads { let _ = handle.unbounded_send(p.clone()); }
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

    for clients in [1, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("single_writes", clients), &clients, |b, &clients| {
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(16).enable_all().build().unwrap();
            let payload_ref = payload_bytes.clone();
            b.to_async(&runtime).iter_custom(move |iters| {
                let payload_bytes = payload_ref.clone();
                async move {
                    let iters = std::cmp::max(iters, clients);
                    let writes_per_client = iters / clients;
                    let config = NodeConfig {
                        node_id: PeerId(1), cluster_id: ClusterId(1),
                        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                        bootstrap_peers: vec![
                            BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                            BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                            BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                        ],
                        ..Default::default()
                    };
                    let storage = MemStorage::new();
                    let transport = MemTransport::new(PeerId(1));
                    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                    let mut node = node;
                    node.become_leader_for_benchmark(Term(1));
                    
                    let mut raft = ArbitroRaft::new(node);
                    let handle = raft.handle();
                    let raft_task = tokio::spawn(async move { let _ = raft.run().await; });

                    let start = Instant::now();
                    let mut handles = Vec::with_capacity(clients as usize);
                    for _ in 0..clients {
                        let handle = handle.clone();
                        let payload = payload_bytes.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..writes_per_client { let _ = handle.unbounded_send(payload.clone()); }
                        }));
                    }
                    drop(handle);
                    for h in handles { let _ = h.await; }
                    raft_task.abort();
                    start.elapsed()
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_memory_hot_path, bench_concurrent_clients);
criterion_main!(benches);
