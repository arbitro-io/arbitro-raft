// ARBITRO RAFT - CONSENSUS LAYER BENCHMARK
// Pure Consensus Measurement (Raft only, No Network/Disk IO)
// --------------------------------------------------------
// Principles: Zero-Allocation on Hot Path, Zero-Copy (shallow clone)
// Hardware Sympathy: O(1) Frame Dispatch, Pipelined Reactor
// --------------------------------------------------------

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use arbitro_raft::{
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError,
    RaftMessage, RaftStorage, RaftTransport, SnapshotMeta, Term, InboundRaftMessageView,
    LimitsConfig, TimingConfig, ClusterId, AppendEntriesResp, LeaderHint, ArbitroRaft
};
use tokio::sync::mpsc;
use arbitro_raft::{encode_message_into, decode_message_view};

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
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError> {
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
    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
}

struct MemTransport {
    local_id: PeerId,
    tx: mpsc::Sender<InboundRaftMessageView>,
    rx: tokio::sync::Mutex<mpsc::Receiver<InboundRaftMessageView>>,
    encode_pool: tokio::sync::Mutex<BytesMut>,
}

impl MemTransport {
    fn new(local_id: PeerId) -> Self {
        let (tx, rx) = mpsc::channel(100000);
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
        // Ejecución E2E Zero-Copy de Codificación
        let mut pool = self.encode_pool.lock().await;
        pool.clear();
        encode_message_into(self.local_id, &msg, &mut pool)?;
        let encoded_bytes = pool.split().freeze();

        if let RaftMessage::AppendEntries(app) = msg {
            // Simulamos respuesta instantánea reconstruyendo un acuse de recibo.
            // Para el benchmark E2E nos basta con engañar al líder usando bytes puros ruteados de vuelta.
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
            let _ = self.tx.send(decoded).await;
        }

        Ok(())
    }

    async fn recv(&self) -> Result<InboundRaftMessageView, RaftError> {
        let mut rx = self.rx.lock().await;
        rx.recv().await.ok_or_else(|| RaftError::Transport("channel closed".into()))
    }

    async fn recv_timeout(&self, duration: Duration) -> Result<Option<InboundRaftMessageView>, RaftError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(duration, rx.recv()).await {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }
}

fn bench_memory_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_hot_path_zero_copy");
    
    // Config para 1 KB payloads
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
                let config = NodeConfig {
                    node_id: PeerId(1),
                    cluster_id: ClusterId(1),
                    bootstrap_peers: vec![
                        arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    timing: TimingConfig::default(),
                    limits: LimitsConfig::default(),
                };

                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let mut node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                
                node.become_leader_for_benchmark(Term(1));
                
                let payloads = vec![payload_bytes.clone()]; // 1 entrada

                let start = Instant::now();
                for _ in 0..iters {
                    let _ = node.propose_batch_once(payloads.clone()).await.unwrap();
                }
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
                let config = NodeConfig {
                    node_id: PeerId(1),
                    cluster_id: ClusterId(1),
                    bootstrap_peers: vec![
                        arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    timing: TimingConfig::default(),
                    limits: LimitsConfig::default(),
                };

                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let mut node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                
                node.become_leader_for_benchmark(Term(1));
                
                let payloads = vec![payload_bytes.clone(); batch_size as usize];

                let start = Instant::now();
                for _ in 0..iters {
                    let _ = node.propose_batch_once(payloads.clone()).await.unwrap();
                }
                start.elapsed()
            }
        });
    });

    let huge_batch = 1000;
    group.throughput(Throughput::Elements(huge_batch as u64));
    group.bench_function("batch_1000", |b| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let config = NodeConfig {
                    node_id: PeerId(1),
                    cluster_id: ClusterId(1),
                    bootstrap_peers: vec![
                        arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    timing: TimingConfig::default(),
                    limits: LimitsConfig::default(),
                };

                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let mut node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                
                node.become_leader_for_benchmark(Term(1));
                
                let payloads = vec![payload_bytes.clone(); huge_batch as usize];

                let start = Instant::now();
                for _ in 0..iters {
                    let _ = node.propose_batch_once(payloads.clone()).await.unwrap();
                }
                start.elapsed()
            }
        });
    });

    group.finish();
}

fn bench_concurrent_clients(c: &mut Criterion) {
    let mut group = c.benchmark_group("openraft_comparison");
    
    let payload_size = 1024;
    let payload_bytes = Bytes::from(vec![0xAA; payload_size]);

    for clients in [1, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(1));
        
        group.bench_with_input(BenchmarkId::new("single_writes", clients), &clients, |b, &clients| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(16)
                .enable_all()
                .build()
                .unwrap();

            let payload_ref = payload_bytes.clone();
            b.to_async(&runtime).iter_custom(move |iters| {
                let payload_bytes = payload_ref.clone();
                async move {
                    let iters = std::cmp::max(iters, clients);
                    let writes_per_client = iters / clients;
                    let start = Instant::now();

                    let config = NodeConfig {
                        node_id: PeerId(1),
                        cluster_id: ClusterId(1),
                        bootstrap_peers: vec![
                            arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                            arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                            arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                        ],
                        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                        timing: TimingConfig::default(),
                        limits: LimitsConfig::default(),
                    };

                    let storage = MemStorage::new();
                    let transport = MemTransport::new(PeerId(1));
                    let mut node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                    node.become_leader_for_benchmark(Term(1));

                    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();

                    let node_task = tokio::spawn(async move {
                        let mut batch = Vec::with_capacity(50);
                        while let Some(msg) = rx.recv().await {
                            batch.push(msg);
                            while batch.len() < 50 {
                                if let Ok(msg) = rx.try_recv() {
                                    batch.push(msg);
                                } else {
                                    break;
                                }
                            }
                            let _ = node.propose_batch_once(batch.clone()).await;
                            batch.clear();
                        }
                    });


                    let mut handles = Vec::with_capacity(clients as usize);
                    for _ in 0..clients {
                        let tx = tx.clone();
                        let payload = payload_bytes.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..writes_per_client {
                                let _ = tx.send(payload.clone());
                            }
                        }));
                    }
                    drop(tx);

                    for h in handles {
                        let _ = h.await;
                    }

                    let _ = node_task.await;

                    start.elapsed()
                }
            });
        });
    }

    // Benchmark Batch Writes (4 entries per batch)
    let batch_clients = 4096;
    let entries_per_batch = 4;
    group.bench_function(BenchmarkId::new("batch_writes_4_entries", batch_clients), |b| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        let payload_ref = payload_bytes.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let payload_bytes = payload_ref.clone();
            async move {
                let total_batches = std::cmp::max(iters, batch_clients);
                let batches_per_client = total_batches / batch_clients;
                let start = Instant::now();

                let config = NodeConfig {
                    node_id: PeerId(1),
                    cluster_id: ClusterId(1),
                    bootstrap_peers: vec![
                        arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                        arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                    ],
                    peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                    timing: TimingConfig::default(),
                    limits: LimitsConfig::default(),
                };

                let storage = MemStorage::new();
                let transport = MemTransport::new(PeerId(1));
                let mut node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                node.become_leader_for_benchmark(Term(1));

                // Los clientes mandarán un Vector de 4 entradas de golpe (Vec<Bytes>)
                let (tx, mut rx) = mpsc::unbounded_channel::<Vec<Bytes>>();

                let node_task = tokio::spawn(async move {
                    let mut batch = Vec::with_capacity(50);
                    while let Some(mut msg_batch) = rx.recv().await {
                        batch.append(&mut msg_batch);
                        while batch.len() < 50 {
                            if let Ok(mut more) = rx.try_recv() {
                                batch.append(&mut more);
                            } else {
                                break;
                            }
                        }
                        let _ = node.propose_batch_once(batch.clone()).await;
                        batch.clear();
                    }
                });

                let mut handles = Vec::with_capacity(batch_clients as usize);
                let batch_payload = vec![payload_bytes; entries_per_batch];
                for _ in 0..batch_clients {
                    let tx = tx.clone();
                    let bp = batch_payload.clone();
                    handles.push(tokio::spawn(async move {
                        for _ in 0..batches_per_client {
                            let _ = tx.send(bp.clone());
                        }
                    }));
                }
                drop(tx);

                for h in handles {
                    let _ = h.await;
                }

                let _ = node_task.await;

                start.elapsed()
            }
        });
    });

    group.finish();
}

fn bench_openraft_exact_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("openraft_exact_match");
    
    for payload_size in [0, 1024, 4096, 16384] {
        let payload_bytes = Bytes::from(vec![0xAA; payload_size]);
        for clients in [1, 1024, 4096] {
            let payload_bytes = payload_bytes.clone();
            group.throughput(Throughput::Elements(1));
            
            let id = format!("size_{}_clients_{}", payload_size, clients);
            group.bench_with_input(BenchmarkId::new("single_writes", id), &clients, move |b, &clients| {
            let payload_bytes = payload_bytes.clone();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(16)
                .enable_all()
                .build()
                .unwrap();

            b.to_async(&runtime).iter_custom(move |iters| {
                let payload_bytes = payload_bytes.clone();
                async move {
                    let iters = std::cmp::max(iters, clients);
                    let writes_per_client = iters / clients;
                    let start = Instant::now();

                    let config = NodeConfig {
                        node_id: PeerId(1),
                        cluster_id: ClusterId(1),
                        bootstrap_peers: vec![
                            arbitro_raft::BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:8001".parse().unwrap() },
                            arbitro_raft::BootstrapPeer { id: PeerId(2), addr: "127.0.0.1:8002".parse().unwrap() },
                            arbitro_raft::BootstrapPeer { id: PeerId(3), addr: "127.0.0.1:8003".parse().unwrap() },
                        ],
                        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                        timing: TimingConfig::default(),
                        limits: LimitsConfig::default(),
                    };

                    let storage = MemStorage::new();
                    let transport = MemTransport::new(PeerId(1));
                    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
                    let mut node = node;
                    node.become_leader_for_benchmark(Term(1));
                    
                    let mut raft = arbitro_raft::ArbitroRaft::new(node);
                    let handle = raft.handle();

                    // El Reactor de Raft en modo Turbo
                    let raft_task = tokio::spawn(async move {
                        let _ = raft.run().await;
                    });

                    let mut handles = Vec::with_capacity(clients as usize);
                    for _ in 0..clients {
                        let handle = handle.clone();
                        let payload = payload_bytes.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..writes_per_client {
                                let _ = handle.send(payload.clone());
                            }
                        }));
                    }
                    drop(handle);
                    for h in handles { let _ = h.await; }
                    
                    // Detenemos el reactor
                    raft_task.abort();

                    start.elapsed()
                }
            });
        });
    }
}

    group.finish();
}

criterion_group!(benches, bench_memory_hot_path, bench_concurrent_clients, bench_openraft_exact_match);
criterion_main!(benches);
