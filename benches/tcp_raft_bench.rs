use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, HardState,
    LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId, RaftError,
    RaftStorage, RaftTransport, Term, TimingConfig,
};

// --- Almacenamiento en Memoria (Sencillo para el bench) ---
#[derive(Clone)]
struct MemStorage {
    entries: Arc<StdMutex<Vec<LogEntry>>>,
    hard_state: Arc<StdMutex<HardState>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            entries: Arc::new(StdMutex::new(Vec::new())),
            hard_state: Arc::new(StdMutex::new(HardState {
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
        entries.extend(new_entries.iter().cloned());
        Ok(())
    }
    fn read_entries(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry>,
    ) -> Result<(), RaftError> {
        let entries = self.entries.lock().unwrap();
        for e in entries.iter() {
            // [from, to) — exclusive upper bound per RaftStorage contract
            if e.index >= from && e.index < to {
                out.push(e.clone());
            }
        }
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(
        &self,
        _meta: &arbitro_raft::SnapshotMeta,
        _snapshot: &[u8],
    ) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(arbitro_raft::SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
}

// --- Transporte TCP (Agnóstico, implementado solo para el bench) ---
#[derive(Clone)]
struct TcpTransport {
    inner: Arc<TcpTransportInner>,
}

struct TcpTransportInner {
    _local_id: PeerId,
    peer_addrs: HashMap<PeerId, SocketAddr>,
    inbound_tx: futures::channel::mpsc::UnboundedSender<Bytes>,
    inbound_rx: tokio::sync::Mutex<futures::channel::mpsc::UnboundedReceiver<Bytes>>,
    connections: tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>,
}

impl TcpTransport {
    fn new(local_id: PeerId, peer_addrs: HashMap<PeerId, SocketAddr>) -> Self {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        Self {
            inner: Arc::new(TcpTransportInner {
                _local_id: local_id,
                peer_addrs,
                inbound_tx: tx,
                inbound_rx: tokio::sync::Mutex::new(rx),
                connections: tokio::sync::Mutex::new(HashMap::new()),
            }),
        }
    }

    async fn listen(&self, addr: SocketAddr) {
        let listener = TcpListener::bind(addr).await.unwrap();
        let inbound_tx = self.inner.inbound_tx.clone();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut tx = inbound_tx.clone();
                tokio::spawn(async move {
                    let mut buf = BytesMut::with_capacity(65536);
                    loop {
                        if stream.read_buf(&mut buf).await.is_err() {
                            break;
                        }

                        while buf.len() >= 32 {
                            let body_len =
                                u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
                            let total_len = 32 + body_len;
                            if buf.len() < total_len {
                                break;
                            }

                            let frame = buf.split_to(total_len).freeze();
                            // Send raw bytes — the node decodes them
                            let _ = tx.unbounded_send(frame);
                        }
                    }
                });
            }
        });
    }
}

#[async_trait::async_trait]
impl RaftTransport for TcpTransport {
    async fn send_frame(&self, peer: PeerId, frame: Bytes) -> Result<(), RaftError> {
        let addr = self
            .inner
            .peer_addrs
            .get(&peer)
            .copied()
            .ok_or_else(|| RaftError::Protocol("Unknown peer".into()))?;

        let mut conns = self.inner.connections.lock().await;
        let stream_arc = if let Some(s) = conns.get(&peer) {
            s.clone()
        } else {
            let s = TcpStream::connect(addr)
                .await
                .map_err(|e| RaftError::Transport(e.to_string()))?;
            let s = Arc::new(tokio::sync::Mutex::new(s));
            conns.insert(peer, s.clone());
            s
        };
        drop(conns);

        let mut stream = stream_arc.lock().await;
        // frame is already encoded — write directly to the socket
        stream
            .write_all(&frame)
            .await
            .map_err(|e| RaftError::Transport(e.to_string()))?;
        Ok(())
    }

    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        let mut rx = self.inner.inbound_rx.lock().await;
        rx.next().await.ok_or(RaftError::Transport("Closed".into()))
    }

    async fn recv_frame_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Bytes>, RaftError> {
        let mut rx = self.inner.inbound_rx.lock().await;
        match tokio::time::timeout(timeout, rx.next()).await {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) => Err(RaftError::Transport("Closed".into())),
            Err(_) => Ok(None),
        }
    }
}

// --- Benchmark ---
fn bench_tcp_raft(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_raft_e2e");

    for clients in [1, 1024] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_writes_tcp", clients),
            &clients,
            |b, &clients| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter_custom(|iters| async move {
                    let iters = std::cmp::max(iters, clients as u64);
                    let writes_per_client = iters / clients as u64;

                    let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
                    let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();
                    let addr3: SocketAddr = "127.0.0.1:9003".parse().unwrap();

                    let mut peers = HashMap::new();
                    peers.insert(PeerId(1), addr1);
                    peers.insert(PeerId(2), addr2);
                    peers.insert(PeerId(3), addr3);

                    let t1 = TcpTransport::new(PeerId(1), peers.clone());
                    let t2 = TcpTransport::new(PeerId(2), peers.clone());
                    let t3 = TcpTransport::new(PeerId(3), peers.clone());

                    t1.listen(addr1).await;
                    t2.listen(addr2).await;
                    t3.listen(addr3).await;

                    let config1 = NodeConfig {
                        node_id: PeerId(1),
                        cluster_id: ClusterId(1),
                        bootstrap_peers: vec![
                            BootstrapPeer {
                                id: PeerId(1),
                                addr: addr1,
                            },
                            BootstrapPeer {
                                id: PeerId(2),
                                addr: addr2,
                            },
                            BootstrapPeer {
                                id: PeerId(3),
                                addr: addr3,
                            },
                        ],
                        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
                        timing: TimingConfig::default(),
                        limits: LimitsConfig::default(),
                    };

                    let storage1 = MemStorage::new();
                    let mut node1 = arbitro_raft::RaftNode::new(config1, storage1, t1).unwrap();
                    node1.become_leader_for_benchmark(Term(1));
                    let mut raft1 = ArbitroRaft::new(node1);
                    let handle1 = raft1.handle();

                    let raft_task = tokio::spawn(async move {
                        let _ = raft1.run().await;
                    });

                    let start = Instant::now();
                    let payload = Bytes::from(vec![0x44; 16]);

                    let mut handles = Vec::with_capacity(clients);
                    for _ in 0..clients {
                        let handle = handle1.clone();
                        let p = payload.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..writes_per_client {
                                let _ = handle.unbounded_send(p.clone());
                            }
                        }));
                    }

                    for h in handles {
                        let _ = h.await;
                    }
                    raft_task.abort();

                    start.elapsed()
                });
            },
        );
    }
}

criterion_group!(benches, bench_tcp_raft);
criterion_main!(benches);
