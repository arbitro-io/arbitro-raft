// ARBITRO RAFT — TCP E2E BENCHMARK
//
// Measures consensus overhead over real TCP loopback (127.0.0.1).
// Followers are simulated state machines (same PeerState as memory bench)
// running as independent tokio tasks connected via real TCP sockets.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{
    decode_message, encode_message_vectored, protocol::codec::AppendEntriesView, AppendEntriesResp,
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LogEntry, LogIndex, NodeConfig,
    PeerId, RaftError, RaftMessage, RaftStorage, RaftTransport, SnapshotMeta, Term,
};
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::channel::mpsc::{self, UnboundedReceiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// MemStorage — O(1) index lookup via base_index offset arithmetic.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone)]
struct MemStorage {
    base_index: Arc<AtomicU64>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            base_index: Arc::new(AtomicU64::new(0)),
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
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        if new_entries.is_empty() {
            return Ok(());
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            self.base_index
                .store(new_entries[0].index.0, Ordering::Relaxed);
        }
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
        if entries.is_empty() {
            return Ok(0);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let start = (from.0.saturating_sub(base)) as usize;
        let end = (to.0.saturating_sub(base)) as usize;
        let end = end.min(entries.len());

        let mut offset = 0;
        if start < end {
            for e in &entries[start..end] {
                let len = e.payload.len();
                if offset + len > payload_buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                payload_buf[offset..offset + len].copy_from_slice(&e.payload);

                // SAFETY: We ensure payload_buf lives as long as 'a
                let payload_slice = unsafe {
                    std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[offset..offset + len])
                };

                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(payload_slice),
                });
                offset += len;
            }
        }
        Ok(offset)
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(());
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let cut = (from.0.saturating_sub(base)) as usize;
        entries.truncate(cut);
        if entries.is_empty() {
            self.base_index.store(0, Ordering::Relaxed);
        }
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(None);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        if index.0 < base {
            return Ok(None);
        }
        let pos = (index.0 - base) as usize;
        if let Some(e) = entries.get(pos) {
            if payload_buf.len() < e.payload.len() {
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[..e.payload.len()].copy_from_slice(&e.payload);
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(&payload_buf[..e.payload.len()]),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// PeerState — real follower log for one simulated peer.
// ---------------------------------------------------------------------------

struct PeerState {
    base_index: u64,
    log: Vec<(LogIndex, Term)>,
}

impl PeerState {
    fn new() -> Self {
        Self {
            base_index: 0,
            log: Vec::new(),
        }
    }

    fn last_position(&self) -> (LogIndex, Term) {
        self.log.last().copied().unwrap_or((LogIndex(0), Term(0)))
    }

    fn term_at(&self, idx: LogIndex) -> Option<Term> {
        if self.log.is_empty() || idx.0 < self.base_index {
            return None;
        }
        let pos = (idx.0 - self.base_index) as usize;
        self.log
            .get(pos)
            .filter(|&&(i, _)| i == idx)
            .map(|&(_, t)| t)
    }

    fn process_append(&mut self, ae: &AppendEntriesView<'_>) -> (bool, LogIndex) {
        let prev_idx = ae.prev_log_index();
        let prev_term = ae.prev_log_term();

        if prev_idx.0 > 0 {
            match self.term_at(prev_idx) {
                None => return (false, self.last_position().0),
                Some(t) if t != prev_term => return (false, self.last_position().0),
                _ => {}
            }
        }

        if let Some(iter) = ae.entries() {
            for ev in iter {
                let idx = ev.index;
                let term = ev.term;
                if self.log.is_empty() {
                    self.base_index = idx.0;
                    self.log.push((idx, term));
                    continue;
                }
                if idx.0 < self.base_index {
                    continue;
                }
                let pos = (idx.0 - self.base_index) as usize;
                if pos < self.log.len() {
                    let (_, et) = self.log[pos];
                    if et == term {
                        continue;
                    }
                    self.log.truncate(pos);
                    if self.log.is_empty() {
                        self.base_index = idx.0;
                    }
                }
                self.log.push((idx, term));
            }
        }

        (true, self.last_position().0)
    }
}

// ---------------------------------------------------------------------------
// TcpTransport — for the leader node only.
// ---------------------------------------------------------------------------

struct TcpTransport {
    inbound_rx: Arc<Mutex<UnboundedReceiver<Vec<u8>>>>,
    peer_addrs: Arc<HashMap<PeerId, SocketAddr>>,
    connections: Arc<tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>>,
}

impl TcpTransport {
    fn new(rx: UnboundedReceiver<Vec<u8>>, peer_addrs: HashMap<PeerId, SocketAddr>) -> Self {
        Self {
            inbound_rx: Arc::new(Mutex::new(rx)),
            peer_addrs: Arc::new(peer_addrs),
            connections: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl RaftTransport for TcpTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let peer_addrs = self.peer_addrs.clone();
        let connections = self.connections.clone();
        let slices_owned = slices.iter().map(|s| s.to_vec()).collect::<Vec<_>>();

        async move {
            let addr = *peer_addrs
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {:?}", peer)))?;

            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };

            let mut s = stream.lock().await;
            for slice in slices_owned {
                s.write_all(&slice)
                    .await
                    .map_err(|e| RaftError::Transport(e.to_string()))?;
            }
            Ok(())
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let peer_addrs = self.peer_addrs.clone();
        let connections = self.connections.clone();
        async move {
            let addr = *peer_addrs
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {:?}", peer)))?;

            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };

            let mut s = stream.lock().await;
            s.write_all(&frame)
                .await
                .map_err(|e| RaftError::Transport(e.to_string()))?;
            Ok(())
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let inbound_rx = self.inbound_rx.clone();
        async move {
            loop {
                if let Ok(frame) = inbound_rx.lock().unwrap().try_recv() {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    return Ok(len);
                }
                tokio::task::yield_now().await;
            }
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let inbound_rx = self.inbound_rx.clone();
        async move {
            if timeout.is_zero() {
                if let Ok(frame) = inbound_rx.lock().unwrap().try_recv() {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    return Ok(Some(len));
                }
                return Ok(None);
            }
            let deadline = Instant::now() + timeout;
            loop {
                if let Ok(frame) = inbound_rx.lock().unwrap().try_recv() {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    return Ok(Some(len));
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                tokio::task::yield_now().await;
            }
        }
    }
}

const HEADER_SIZE: usize = 32;
const BODY_LEN_OFFSET: usize = 16;

async fn read_frame_into(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if buf.len() >= HEADER_SIZE {
            let body_len = u32::from_le_bytes(
                buf[BODY_LEN_OFFSET..BODY_LEN_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let total = HEADER_SIZE + body_len;
            if buf.len() >= total {
                let frame = buf[..total].to_vec();
                buf.drain(..total);
                return Some(frame);
            }
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn run_follower_sim(listener: TcpListener, leader_addr: SocketAddr, my_id: PeerId) {
    let mut leader_conn = TcpStream::connect(leader_addr).await.unwrap();
    let (mut stream, _) = listener.accept().await.unwrap();

    let mut buf = Vec::with_capacity(65536);
    let mut state = PeerState::new();

    while let Some(frame) = read_frame_into(&mut stream, &mut buf).await {
        let inbound = match decode_message(&frame) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(ae) = inbound.as_append_entries() {
            let term = ae.term();
            let (success, match_index) = state.process_append(&ae);
            let resp = AppendEntriesResp {
                term: term.0.into(),
                success: if success { 1 } else { 0 },
                match_index: match_index.0.into(),
                _pad: [0; 7],
            };

            let mut header_buf = [0u8; 128];
            let mut vectors = Vec::new();
            if let Ok(_) = encode_message_vectored(
                my_id,
                &RaftMessage::AppendEntriesResp(&resp),
                &mut header_buf,
                &mut vectors,
            ) {
                for v in vectors {
                    let _ = leader_conn.write_all(v).await;
                }
            }
        }
    }
}

struct BenchCluster {
    raft: ArbitroRaft<MemStorage, TcpTransport>,
    _tasks: Vec<JoinHandle<()>>,
}

async fn make_cluster() -> BenchCluster {
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let addr1 = l1.local_addr().unwrap();
    let addr2 = l2.local_addr().unwrap();
    let addr3 = l3.local_addr().unwrap();

    let (inbound_tx, inbound_rx) = mpsc::unbounded::<Vec<u8>>();

    let leader_listener_task = {
        let tx = inbound_tx.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = l1.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::with_capacity(65536);
                    while let Some(frame) = read_frame_into(&mut stream, &mut buf).await {
                        let _ = tx.unbounded_send(frame);
                    }
                });
            }
        })
    };

    let mut peer_addrs = HashMap::new();
    peer_addrs.insert(PeerId(2), addr2);
    peer_addrs.insert(PeerId(3), addr3);

    let transport = TcpTransport::new(inbound_rx, peer_addrs);

    let config = NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
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
        ..Default::default()
    };
    let mut node = arbitro_raft::RaftNode::new(config, MemStorage::new(), transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    let raft = ArbitroRaft::new(node);

    let f2 = tokio::spawn(run_follower_sim(l2, addr1, PeerId(2)));
    let f3 = tokio::spawn(run_follower_sim(l3, addr1, PeerId(3)));

    tokio::time::sleep(Duration::from_millis(15)).await;

    BenchCluster {
        raft,
        _tasks: vec![leader_listener_task, f2, f3],
    }
}

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// 1. Latency
// ---------------------------------------------------------------------------

fn bench_tcp_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_latency");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));

    let rt = make_runtime();

    group.throughput(Throughput::Elements(1));
    group.bench_function("propose_once/empty", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut cluster = make_cluster().await;
            let start = Instant::now();
            let empty = [];
            for _ in 0..iters {
                cluster.raft.propose_once(&empty).await.unwrap();
            }
            start.elapsed()
        });
    });

    group.bench_function("propose_once/1kb", |b| {
        let payload = vec![0xAA; 1024];
        b.to_async(&rt).iter_custom(move |iters| {
            let p = payload.clone();
            async move {
                let mut cluster = make_cluster().await;
                let start = Instant::now();
                for _ in 0..iters {
                    cluster.raft.propose_once(&p).await.unwrap();
                }
                start.elapsed()
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Batch throughput
// ---------------------------------------------------------------------------

fn bench_tcp_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_batch_throughput");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime();

    for &batch in &[1u64, 64, 256, 1024] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("empty/clients_{batch}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut cluster = make_cluster().await;
                let empty = [];
                let batch_payload: Vec<&[u8]> = vec![&empty; batch as usize];
                let start = Instant::now();
                for _ in 0..iters {
                    cluster
                        .raft
                        .propose_batch_once(&batch_payload)
                        .await
                        .unwrap();
                }
                start.elapsed()
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Concurrent writes
// ---------------------------------------------------------------------------

fn bench_tcp_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_concurrent_writes");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime();

    for &num_clients in &[1usize, 4, 16, 64] {
        group.throughput(Throughput::Elements(num_clients as u64));
        group.bench_function(format!("empty/clients_{num_clients}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let cluster = make_cluster().await;
                let mut raft = cluster.raft;
                let handle = raft.client_handle();

                let raft_task = tokio::spawn(async move {
                    let _ = raft.run().await;
                });

                let start = Instant::now();

                let client_tasks: Vec<_> = (0..num_clients)
                    .map(|_| {
                        let h = handle.clone();
                        tokio::spawn(async move {
                            let empty = [];
                            for _ in 0..iters {
                                h.write(&empty).await.unwrap();
                            }
                        })
                    })
                    .collect();

                for t in client_tasks {
                    t.await.unwrap();
                }
                let elapsed = start.elapsed();
                raft_task.abort();
                elapsed
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_tcp_latency,
    bench_tcp_batch_throughput,
    bench_tcp_concurrent_writes
);
criterion_main!(benches);
