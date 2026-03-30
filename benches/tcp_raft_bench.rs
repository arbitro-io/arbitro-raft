// ARBITRO RAFT — TCP E2E BENCHMARK
//
// Measures consensus overhead over real TCP loopback (127.0.0.1).
// Followers are simulated state machines (same PeerState as memory bench)
// running as independent tokio tasks connected via real TCP sockets.
//
// Structure mirrors memory_e2e_bench.rs so results are directly comparable:
//   1. raft_tcp_latency          — propose_once, single write per round-trip
//   2. raft_tcp_batch_throughput — propose_batch_once, N entries per round
//   3. raft_tcp_concurrent_writes — N tokio tasks via ClientHandle::write()
//
// Transport design:
//   - Leader: TcpTransport with pre-established OwnedWriteHalf per peer,
//     std::sync::Mutex + try_recv on inbound_rx (same as memory bench hot path)
//   - Followers: standalone async tasks — accept frames, run PeerState::process_append,
//     send AppendEntriesResp back to leader via a pre-established TCP connection

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{
    decode_message_view, encode_message, AppendEntriesResp, ArbitroRaft, BootstrapPeer, ClusterId,
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftMessageView,
    RaftStorage, RaftTransport, SnapshotMeta, Term,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::channel::mpsc::{self, UnboundedReceiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// MemStorage — O(1) index lookup via base_index offset arithmetic.
// Identical to memory_e2e_bench.rs.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MemStorage {
    base_index: Arc<AtomicU64>,
    entries:    Arc<Mutex<Vec<LogEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            base_index: Arc::new(AtomicU64::new(0)),
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
        if new_entries.is_empty() { return Ok(()); }
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            self.base_index.store(new_entries[0].index.0, Ordering::Relaxed);
        }
        entries.extend_from_slice(new_entries);
        Ok(())
    }
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError> {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() { return Ok(()); }
        let base  = self.base_index.load(Ordering::Relaxed);
        let start = (from.0.saturating_sub(base)) as usize;
        let end   = (to.0.saturating_sub(base)) as usize;
        let end   = end.min(entries.len());
        if start < end { out.extend_from_slice(&entries[start..end]); }
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() { return Ok(()); }
        let base = self.base_index.load(Ordering::Relaxed);
        let cut  = (from.0.saturating_sub(base)) as usize;
        entries.truncate(cut);
        if entries.is_empty() { self.base_index.store(0, Ordering::Relaxed); }
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> { Ok(None) }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries.last().map(|e| (e.index, e.term)).unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() { return Ok(None); }
        let base = self.base_index.load(Ordering::Relaxed);
        if index.0 < base { return Ok(None); }
        let pos = (index.0 - base) as usize;
        Ok(entries.get(pos).cloned())
    }
}

// ---------------------------------------------------------------------------
// PeerState — real follower log for one simulated peer.
// Identical to memory_e2e_bench.rs: O(1) index lookup via base_index.
// ---------------------------------------------------------------------------

struct PeerState {
    base_index: u64,
    log: Vec<(LogIndex, Term)>,
}

impl PeerState {
    fn new() -> Self { Self { base_index: 0, log: Vec::new() } }

    fn last_position(&self) -> (LogIndex, Term) {
        self.log.last().copied().unwrap_or((LogIndex(0), Term(0)))
    }

    fn term_at(&self, idx: LogIndex) -> Option<Term> {
        if self.log.is_empty() || idx.0 < self.base_index { return None; }
        let pos = (idx.0 - self.base_index) as usize;
        self.log.get(pos).filter(|&&(i, _)| i == idx).map(|&(_, t)| t)
    }

    fn process_append(&mut self, ae: &arbitro_raft::AppendEntriesView) -> (bool, LogIndex) {
        let prev_idx  = ae.prev_log_index();
        let prev_term = ae.prev_log_term();

        if prev_idx.0 > 0 {
            match self.term_at(prev_idx) {
                None    => return (false, self.last_position().0),
                Some(t) if t != prev_term => return (false, self.last_position().0),
                _ => {}
            }
        }

        if let Ok(iter) = ae.entries() {
            for ev in iter {
                let idx  = ev.index();
                let term = ev.term();
                if self.log.is_empty() {
                    self.base_index = idx.0;
                    self.log.push((idx, term));
                    continue;
                }
                if idx.0 < self.base_index { continue; }
                let pos = (idx.0 - self.base_index) as usize;
                if pos < self.log.len() {
                    let (_, et) = self.log[pos];
                    if et == term { continue; }
                    self.log.truncate(pos);
                    if self.log.is_empty() { self.base_index = idx.0; }
                }
                self.log.push((idx, term));
            }
        }

        (true, self.last_position().0)
    }
}

// ---------------------------------------------------------------------------
// TcpTransport — for the leader node only.
//
// Inbound (responses from followers):
//   std::sync::Mutex<UnboundedReceiver<Bytes>> — hot path is try_recv with
//   a single CAS, no async scheduler involvement.
//
// Outbound (AppendEntries to followers):
//   Lazy connection per peer — established on first send_frame and cached.
//   Avoids split-half issues; set_nodelay(true) prevents Nagle coalescing.
//
// recv_frame_timeout (non-zero): polls with yield_now() instead of sleep.
//   On Windows, tokio::time::sleep has 15ms resolution, making sleep(50µs)
//   actually sleep(15ms). yield_now() is timer-free and lets follower tasks
//   run between polls so responses arrive in < 1 yield cycle.
// ---------------------------------------------------------------------------

struct TcpTransport {
    inbound_rx:   Mutex<UnboundedReceiver<Bytes>>,
    peer_addrs:   HashMap<PeerId, SocketAddr>,
    connections:  tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>,
}

impl TcpTransport {
    fn new(rx: UnboundedReceiver<Bytes>, peer_addrs: HashMap<PeerId, SocketAddr>) -> Self {
        Self {
            inbound_rx:  Mutex::new(rx),
            peer_addrs,
            connections: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RaftTransport for TcpTransport {
    async fn send_frame(&self, peer: PeerId, frame: Bytes) -> Result<(), RaftError> {
        let addr = *self.peer_addrs.get(&peer)
            .ok_or_else(|| RaftError::Transport(format!("unknown peer {:?}", peer)))?;

        // Lazy-init: connect on first send, cache for subsequent sends.
        let stream = {
            let mut conns = self.connections.lock().await;
            if let Some(s) = conns.get(&peer) {
                s.clone()
            } else {
                let s = TcpStream::connect(addr).await
                    .map_err(|e| RaftError::Transport(e.to_string()))?;
                s.set_nodelay(true).ok();
                let s = Arc::new(tokio::sync::Mutex::new(s));
                conns.insert(peer, s.clone());
                s
            }
        };

        let mut s = stream.lock().await;
        s.write_all(&frame).await.map_err(|e| RaftError::Transport(e.to_string()))
    }

    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        loop {
            if let Ok(frame) = self.inbound_rx.lock().unwrap().try_recv() {
                return Ok(frame);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn recv_frame_timeout(&self, timeout: Duration) -> Result<Option<Bytes>, RaftError> {
        if timeout.is_zero() {
            return Ok(self.inbound_rx.lock().unwrap().try_recv().ok());
        }
        // yield_now() instead of sleep — lets follower tasks push response frames
        // without being subject to OS timer resolution (15ms on Windows).
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(frame) = self.inbound_rx.lock().unwrap().try_recv() {
                return Ok(Some(frame));
            }
            if Instant::now() >= deadline { return Ok(None); }
            tokio::task::yield_now().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Frame framing — reads complete arbitro-raft frames from a TCP stream.
//
// RaftFrameHeader layout (#[repr(C)], no padding):
//   [0..4]   magic    (U32 LE)
//   [4]      version  (u8)
//   [5]      kind     (u8)
//   [6..8]   flags    (U16 LE)
//   [8..16]  from     (U64 LE)
//   [16..20] body_len (U32 LE)
//   [20..24] reserved (U32 LE)
//   total header = 24 bytes, then body_len bytes of body
// ---------------------------------------------------------------------------

const HEADER_SIZE: usize = 24;
const BODY_LEN_OFFSET: usize = 16;

async fn read_frame(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Bytes> {
    loop {
        if buf.len() >= HEADER_SIZE {
            let body_len = u32::from_le_bytes(
                buf[BODY_LEN_OFFSET..BODY_LEN_OFFSET + 4].try_into().unwrap()
            ) as usize;
            let total = HEADER_SIZE + body_len;
            if buf.len() >= total {
                return Some(buf.split_to(total).freeze());
            }
        }
        if stream.read_buf(buf).await.unwrap_or(0) == 0 {
            return None; // connection closed
        }
    }
}

// ---------------------------------------------------------------------------
// Follower simulation task.
//
// Accepts one connection from the leader, processes AppendEntries via
// PeerState, sends AppendEntriesResp back to the leader.
// ---------------------------------------------------------------------------

async fn run_follower_sim(
    listener:    TcpListener,
    leader_addr: SocketAddr,
    my_id:       PeerId,
) {
    // Connect back to leader for response frames.
    let mut leader_conn = TcpStream::connect(leader_addr).await.unwrap();
    // Accept the leader's AppendEntries connection.
    let (mut stream, _) = listener.accept().await.unwrap();

    let mut buf   = BytesMut::with_capacity(65536);
    let mut state = PeerState::new();

    while let Some(frame) = read_frame(&mut stream, &mut buf).await {
        let view = match decode_message_view(frame) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let RaftMessageView::AppendEntries(ae) = &view.message {
            let term = ae.term();
            let (success, match_index) = state.process_append(ae);
            let resp = RaftMessage::AppendEntriesResp(AppendEntriesResp { term, success, match_index });
            if let Ok(encoded) = encode_message(my_id, &resp) {
                let _ = leader_conn.write_all(&encoded).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster setup — builds a leader + 2 simulated followers over loopback TCP.
//
// Uses OS-assigned ports to avoid conflicts between bench iterations.
// Pre-establishes all connections so send_frame has zero connect overhead.
// ---------------------------------------------------------------------------

struct BenchCluster {
    raft:     ArbitroRaft<MemStorage, TcpTransport>,
    // Keep task handles alive; abort on drop via explicit .abort().
    _tasks:   Vec<JoinHandle<()>>,
}

async fn make_cluster() -> BenchCluster {
    // Bind all listeners on OS-assigned ports.
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let addr1 = l1.local_addr().unwrap();
    let addr2 = l2.local_addr().unwrap();
    let addr3 = l3.local_addr().unwrap();

    // Inbound channel for leader responses.
    let (inbound_tx, inbound_rx) = mpsc::unbounded::<Bytes>();

    // Spawn leader TCP listener — reads response frames and pushes to inbound_rx.
    let leader_listener_task = {
        let tx = inbound_tx.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = l1.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = BytesMut::with_capacity(65536);
                    while let Some(frame) = read_frame(&mut stream, &mut buf).await {
                        let _ = tx.unbounded_send(frame);
                    }
                });
            }
        })
    };

    // Build peer address map — connections are established lazily on first send_frame.
    let mut peer_addrs = HashMap::new();
    peer_addrs.insert(PeerId(2), addr2);
    peer_addrs.insert(PeerId(3), addr3);

    let transport = TcpTransport::new(inbound_rx, peer_addrs);

    // Build leader node.
    let config = NodeConfig {
        node_id:  PeerId(1),
        cluster_id: ClusterId(1),
        peers:    vec![PeerId(1), PeerId(2), PeerId(3)],
        bootstrap_peers: vec![
            BootstrapPeer { id: PeerId(1), addr: addr1 },
            BootstrapPeer { id: PeerId(2), addr: addr2 },
            BootstrapPeer { id: PeerId(3), addr: addr3 },
        ],
        ..Default::default()
    };
    let mut node = arbitro_raft::RaftNode::new(config, MemStorage::new(), transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    let raft = ArbitroRaft::new(node);

    // Spawn simulated followers (they connect back to addr1 for responses).
    let f2 = tokio::spawn(run_follower_sim(l2, addr1, PeerId(2)));
    let f3 = tokio::spawn(run_follower_sim(l3, addr1, PeerId(3)));

    // Give the followers a moment to connect back to the leader listener.
    tokio::time::sleep(Duration::from_millis(5)).await;

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
// 1. Latency — single propose_once per round-trip over real TCP
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
            for _ in 0..iters {
                cluster.raft.propose_once(Bytes::new()).await.unwrap();
            }
            start.elapsed()
        });
    });

    group.bench_function("propose_once/1kb", |b| {
        let payload = Bytes::from(vec![0xAA; 1024]);
        b.to_async(&rt).iter_custom(move |iters| {
            let p = payload.clone();
            async move {
                let mut cluster = make_cluster().await;
                let start = Instant::now();
                for _ in 0..iters {
                    cluster.raft.propose_once(p.clone()).await.unwrap();
                }
                start.elapsed()
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Batch throughput — propose_batch_once(N entries per round)
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
                let batch_payload = vec![Bytes::new(); batch as usize];
                let start = Instant::now();
                for _ in 0..iters {
                    cluster.raft.propose_batch_once(batch_payload.clone()).await.unwrap();
                }
                start.elapsed()
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Concurrent writes — N real tokio tasks via ClientHandle::write()
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
                let mut raft  = cluster.raft;
                let handle    = raft.client_handle();

                let raft_task = tokio::spawn(async move { let _ = raft.run().await; });

                let start = Instant::now();

                let client_tasks: Vec<_> = (0..num_clients)
                    .map(|_| {
                        let h = handle.clone();
                        tokio::spawn(async move {
                            for _ in 0..iters { h.write(Bytes::new()).await.unwrap(); }
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

criterion_group!(benches, bench_tcp_latency, bench_tcp_batch_throughput, bench_tcp_concurrent_writes);
criterion_main!(benches);
