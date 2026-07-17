// ARBITRO RAFT — TCP E2E BENCHMARK
//
// Measures consensus overhead over real TCP loopback (127.0.0.1).
// Followers are simulated state machines (same PeerState as memory bench)
// running as independent tokio tasks connected via real TCP sockets.

use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::protocol::{
    AppendEntries, AppendEntriesEntryIter, AppendEntriesResp, EntryHeader, RaftMessage,
};
use arbitro_raft::{
    decode_message, encode_message_vectored, ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload,
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport,
    SnapshotMeta, StateMachine, Term,
};

/// Bench-local no-op StateMachine — apply is a no-op; snapshot/restore return empty.
struct NoopSM;
impl StateMachine for NoopSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> { Ok(Vec::new()) }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> { Ok(()) }
}
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::channel::mpsc::{self, UnboundedReceiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use zerocopy::IntoBytes;

/// ---------------------------------------------------------------------------
// SeededMemStorage — Pure Zero-Copy Dual Arena for TCP benchmark.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SeededMemStorage {
    base_index: Arc<AtomicU64>,
    headers: Arc<Mutex<Vec<EntryHeader>>>,
    data: Arc<Mutex<Vec<u8>>>,
    payload_offsets: Arc<Mutex<Vec<(usize, usize)>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl SeededMemStorage {
    fn new() -> Self {
        Self {
            base_index: Arc::new(AtomicU64::new(0)),
            headers: Arc::new(Mutex::new(Vec::new())),
            data: Arc::new(Mutex::new(Vec::new())),
            payload_offsets: Arc::new(Mutex::new(Vec::new())),
            hard_state: Arc::new(Mutex::new(HardState {
                current_term: Term(1),
                voted_for: None,
            })),
        }
    }
}

impl RaftStorage for SeededMemStorage {
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
        let mut headers = self.headers.lock().unwrap();
        let mut data = self.data.lock().unwrap();
        let mut offsets = self.payload_offsets.lock().unwrap();

        if headers.is_empty() {
            self.base_index
                .store(new_entries[0].index.0, Ordering::Relaxed);
        }

        for e in new_entries {
            let offset = data.len();
            data.extend_from_slice(e.payload.0);
            let len = e.payload.0.len();
            offsets.push((offset, len));
            headers.push(EntryHeader {
                term: e.term.0.into(),
                index: e.index.0.into(),
                payload_len: (len as u32).into(),
                _pad: 0.into(),
            });
        }
        Ok(())
    }

    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        _payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let headers = self.headers.lock().unwrap();
        let data = self.data.lock().unwrap();
        let offsets = self.payload_offsets.lock().unwrap();
        if headers.is_empty() {
            return Ok(0);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let start = (from.0.saturating_sub(base)) as usize;
        let end = (to.0.saturating_sub(base)) as usize;
        let end = end.min(headers.len());

        let mut bytes_read = 0;
        for i in start..end {
            let h = &headers[i];
            let (off, len) = offsets[i];
            let payload = unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&data[off..off + len]) };
            out.push(LogEntry {
                term: Term(h.term.get()),
                index: LogIndex(h.index.get()),
                payload: EntryPayload(payload),
            });
            bytes_read += len;
        }
        Ok(bytes_read)
    }

    fn read_entry_headers(
        &self,
        from: LogIndex,
        max: LogIndex,
    ) -> Result<Option<&[EntryHeader]>, RaftError> {
        let headers = self.headers.lock().unwrap();
        if headers.is_empty() {
            return Ok(None);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let s = (from.0.saturating_sub(base)) as usize;
        if s >= headers.len() {
            return Ok(None);
        }

        let mut e = (max.0.saturating_sub(base)) as usize + 1;
        e = e.min(headers.len());

        // SAFETY: Benchmark-only stable pointer trick.
        if s < e {
            let slice = &headers[s..e];
            Ok(Some(unsafe {
                std::mem::transmute::<&[EntryHeader], &'static [EntryHeader]>(slice)
            }))
        } else {
            Ok(None)
        }
    }

    fn for_each_payload<'a>(
        &'a self,
        from: LogIndex,
        to: LogIndex,
        callback: &mut dyn FnMut(&'a [u8]),
    ) -> Result<(), RaftError> {
        let offsets = self.payload_offsets.lock().unwrap();
        let data = self.data.lock().unwrap();
        let base = self.base_index.load(Ordering::Relaxed);

        let start = (from.0.saturating_sub(base)) as usize;
        let end = (to.0.saturating_sub(base)) as usize + 1;
        let end = end.min(offsets.len());

        for i in start..end {
            let (off, len) = offsets[i];
            // SAFETY: Benchmark-only stable pointer trick, same as
            // `read_entry_headers` above — the data buffer only grows and is
            // never truncated while the node borrows the storage.
            let payload =
                unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&data[off..off + len]) };
            callback(payload);
        }
        Ok(())
    }

    fn append_entries_seeded(
        &self,
        headers: &[EntryHeader],
        payloads: &[&[u8]],
    ) -> Result<(), RaftError> {
        let mut h_lock = self.headers.lock().unwrap();
        let mut d_lock = self.data.lock().unwrap();
        let mut o_lock = self.payload_offsets.lock().unwrap();

        if h_lock.is_empty() && !headers.is_empty() {
            self.base_index
                .store(headers[0].index.get(), Ordering::Relaxed);
        }

        for (h, p) in headers.iter().zip(payloads.iter()) {
            let off = d_lock.len();
            d_lock.extend_from_slice(p);
            h_lock.push(*h);
            o_lock.push((off, p.len()));
        }
        Ok(())
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut headers = self.headers.lock().unwrap();
        let mut offsets = self.payload_offsets.lock().unwrap();
        let mut data = self.data.lock().unwrap();
        if headers.is_empty() {
            return Ok(());
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let cut = (from.0.saturating_sub(base)) as usize;
        if cut < headers.len() {
            let (data_cut, _) = offsets[cut];
            headers.truncate(cut);
            offsets.truncate(cut);
            data.truncate(data_cut);
            if headers.is_empty() {
                self.base_index.store(0, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let headers = self.headers.lock().unwrap();
        Ok(headers
            .last()
            .map(|h| (LogIndex(h.index.get()), Term(h.term.get())))
            .unwrap_or((LogIndex(0), Term(0))))
    }

    fn entry_at<'a>(
        &self,
        index: LogIndex,
        _payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let headers = self.headers.lock().unwrap();
        let data = self.data.lock().unwrap();
        let offsets = self.payload_offsets.lock().unwrap();
        if headers.is_empty() {
            return Ok(None);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        if index.0 < base {
            return Ok(None);
        }
        let pos = (index.0 - base) as usize;
        if let Some(h) = headers.get(pos) {
            let (off, len) = offsets[pos];
            let payload = unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&data[off..off + len]) };
            Ok(Some(LogEntry {
                term: Term(h.term.get()),
                index: LogIndex(h.index.get()),
                payload: EntryPayload(payload),
            }))
        } else {
            Ok(None)
        }
    }

    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
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

    fn process_append(&mut self, ae: &AppendEntries, body: &[u8]) -> (bool, LogIndex) {
        let prev_idx = LogIndex(ae.prev_log_index.get());
        let prev_term = Term(ae.prev_log_term.get());

        if prev_idx.0 > 0 {
            match self.term_at(prev_idx) {
                None => return (false, self.last_position().0),
                Some(t) if t != prev_term => return (false, self.last_position().0),
                _ => {}
            }
        }

        let iter = AppendEntriesEntryIter::new(body, ae.entry_count.get() as usize);
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

        // Zero-Allocation: We don't clone the data. We take advantage of the fact
        // that RaftNode ensures the storage/scratchpad lives until the future completes.
        // For the benchmark, we'll use unsafe transmute to 'static to satisfy Send
        // since we know the cluster lifecycle is controlled.
        let slices_static =
            unsafe { std::mem::transmute::<&[&[u8]], &'static [&'static [u8]]>(slices) };

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

            // True vectored write: one writev syscall covers all iovecs
            // (header + payloads). Previous impl did N×write_all which
            // destroyed the point of vectored fan-out.
            //
            // Partial writes are rare on loopback for these sizes but handled
            // correctly via IoSlice::advance_slices.
            let mut io_bufs: Vec<IoSlice<'_>> =
                slices_static.iter().map(|s| IoSlice::new(s)).collect();
            let mut bufs: &mut [IoSlice<'_>] = &mut io_bufs;
            while !bufs.is_empty() {
                let n = s
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| RaftError::Transport(e.to_string()))?;
                if n == 0 {
                    return Err(RaftError::Transport(
                        "write_vectored returned 0 — peer closed".into(),
                    ));
                }
                IoSlice::advance_slices(&mut bufs, n);
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
        match inbound.message {
            RaftMessage::AppendEntries(ae, body) => {
                let (success, match_index) = state.process_append(ae, body);
                let resp = AppendEntriesResp {
                    term: ae.term.get().into(),
                    match_index: match_index.0.into(),
                    success: if success { 1 } else { 0 },
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
            RaftMessage::AppendEntriesSeeded {
                ae,
                headers,
                payloads,
            } => {
                // For Seeded, we use the AppendEntriesRawIter to simulate storage append
                let iter = arbitro_raft::protocol::AppendEntriesRawIter::new(
                    headers,
                    arbitro_raft::protocol::SeededPayloads::Contiguous(payloads),
                );

                // Simulate follower log update (PeerState only needs index/term for benchmark)
                let mut success = true;
                let prev_idx = LogIndex(ae.prev_log_index.get());
                let prev_term = Term(ae.prev_log_term.get());

                if prev_idx.0 > 0 {
                    match state.term_at(prev_idx) {
                        None => success = false,
                        Some(t) if t != prev_term => success = false,
                        _ => {}
                    }
                }

                if success {
                    for (h, _) in iter {
                        state
                            .log
                            .push((LogIndex(h.index.get()), Term(h.term.get())));
                    }
                }

                let match_index = state.last_position().0;
                let resp = AppendEntriesResp {
                    term: ae.term.get().into(),
                    match_index: match_index.0.into(),
                    success: if success { 1 } else { 0 },
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
            _ => {}
        }
    }
}

struct BenchCluster {
    raft: ArbitroRaft<SeededMemStorage, TcpTransport, NoopSM>,
    storage: SeededMemStorage,
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
        learners: Vec::new(),
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
    let storage = SeededMemStorage::new();
    let node_storage = storage.clone();
    let mut node = arbitro_raft::RaftNode::new(config, node_storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    let raft = ArbitroRaft::new(node, NoopSM);

    let f2 = tokio::spawn(run_follower_sim(l2, addr1, PeerId(2)));
    let f3 = tokio::spawn(run_follower_sim(l3, addr1, PeerId(3)));

    tokio::time::sleep(Duration::from_millis(15)).await;

    BenchCluster {
        raft,
        storage,
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

// ---------------------------------------------------------------------------
// 4. Seeded vs Ergo comparison
// ---------------------------------------------------------------------------

fn bench_tcp_seeded_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_seeded_vs_ergo");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime();

    for &batch in &[1u64, 64, 256, 1024] {
        group.throughput(Throughput::Elements(batch));

        // CASE 1: ERGO (Standard Path)
        group.bench_function(format!("ergo/batch_{batch}"), |b| {
            let payload = vec![0xAA; 4096];
            b.to_async(&rt).iter_custom(|iters| {
                let p = payload.clone();
                async move {
                    let mut cluster = make_cluster().await;
                    let batch_payload: Vec<&[u8]> = vec![&p; batch as usize];
                    let start = Instant::now();
                    for _ in 0..iters {
                        cluster
                            .raft
                            .propose_batch_once(&batch_payload)
                            .await
                            .unwrap();
                    }
                    start.elapsed()
                }
            });
        });

        // CASE 2: SEEDED (Optimized Path)
        group.bench_function(format!("seeded/batch_{batch}"), |b| {
            let payload = vec![0xAA; 4096];
            b.to_async(&rt).iter_custom(|iters| {
                let p = payload.clone();
                async move {
                    let mut cluster = make_cluster().await;

                    // Pre-fill storage to ensure Seeded path is primed
                    let mut initial_entries = Vec::new();
                    for i in 0..batch {
                        initial_entries.push(LogEntry {
                            term: Term(1),
                            index: LogIndex(i + 1),
                            payload: EntryPayload(&p),
                        });
                    }
                    cluster.storage.append_entries(&initial_entries).unwrap();

                    let batch_payload: Vec<&[u8]> = vec![&p; batch as usize];
                    let start = Instant::now();
                    for _ in 0..iters {
                        cluster
                            .raft
                            .propose_batch_once(&batch_payload)
                            .await
                            .unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_tcp_latency,
    bench_tcp_batch_throughput,
    bench_tcp_concurrent_writes,
    bench_tcp_seeded_comparison
);
criterion_main!(benches);
