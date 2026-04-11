// ARBITRO RAFT — IN-MEMORY CONSENSUS BENCHMARK
//
// Measures pure consensus overhead: no real network, no disk IO.
// Transport simulates 2 real follower state machines (prev_log check,
// conflict truncation, entry append) — equivalent to OpenRaft's MemLogStore.
//
// Design principles vs the previous version:
//   1. Uses propose_once / propose_batch_once — real backpressure, blocks until quorum.
//   2. Leader is created once per benchmark run (inside iter_custom), not per sample.
//   3. No yield_now() spin loop — propose_once returns on commit, no polling needed.
//   4. Batch scenarios simulate N concurrent clients by proposing N entries in one round.
//   5. Empty-payload baseline for direct comparison with openraft's minimal benchmark.

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
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;

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

        let start_idx = (from.0.saturating_sub(base)) as usize;
        let end_idx = (to.0.saturating_sub(base)) as usize;
        let end_idx = end_idx.min(entries.len());

        let mut offset = 0;
        if start_idx < end_idx {
            for e in &entries[start_idx..end_idx] {
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
    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> {
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
// MemTransport — 2 real follower state machines that process AppendEntries.
// ---------------------------------------------------------------------------

struct MemTransport {
    tx: UnboundedSender<Vec<u8>>,
    rx: Mutex<UnboundedReceiver<Vec<u8>>>,
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

impl RaftTransport for MemTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new(); // benchmark mock allows some allocation, but node doesn't
        for s in slices {
            frame.extend_from_slice(s);
        }

        let result = (|| -> Result<(), RaftError> {
            let inbound = decode_message(&frame)?;
            if let Some(ae) = inbound.as_append_entries() {
                let term = ae.term();

                let (success, match_index) = match self.peer_state(peer) {
                    Some(state) => state.lock().unwrap().process_append(&ae),
                    None => (false, LogIndex(0)),
                };

                let resp = AppendEntriesResp {
                    term: term.0.into(),
                    success: if success { 1 } else { 0 },
                    match_index: match_index.0.into(),
                    _pad: [0; 7],
                };

                let mut header_buf = [0u8; 128];
                let mut vectors = Vec::new();
                encode_message_vectored(
                    peer,
                    &RaftMessage::AppendEntriesResp(&resp),
                    &mut header_buf,
                    &mut vectors,
                )?;

                let mut resp_frame = Vec::new();
                for v in vectors {
                    resp_frame.extend_from_slice(v);
                }
                let _ = self.tx.unbounded_send(resp_frame);
            }
            Ok(())
        })();

        async move { result }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        // We use a manual capture of the mut buffer lifetime by wrapping in a future
        // Note: For benchmarks, copying to 'out' is standard.
        async move {
            loop {
                if let Ok(frame) = self.rx.lock().unwrap().try_recv() {
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
        async move {
            if timeout.is_zero() {
                if let Ok(frame) = self.rx.lock().unwrap().try_recv() {
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
                if let Ok(frame) = self.rx.lock().unwrap().try_recv() {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_config(node_id: PeerId) -> NodeConfig {
    NodeConfig {
        node_id,
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
        bootstrap_peers: vec![
            BootstrapPeer {
                id: PeerId(1),
                addr: "127.0.0.1:8001".parse().unwrap(),
            },
            BootstrapPeer {
                id: PeerId(2),
                addr: "127.0.0.1:8002".parse().unwrap(),
            },
            BootstrapPeer {
                id: PeerId(3),
                addr: "127.0.0.1:8003".parse().unwrap(),
            },
        ],
        ..Default::default()
    }
}

fn make_leader() -> ArbitroRaft<MemStorage, MemTransport> {
    let storage = MemStorage::new();
    let transport = MemTransport::new();
    let mut node = arbitro_raft::RaftNode::new(make_config(PeerId(1)), storage, transport).unwrap();
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
// 1. Latency benchmark
// ---------------------------------------------------------------------------

fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_latency");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));

    let rt = make_runtime(4);

    group.throughput(Throughput::Elements(1));
    group.bench_function("propose_once/empty", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut raft = make_leader();
            let start = Instant::now();
            let empty = [];
            for _ in 0..iters {
                raft.propose_once(&empty).await.unwrap();
            }
            start.elapsed()
        });
    });

    group.bench_function("propose_once/1kb", |b| {
        let payload = vec![0xAA; 1024];
        b.to_async(&rt).iter_custom(move |iters| {
            let p = payload.clone();
            async move {
                let mut raft = make_leader();
                let start = Instant::now();
                for _ in 0..iters {
                    raft.propose_once(&p).await.unwrap();
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

fn bench_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_batch_throughput");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime(4);

    for &batch in &[1u64, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("empty/clients_{batch}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut raft = make_leader();
                let empty = [];
                let batch_payload: Vec<&[u8]> = vec![&empty; batch as usize];
                let start = Instant::now();
                for _ in 0..iters {
                    raft.propose_batch_once(&batch_payload).await.unwrap();
                }
                start.elapsed()
            });
        });
    }

    for &batch in &[1u64, 4096] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("1kb/clients_{batch}"), |b| {
            let p = vec![0xAA; 1024];
            b.to_async(&rt).iter_custom(move |iters| {
                let p = p.clone();
                async move {
                    let mut raft = make_leader();
                    let batch_payload: Vec<&[u8]> = vec![&p; batch as usize];
                    let start = Instant::now();
                    for _ in 0..iters {
                        raft.propose_batch_once(&batch_payload).await.unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_batch_write");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime(4);

    let total_entries: u64 = 4096 * 4;
    group.throughput(Throughput::Elements(total_entries));
    group.bench_function("empty/clients_4096_batch_4", |b| {
        b.to_async(&rt).iter_custom(move |iters| async move {
            let mut raft = make_leader();
            let empty = [];
            let batch_payload: Vec<&[u8]> = vec![&empty; total_entries as usize];
            let start = Instant::now();
            for _ in 0..iters {
                raft.propose_batch_once(&batch_payload).await.unwrap();
            }
            start.elapsed()
        });
    });

    group.finish();
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_concurrent_writes");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let rt = make_runtime(16);

    for &num_clients in &[1usize, 4, 16, 64, 256, 1024] {
        group.throughput(Throughput::Elements(num_clients as u64));
        group.bench_function(format!("empty/clients_{num_clients}"), |b| {
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut raft = make_leader();
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
    bench_latency,
    bench_batch_throughput,
    bench_batch_write,
    bench_concurrent_writes
);
criterion_main!(benches);
