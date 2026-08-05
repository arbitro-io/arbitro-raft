// ARBITRO RAFT — FIXED-N IN-MEMORY CONSENSUS ROUND-TRIP PROBE
//
// Commits exactly N entries through a REAL 3-node in-memory quorum and prints
// plain numbers — no Criterion machinery. Each commit is a full consensus
// round: propose -> encode (real wire codec) -> tokio MPSC channel to REAL
// follower tasks (cross-task scheduling, real wakeups) -> decode -> prev_log
// check + append -> encode ack -> channel back -> quorum gather -> commit.
// `propose_once` blocks until the entry actually commits under quorum
// (returns NoQuorum otherwise), so nothing enqueue-only is counted.
//
// This is the SAME honest in-memory harness as `benches/raft_transport_bench.rs`
// (inmem variant), minus TCP and minus Criterion. Sequential wait-per-commit —
// the number is single-commit round-trip latency, not pipelined throughput.
//
// Usage:
//   cargo run --release --example inmem_throughput -- [N] [PAYLOAD_BYTES]
//     N             number of commits (default 1000)
//     PAYLOAD_BYTES payload size per entry (default 0)
//
// Output (one line):
//   inmem_roundtrip N=<n> payload=<b>B elapsed=<s>s ops_sec=<r> us_per_commit=<u>

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::protocol::{
    AppendEntries, AppendEntriesEntryIter, AppendEntriesRawIter, AppendEntriesResp, RaftMessage,
    SeededPayloads,
};
use arbitro_raft::{
    decode_message, encode_message_vectored, ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload,
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, StateMachine, Term,
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// NoopSM — apply is a no-op; numbers exclude state-machine apply cost.
// ---------------------------------------------------------------------------

struct NoopSM;
impl StateMachine for NoopSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MemStorage — Vec-backed leader storage. Same as the bench harness: no
// unsafe, no 'static transmutes; payload_buf carved with split_at_mut.
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
        let end = ((to.0.saturating_sub(base)) as usize).min(entries.len());

        let mut written = 0usize;
        let mut buf: &'a mut [u8] = payload_buf;
        if start < end {
            for e in &entries[start..end] {
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                let chunk: &'a [u8] = chunk;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                buf = rest;
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
        if entries.is_empty() {
            return Ok(None);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        if index.0 < base {
            return Ok(None);
        }
        let pos = (index.0 - base) as usize;
        let Some(e) = entries.get(pos) else {
            return Ok(None);
        };
        let len = e.payload.len();
        if payload_buf.len() < len {
            return Err(RaftError::Storage("payload_buf too small".into()));
        }
        payload_buf[..len].copy_from_slice(&e.payload);
        Ok(Some(LogEntry {
            term: e.term,
            index: e.index,
            payload: EntryPayload(&payload_buf[..len]),
        }))
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((LogIndex(0), Term(0))))
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
}

// ---------------------------------------------------------------------------
// Real follower model — decode, prev_log check, conflict truncation, append,
// encode ack. Runs inside a REAL tokio task blocking on its channel.
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

fn encode_resp_frame(my_id: PeerId, term: u64, success: bool, match_index: u64) -> Vec<u8> {
    let resp = AppendEntriesResp {
        term: term.into(),
        success: if success { 1 } else { 0 },
        match_index: match_index.into(),
        _pad: [0; 7],
    };
    let mut header_buf = [0u8; 128];
    let mut vectors = Vec::new();
    encode_message_vectored(
        my_id,
        &RaftMessage::AppendEntriesResp(&resp),
        &mut header_buf,
        &mut vectors,
    )
    .expect("encode AppendEntriesResp");
    let total: usize = vectors.iter().map(|v| v.len()).sum();
    let mut frame = Vec::with_capacity(total);
    for v in vectors {
        frame.extend_from_slice(v);
    }
    frame
}

fn follower_handle_frame(state: &mut PeerState, my_id: PeerId, frame: &[u8]) -> Option<Vec<u8>> {
    let inbound = decode_message(frame).ok()?;
    match inbound.message {
        RaftMessage::AppendEntries(ae, body) => {
            let (success, match_index) = state.process_append(ae, body);
            Some(encode_resp_frame(
                my_id,
                ae.term.get(),
                success,
                match_index.0,
            ))
        }
        RaftMessage::AppendEntriesSeeded {
            ae,
            headers,
            payloads,
        } => {
            let iter = AppendEntriesRawIter::new(headers, SeededPayloads::Contiguous(payloads));

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
            Some(encode_resp_frame(
                my_id,
                ae.term.get(),
                success,
                match_index.0,
            ))
        }
        _ => None,
    }
}

/// In-memory follower: a REAL tokio task blocking on a tokio MPSC channel.
/// Every leader->follower frame crosses the tokio scheduler (task wakeup);
/// nothing is inline-synchronous.
async fn run_channel_follower(
    mut rx: UnboundedReceiver<Vec<u8>>,
    tx: UnboundedSender<Vec<u8>>,
    my_id: PeerId,
) {
    let mut state = PeerState::new();
    while let Some(frame) = rx.recv().await {
        if let Some(resp) = follower_handle_frame(&mut state, my_id, &frame) {
            if tx.send(resp).is_err() {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound + ChannelTransport — leader side. Real async recv, no spin loops.
// ---------------------------------------------------------------------------

struct Inbound {
    rx: tokio::sync::Mutex<UnboundedReceiver<Vec<u8>>>,
}

impl Inbound {
    fn new(rx: UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    fn copy_out(frame: &[u8], out: &mut [u8]) -> Result<usize, RaftError> {
        if out.len() < frame.len() {
            return Err(RaftError::Transport("recv buffer too small".into()));
        }
        out[..frame.len()].copy_from_slice(frame);
        Ok(frame.len())
    }

    async fn recv(&self, out: &mut [u8]) -> Result<usize, RaftError> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(frame) => Self::copy_out(&frame, out),
            None => Err(RaftError::Transport("inbound channel closed".into())),
        }
    }

    async fn recv_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> Result<Option<usize>, RaftError> {
        let mut rx = self.rx.lock().await;
        if timeout.is_zero() {
            return match rx.try_recv() {
                Ok(frame) => Self::copy_out(&frame, out).map(Some),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => {
                    Err(RaftError::Transport("inbound channel closed".into()))
                }
            };
        }
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(frame)) => Self::copy_out(&frame, out).map(Some),
            Ok(None) => Err(RaftError::Transport("inbound channel closed".into())),
            Err(_) => Ok(None),
        }
    }
}

struct ChannelTransport {
    inbound: Inbound,
    peers: HashMap<PeerId, UnboundedSender<Vec<u8>>>,
}

impl RaftTransport for ChannelTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let tx = self
                .peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            let total: usize = slices.iter().map(|s| s.len()).sum();
            let mut frame = Vec::with_capacity(total);
            for s in slices {
                frame.extend_from_slice(s);
            }
            tx.send(frame)
                .map_err(|_| RaftError::Transport("follower channel closed".into()))
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let tx = self
                .peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            tx.send(frame.to_vec())
                .map_err(|_| RaftError::Transport("follower channel closed".into()))
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { self.inbound.recv(out).await }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { self.inbound.recv_timeout(timeout, out).await }
    }
}

// ---------------------------------------------------------------------------
// Cluster construction — leader + 2 real follower tasks, quorum = 2.
// ---------------------------------------------------------------------------

struct InmemCluster {
    raft: ArbitroRaft<MemStorage, ChannelTransport, NoopSM>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for InmemCluster {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn make_config(addrs: [SocketAddr; 3]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
        learners: Vec::new(),
        bootstrap_peers: vec![
            BootstrapPeer {
                id: PeerId(1),
                addr: addrs[0],
            },
            BootstrapPeer {
                id: PeerId(2),
                addr: addrs[1],
            },
            BootstrapPeer {
                id: PeerId(3),
                addr: addrs[2],
            },
        ],
        ..Default::default()
    }
}

fn make_inmem_cluster() -> InmemCluster {
    let (inbound_tx, inbound_rx) = unbounded_channel::<Vec<u8>>();
    let mut peers = HashMap::new();
    let mut tasks = Vec::new();
    for id in [2u64, 3] {
        let (tx, rx) = unbounded_channel::<Vec<u8>>();
        peers.insert(PeerId(id), tx);
        tasks.push(tokio::spawn(run_channel_follower(
            rx,
            inbound_tx.clone(),
            PeerId(id),
        )));
    }
    drop(inbound_tx);

    let transport = ChannelTransport {
        inbound: Inbound::new(inbound_rx),
        peers,
    };
    let addrs: [SocketAddr; 3] = [
        "127.0.0.1:9101".parse().unwrap(),
        "127.0.0.1:9102".parse().unwrap(),
        "127.0.0.1:9103".parse().unwrap(),
    ];
    let storage = MemStorage::new();
    let mut node = RaftNode::new(make_config(addrs), storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    InmemCluster {
        raft: ArbitroRaft::new(node, NoopSM),
        tasks,
    }
}

// ---------------------------------------------------------------------------
// main — commit exactly N entries, print plain numbers.
// ---------------------------------------------------------------------------

fn main() {
    let mut args = std::env::args().skip(1);
    let n: u64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let payload_bytes: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(0);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let payload = vec![0xAAu8; payload_bytes];

    let elapsed = rt.block_on(async {
        let mut cluster = make_inmem_cluster();

        // Untimed warm-up: 10 commits so first-touch costs never skew the run.
        for _ in 0..10 {
            cluster
                .raft
                .propose_once(&payload)
                .await
                .expect("warm-up propose must reach quorum");
        }

        let start = Instant::now();
        for _ in 0..n {
            cluster
                .raft
                .propose_once(&payload)
                .await
                .expect("propose must reach quorum");
        }
        start.elapsed()
    });

    let secs = elapsed.as_secs_f64();
    let ops_sec = n as f64 / secs;
    let us_per_commit = secs * 1e6 / n as f64;
    println!(
        "inmem_roundtrip N={} payload={}B elapsed={:.4}s ops_sec={:.0} us_per_commit={:.2}",
        n, payload_bytes, secs, ops_sec, us_per_commit
    );
}
