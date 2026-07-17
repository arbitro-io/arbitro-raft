//! Shared in-memory harness for the A7 regression-pin suite
//! (`regression_election_safety.rs`, `regression_joint_consensus.rs`,
//! `regression_log_repair.rs`).
//!
//! Everything here is deliberately deterministic: storage is `Arc`-shared so
//! a "crash-restart" is `RaftNode::new` over the same storage (hard state and
//! log survive, volatile state resets — exactly Raft's restart contract), and
//! the transports either capture outbound frames for manual ferrying
//! ([`CaptureTransport`]), serve a pre-scripted inbound sequence one frame per
//! drain batch ([`ScriptedTransport`]), or expose an injectable inbound queue
//! ([`InjectTransport`]). The hub/driver plumbing mirrors the established
//! pattern in `tests/membership_change.rs` for live-cluster scenarios.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, NoopStateMachine, PeerId, RaftError, RaftStorage, RaftTransport,
    SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// TestStorage — in-memory RaftStorage, Arc-shared so it survives a "restart".
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StoredEntry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct TestStorage {
    pub hard_state: Arc<Mutex<Option<HardState>>>,
    pub entries: Arc<Mutex<Vec<StoredEntry>>>,
    /// When set, `save_hard_state` fails with `RaftError::Storage` — used to
    /// pin the persist-BEFORE-respond ordering of vote grants (C1).
    pub fail_save_hard_state: Arc<AtomicBool>,
}

impl TestStorage {
    /// Pre-seed one log entry (setup only — bypasses the engine on purpose).
    pub fn seed_entry(&self, index: u64, term: u64, payload: &[u8]) {
        self.entries.lock().unwrap().push(StoredEntry {
            term: Term(term),
            index: LogIndex(index),
            payload: payload.to_vec(),
        });
    }

    /// Pre-seed the durable hard state (setup only).
    pub fn seed_hard_state(&self, term: u64, voted_for: Option<u64>) {
        *self.hard_state.lock().unwrap() = Some(HardState {
            current_term: Term(term),
            voted_for: voted_for.map(PeerId),
        });
    }

    /// Snapshot of the log as comparable `(index, term, payload)` triples,
    /// sorted by index — the "byte-identical" comparison unit for C5.
    pub fn log_triples(&self) -> Vec<(u64, u64, Vec<u8>)> {
        let mut v: Vec<(u64, u64, Vec<u8>)> = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|e| (e.index.0, e.term.0, e.payload.clone()))
            .collect();
        v.sort_by_key(|(i, _, _)| *i);
        v
    }

    /// The persisted hard state as the engine would reload it.
    pub fn persisted_hard_state(&self) -> HardState {
        self.hard_state.lock().unwrap().clone().unwrap_or_default()
    }
}

impl RaftStorage for TestStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone().unwrap_or_default())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        if self.fail_save_hard_state.load(Ordering::SeqCst) {
            return Err(RaftError::Storage("injected hard-state save failure".into()));
        }
        *self.hard_state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
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
        let mut buf = payload_buf;
        let mut written = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                // Split the front chunk off the remaining buffer so the shared
                // ref pushed into `out` is never invalidated by a later write
                // through `buf` — no transmute, Stacked Borrows (Miri) clean.
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                buf = rest;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                written += len;
            }
        }
        Ok(written)
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if let Some(e) = entries.iter().rev().find(|e| e.index == index) {
            if payload_buf.len() < e.payload.len() {
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[..e.payload.len()].copy_from_slice(&e.payload);
            // Shared reborrow-for-return of the 'a buffer (borrow-checked).
            let static_payload: &'a [u8] = &payload_buf[..e.payload.len()];
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(static_payload),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Config helper.
// ---------------------------------------------------------------------------

pub fn make_config(node_id: u64, peers: &[u64]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9200 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_max_ms: 300,
        },
        limits: LimitsConfig::default(),
    }
}

// ---------------------------------------------------------------------------
// CaptureTransport — records every outbound frame; nothing inbound. Used for
// the manual message-ferry loops (C5) and the voter grant/restart pins (C1).
// ---------------------------------------------------------------------------

pub struct CaptureTransport {
    pub sent: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl CaptureTransport {
    pub fn new() -> (Self, Arc<Mutex<Vec<(u64, Vec<u8>)>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        (Self { sent: sent.clone() }, sent)
    }
}

/// Drain (and clear) all captured frames addressed to `peer`, in send order.
pub fn take_frames_for(sent: &Arc<Mutex<Vec<(u64, Vec<u8>)>>>, peer: u64) -> Vec<Vec<u8>> {
    let mut guard = sent.lock().unwrap();
    let mut taken = Vec::new();
    let mut kept = Vec::new();
    for (to, frame) in guard.drain(..) {
        if to == peer {
            taken.push(frame);
        } else {
            kept.push((to, frame));
        }
    }
    *guard = kept;
    taken
}

impl RaftTransport for CaptureTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.sent.lock().unwrap().push((peer.0, frame));
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.sent.lock().unwrap().push((peer.0, frame.to_vec()));
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { Err(RaftError::Transport("capture transport has no inbound".into())) }
    }
    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move {
            if !timeout.is_zero() {
                tokio::time::sleep(timeout).await;
            }
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// ScriptedTransport — serves a pre-scripted inbound frame sequence, exactly
// ONE frame per blocking recv (zero-timeout polls return None). This makes
// the campaign drain loop (`drain_inbound_frames`) process one message per
// batch, giving the test full deterministic control over the interleaving of
// vote grants and step-down triggers (C1). Outbound frames are recorded.
// ---------------------------------------------------------------------------

pub struct ScriptedTransport {
    pub queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    pub sent: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl ScriptedTransport {
    pub fn new(frames: Vec<Vec<u8>>) -> (Self, Arc<Mutex<VecDeque<Vec<u8>>>>) {
        let queue = Arc::new(Mutex::new(frames.into_iter().collect::<VecDeque<_>>()));
        (
            Self {
                queue: queue.clone(),
                sent: Arc::new(Mutex::new(Vec::new())),
            },
            queue,
        )
    }
}

impl RaftTransport for ScriptedTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.sent.lock().unwrap().push((peer.0, frame));
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.sent.lock().unwrap().push((peer.0, frame.to_vec()));
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let queue = self.queue.clone();
        async move {
            let frame = queue
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(RaftError::Transport("script exhausted".into()))?;
            if out.len() < frame.len() {
                return Err(RaftError::Transport("buffer too small".into()));
            }
            out[..frame.len()].copy_from_slice(&frame);
            Ok(frame.len())
        }
    }
    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let queue = self.queue.clone();
        async move {
            // Zero-timeout polls see nothing: one frame per drain batch.
            if timeout.is_zero() {
                return Ok(None);
            }
            let frame = queue.lock().unwrap().pop_front();
            match frame {
                Some(frame) => {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    Ok(Some(frame.len()))
                }
                None => {
                    tokio::time::sleep(timeout).await;
                    Ok(None)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// InjectTransport — injectable inbound queue; outbound frames are discarded.
// Used with `ArbitroRaft::run_once` for the joint-consensus commit pins (C2).
// ---------------------------------------------------------------------------

pub struct InjectTransport {
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>,
}

impl InjectTransport {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                rx: Arc::new(tokio::sync::Mutex::new(rx)),
            },
            tx,
        )
    }
}

impl RaftTransport for InjectTransport {
    fn send_vectored(
        &self,
        _peer: PeerId,
        _slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        _peer: PeerId,
        _frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            let frame = rx
                .recv()
                .await
                .ok_or(RaftError::Transport("closed".into()))?;
            if out.len() < frame.len() {
                return Err(RaftError::Transport("buffer too small".into()));
            }
            out[..frame.len()].copy_from_slice(&frame);
            Ok(frame.len())
        }
    }
    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if timeout.is_zero() {
                if let Ok(frame) = rx.try_recv() {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    return Ok(Some(frame.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(frame)) => {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    Ok(Some(frame.len()))
                }
                _ => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NetworkHub + RoutingTransport + driver plumbing for live-cluster scenarios
// (same shape as tests/membership_change.rs, plus storage-preserving restart).
// ---------------------------------------------------------------------------

pub struct AbortOnDrop {
    pub handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

pub struct NetworkHub {
    senders: Mutex<
        std::collections::HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>,
    >,
}

impl NetworkHub {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }

    pub fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        if let Some(sender) = self.senders.lock().unwrap().get(&to) {
            let _ = sender.send((from, data));
        }
    }
}

pub struct RoutingTransport {
    pub from: PeerId,
    pub hub: Arc<NetworkHub>,
    pub rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(PeerId, Vec<u8>)>>>,
}

impl RaftTransport for RoutingTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut data = Vec::new();
        for s in slices {
            data.extend_from_slice(s);
        }
        self.hub.send(self.from, peer, data);
        async move { Ok(()) }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.hub.send(self.from, peer, frame.to_vec());
        async move { Ok(()) }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if let Some((_from, data)) = rx.recv().await {
                if out.len() < data.len() {
                    return Err(RaftError::Transport("buffer too small".into()));
                }
                out[..data.len()].copy_from_slice(&data);
                Ok(data.len())
            } else {
                Err(RaftError::Transport("channel closed".into()))
            }
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if timeout.is_zero() {
                if let Ok((_sender, data)) = rx.try_recv() {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    return Ok(Some(data.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some((_sender, data))) => {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                _ => Ok(None),
            }
        }
    }
}

pub type LiveRaft = ArbitroRaft<TestStorage, RoutingTransport, NoopStateMachine>;
pub type SharedRaft = Arc<tokio::sync::Mutex<LiveRaft>>;

pub fn spawn_driver(raft: SharedRaft) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut r = raft.lock().await;
            let keep_going = match r.run_once().await {
                Ok(cont) => cont,
                Err(_) => break,
            };
            drop(r);
            if !keep_going {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
}

/// Boot (or RE-boot) a node over an existing storage: hard state and log are
/// reloaded from `storage`, volatile state resets — the in-memory harness's
/// crash-restart approximation.
pub fn boot_node_with_storage(
    hub: Arc<NetworkHub>,
    id: u64,
    peers: &[u64],
    storage: TestStorage,
) -> SharedRaft {
    let peer_id = PeerId(id);
    let config = make_config(id, peers);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    hub.register(peer_id, tx);

    let transport = RoutingTransport {
        from: peer_id,
        hub,
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
    };

    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
    let raft = ArbitroRaft::new(node, NoopStateMachine);
    Arc::new(tokio::sync::Mutex::new(raft))
}
