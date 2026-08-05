use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta,
    StateMachine, Term, TimingConfig,
};

/// Test-local no-op StateMachine — apply is a no-op; snapshot/restore return empty.
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
// AbortOnDrop - Drop guard to prevent task leakage and hung test threads.
// ---------------------------------------------------------------------------
struct AbortOnDrop {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal in-process storage
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct TestStorage {
    hard_state: Arc<Mutex<Option<HardState>>>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
}

impl RaftStorage for TestStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone().unwrap_or_default())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
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
                // Split the front chunk off the remaining buffer so the
                // shared ref pushed into `out` is never invalidated by a
                // later write through `buf` — no transmute, and Stacked
                // Borrows (Miri) clean.
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
// Helpers
// ---------------------------------------------------------------------------
fn config_3node(node_id: u64) -> NodeConfig {
    let peers_id = [1u64, 2, 3];
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers_id.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers_id
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9000 + id as u16)),
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
// Network Hub & Routing Transport
// ---------------------------------------------------------------------------
struct NetworkHub {
    senders: Mutex<
        std::collections::HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>,
    >,
    partitions: Arc<Mutex<std::collections::HashSet<(PeerId, PeerId)>>>,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(std::collections::HashMap::new()),
            partitions: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }

    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        let is_partitioned = self.partitions.lock().unwrap().contains(&(from, to))
            || self.partitions.lock().unwrap().contains(&(to, from));
        if is_partitioned {
            return;
        }
        if let Some(sender) = self.senders.lock().unwrap().get(&to) {
            let _ = sender.send((from, data));
        }
    }
}

struct RoutingTransport {
    from: PeerId,
    hub: Arc<NetworkHub>,
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(PeerId, Vec<u8>)>>>,
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
                Ok(None) => Ok(None),
                Err(_) => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_prevote_prevents_term_inflation() {
    let hub = Arc::new(NetworkHub::new());
    let mut tasks = vec![];
    let mut storages = vec![];

    for i in 1..=3 {
        let peer_id = PeerId(i);
        let config = config_3node(i);
        let storage = TestStorage::default();
        storages.push(storage.clone());

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        hub.register(peer_id, tx);

        let transport = RoutingTransport {
            from: peer_id,
            hub: hub.clone(),
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        };

        let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
        let mut raft = ArbitroRaft::new(node, NoopSM);

        tasks.push(tokio::spawn(async move {
            let _ = raft.run().await;
        }));
    }
    let _guard = AbortOnDrop { handles: tasks };

    // Let election complete and leader term establish
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let term_after_election = storages[0].load_hard_state().unwrap().current_term;
    assert!(
        term_after_election.0 >= 1,
        "Leader should have been elected with a term of at least 1"
    );

    // Partition Node 3 completely
    hub.partitions
        .lock()
        .unwrap()
        .insert((PeerId(1), PeerId(3)));
    hub.partitions
        .lock()
        .unwrap()
        .insert((PeerId(2), PeerId(3)));

    // Let Node 3 run isolated and timeout several times
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let term_during_partition = storages[2].load_hard_state().unwrap().current_term;
    assert_eq!(
        term_during_partition, term_after_election,
        "Pre-vote failed to prevent term inflation on isolated node!"
    );

    // Rejoin Node 3
    hub.partitions.lock().unwrap().clear();

    // Node 3 should rejoin and accept the current leader without forcing reelection
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let final_term = storages[0].load_hard_state().unwrap().current_term;
    assert_eq!(
        final_term, term_after_election,
        "Term inflated after partitioned node re-joined!"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_campaign_converges() {
    let hub = Arc::new(NetworkHub::new());
    let mut tasks = vec![];
    let mut storages = vec![];

    for i in 1..=3 {
        let peer_id = PeerId(i);
        let config = config_3node(i);
        let storage = TestStorage::default();
        storages.push(storage.clone());

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        hub.register(peer_id, tx);

        let transport = RoutingTransport {
            from: peer_id,
            hub: hub.clone(),
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        };

        let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
        let mut raft = ArbitroRaft::new(node, NoopSM);

        tasks.push(tokio::spawn(async move {
            let _ = raft.run().await;
        }));
    }
    let _guard = AbortOnDrop { handles: tasks };

    // Await election convergence
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let term1 = storages[0].load_hard_state().unwrap().current_term;
    let term2 = storages[1].load_hard_state().unwrap().current_term;
    let term3 = storages[2].load_hard_state().unwrap().current_term;

    assert!(
        term1.0 >= 1,
        "Cluster failed to elect a leader during simultaneous startup"
    );
    assert_eq!(term1, term2, "Node terms did not converge");
    assert_eq!(term2, term3, "Node terms did not converge");
}
