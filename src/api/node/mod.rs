use std::collections::HashMap;
use std::sync::OnceLock;


use bytes::Bytes;

use crate::{
    HardState, InboundRaftMessageView, LogEntry, LogIndex, NodeConfig, PeerId, RaftCustomRegistry,
    RaftError, RaftMessage, Role, SoftState, Term, TimingConfig,
};

mod dispatch;
mod election;
mod progress;
mod replication;
mod snapshot;

pub(crate) use dispatch::PendingCustomDispatch;
pub(crate) use progress::{AppendAttemptState, PeerMap, PeerProgress, PendingSnapshot};

pub struct RaftNode<S, T> {
    pub(crate) config: NodeConfig,
    pub(crate) storage: S,
    pub(crate) transport: T,
    pub(crate) hard_state: HardState,
    pub(crate) soft_state: SoftState,
    pub(crate) custom_registry: RaftCustomRegistry,
    pub(crate) pending_custom: HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    pub(crate) peer_progress: PeerMap<PeerProgress>,
    pub(crate) pending_snapshots: HashMap<PeerId, PendingSnapshot>,

    // Scratchpads — pre-allocated buffers reused across calls on the hot path.
    // Always call .clear() before use; never assume they are empty.
    pub(crate) scratch_entries: Vec<LogEntry>,
    pub(crate) scratch_indexes: Vec<LogIndex>,
    pub(crate) scratch_peers: Vec<PeerId>,
    pub(crate) scratch_pending: PeerMap<AppendAttemptState>,
    #[allow(dead_code)] // reserved for per-peer attempt timing instrumentation
    pub(crate) scratch_started: HashMap<PeerId, std::time::Instant>,

    /// Cached last-log position — kept in sync with every append/truncate so
    /// `try_advance_commit_index` and leader-progress init avoid a storage read.
    pub(crate) cached_last_log: (LogIndex, Term),
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub fn new(config: NodeConfig, storage: S, transport: T) -> Result<Self, RaftError> {
        crate::validate_node_config(&config)?;
        let hard_state = storage.load_hard_state()?;
        let cached_last_log = storage.last_log_position()?;
        let soft_state = SoftState {
            leader_id: None,
            is_leader: false,
            role: Role::Follower,
            // commit_index is volatile — always 0 on restart, advanced by AppendEntries.
            commit_index: LogIndex(0),
        };
        Ok(Self {
            config,
            storage,
            transport,
            hard_state,
            soft_state,
            custom_registry: RaftCustomRegistry::new(),
            pending_custom: HashMap::new(),
            peer_progress: PeerMap::new(),
            pending_snapshots: HashMap::new(),
            scratch_entries: Vec::new(),
            scratch_indexes: Vec::new(),
            scratch_peers: Vec::new(),
            scratch_pending: PeerMap::new(),
            scratch_started: HashMap::new(),
            cached_last_log,
        })
    }

    #[inline]
    pub fn custom_registry(&self) -> &RaftCustomRegistry {
        &self.custom_registry
    }

    #[inline]
    pub fn role(&self) -> Role {
        self.soft_state.role
    }

    #[inline]
    pub fn node_id(&self) -> PeerId {
        self.config.node_id
    }

    #[inline]
    pub fn timing(&self) -> TimingConfig {
        self.config.timing
    }

    #[inline]
    pub fn transport(&self) -> &T {
        &self.transport
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.soft_state.role == Role::Leader
    }

    #[inline]
    pub fn current_term(&self) -> Term {
        self.hard_state.current_term
    }

    #[inline]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub fn commit_index(&self) -> LogIndex {
        self.soft_state.commit_index
    }

    #[inline]
    pub fn peer_progress(&self, peer: PeerId) -> Option<(LogIndex, LogIndex)> {
        self.peer_progress
            .get(&peer)
            .map(|p| (p.next_index, p.match_index))
    }

    /// Forces this node to become leader for the given term bypassing election.
    ///
    /// # Stability
    ///
    /// **Benchmark and test helper only.** This bypasses the Raft election protocol.
    /// MUST NOT be called in production code. May be removed in any minor release.
    #[doc(hidden)]
    pub fn become_leader_for_benchmark(&mut self, term: Term) {
        self.soft_state.is_leader = true;
        self.soft_state.role = Role::Leader;
        self.hard_state.current_term = term;
        self.soft_state.leader_id = Some(self.config.node_id);
        let _ = self.initialize_leader_progress();
    }

    /// Receive one raw frame from the transport, decode it, and dispatch to the
    /// appropriate handler.
    pub async fn handle_once(&mut self) -> Result<(), RaftError> {
        let raw = self.transport.recv_frame().await?;
        let inbound = crate::decode_message_view(raw)?;
        self.handle_inbound(inbound).await
    }

    /// O(1) dispatch — switch compiles to a jump table in optimized builds.
    pub async fn handle_inbound(
        &mut self,
        inbound: InboundRaftMessageView,
    ) -> Result<(), RaftError> {
        match inbound.message {
            crate::RaftMessageView::RequestVote(msg) => self.handle_request_vote(msg).await,
            crate::RaftMessageView::RequestVoteResp(msg) => {
                self.handle_request_vote_response(msg).await
            }
            crate::RaftMessageView::AppendEntries(msg) => self.handle_append_entries(msg).await,
            crate::RaftMessageView::AppendEntriesResp(msg) => {
                self.handle_append_entries_response(msg).await
            }
            crate::RaftMessageView::InstallSnapshot(msg) => self.handle_install_snapshot(msg).await,
            crate::RaftMessageView::InstallSnapshotResp(msg) => {
                self.handle_install_snapshot_response(msg).await
            }
            crate::RaftMessageView::Custom(msg) => self.handle_custom_message(msg).await,
            crate::RaftMessageView::CustomResponse(msg) => self.handle_custom_response(msg).await,
        }
    }

    pub(crate) fn step_down(&mut self, new_term: Term) -> Result<(), RaftError> {
        self.hard_state.current_term = new_term;
        self.hard_state.voted_for = None;
        // Persist before any outbound send (guide §Orden de persistencia)
        self.storage.save_hard_state(&self.hard_state)?;
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        // commit_index is monotone — do not reset on step_down
        self.peer_progress.clear();
        self.pending_snapshots.clear();
        Ok(())
    }

    /// Truncate the log and refresh the last-log cache.
    /// Always use this instead of calling `storage.truncate_suffix()` directly.
    #[inline]
    pub(crate) fn storage_truncate(&mut self, from: LogIndex) -> Result<(), RaftError> {
        self.storage.truncate_suffix(from)?;
        self.cached_last_log = self.storage.last_log_position()?;
        Ok(())
    }

    /// Encode a `RaftMessage` into a pre-allocated `Bytes` frame ready for the transport.
    #[inline]
    pub(crate) fn encode_msg(&self, msg: &RaftMessage) -> Result<Bytes, RaftError> {
        crate::encode_message(self.config.node_id, msg)
    }

    /// Encode a message and send best-effort (log on failure, never panic).
    pub(crate) async fn encode_and_send_best_effort(
        &self,
        peer: PeerId,
        msg: &RaftMessage,
    ) -> bool {
        match self.encode_msg(msg) {
            Ok(frame) => self.send_best_effort(peer, frame).await,
            Err(_) => false,
        }
    }

    /// Send a pre-encoded frame best-effort (log on failure, never panic).
    pub(crate) async fn send_best_effort(&self, peer: PeerId, frame: Bytes) -> bool {
        self.transport.send_frame(peer, frame).await.is_ok()
    }
}

pub(crate) fn quorum(nodes: usize) -> usize {
    (nodes / 2) + 1
}

pub(crate) fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

/// Emit a trace-level log entry. Guarded by ARBITRO_RAFT_TRACE env var.
///
/// Uses tracing::trace! — never eprintln! — so this is a no-op at runtime
/// unless the tracing subscriber has TRACE enabled.
pub(crate) fn trace_log(node_id: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        tracing::trace!(node = node_id.0, "{}", msg.as_ref());
    }
}
