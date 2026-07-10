use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::{
    HardState, InboundRaftMessage, LogEntry, LogIndex, NodeConfig, PeerId, RaftCustomRegistry,
    RaftError, RaftMessage, Role, SoftState, Term, TimingConfig,
};

mod dispatch;
mod election;
mod generational;
mod progress;
mod replication;
mod snapshot;
mod leader_balance;
mod log_compaction;
pub(crate) mod membership;
mod snapshot_install;

pub(crate) use dispatch::PendingCustomDispatch;
pub(crate) use progress::{AppendAttemptState, PeerMap, PeerProgress, PendingSnapshot};

/// RaftNode implements the core Raft state machine logic.
/// To allow the node to be long-lived (not bound by ephemeral lifetimes),
/// the preallocated scratchpads use 'static internally but are safely
/// cleared before and after every transient use.
pub struct RaftNode<S, T> {
    pub(crate) config: NodeConfig,
    pub(crate) storage: S,
    pub(crate) transport: T,
    pub(crate) hard_state: HardState,
    pub(crate) soft_state: SoftState,
    pub(crate) custom_registry: RaftCustomRegistry,
    pub(crate) pending_custom: HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    pub(crate) peer_progress: PeerMap<PeerProgress>,
    /// Joint-consensus voter sets active while a `C_old_new` entry is the
    /// effective configuration. `None` outside a transition. During Joint
    /// phase, commit requires majority-of-old AND majority-of-new (Raft §4.3);
    /// `config.peers` still holds the union for broadcast/replication targets.
    pub(crate) joint_peers: Option<(Vec<PeerId>, Vec<PeerId>)>,
    pub(crate) pending_snapshots: HashMap<PeerId, PendingSnapshot>,
    pub(crate) log_metadata: generational::LogMetadataArena,

    // Scratchpads — pre-allocated buffers reused across calls on the hot path.
    // Always call .clear() before use; never assume they are empty.

    // Safety: we use 'static here only for the preallocated storage.
    // Elements are cleared after every call.
    pub(crate) scratch_entries: Vec<LogEntry<'static>>,
    pub(crate) scratch_indexes: Vec<LogIndex>,
    pub(crate) scratch_peers: Vec<PeerId>,
    pub(crate) scratch_quorum_buf: Vec<u8>,
    pub(crate) scratch_vectored: Vec<(*const u8, usize)>,
    pub(crate) scratch_pending: PeerMap<AppendAttemptState>,
    #[allow(dead_code)] // reserved for per-peer attempt timing instrumentation
    pub(crate) scratch_started: HashMap<PeerId, std::time::Instant>,
    pub(crate) scratch_outbound: Vec<u8>,
    pub(crate) scratch_payload: Vec<u8>,
    pub(crate) scratch_payload_refs: Vec<&'static [u8]>,
    pub(crate) scratch_responders: Vec<PeerId>,

    /// Cached last-log position — kept in sync with every append/truncate so
    /// `try_advance_commit_index` and leader-progress init avoid a storage read.
    pub(crate) cached_last_log: (LogIndex, Term),

    /// Atomic mirror of `soft_state.commit_index`, updated via
    /// [`RaftNode::set_commit_index`] so external observers (e.g. an
    /// apply loop that lives in another task) can safely read the
    /// committed boundary without holding the node lock.
    pub(crate) commit_index_pub: Arc<AtomicU64>,

    /// Highest log index applied to the state machine.
    ///
    /// Volatile — reset to `LogIndex(0)` on restart. The state
    /// machine is responsible for its own persistence and
    /// reconciliation via `snapshot`/`restore`. The Raft protocol
    /// guarantees `last_applied <= commit_index` at all times.
    pub(crate) last_applied: LogIndex,
}

/// Read-only, cheaply-clonable view of a Raft node's committed log index.
///
/// The Raft protocol guarantees that once an entry is committed it is
/// safe to apply to the state machine. Consumers polling `get()` see a
/// monotonically non-decreasing value.
#[derive(Debug, Clone)]
pub struct CommitIndexObserver {
    inner: Arc<AtomicU64>,
}

impl CommitIndexObserver {
    /// Current committed index. Safe to call from any thread.
    #[inline]
    pub fn get(&self) -> LogIndex {
        LogIndex(self.inner.load(Ordering::Acquire))
    }
}

// SAFETY: All raw pointers in scratch_vectored are ephemeral and cleared after use.
unsafe impl<S, T> Send for RaftNode<S, T>
where
    S: Send,
    T: Send,
{
}
unsafe impl<S, T> Sync for RaftNode<S, T>
where
    S: Sync,
    T: Sync,
{
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
        let peer_count = config.peers.len();
        Ok(Self {
            config,
            storage,
            transport,
            hard_state,
            soft_state,
            custom_registry: RaftCustomRegistry::new(),
            pending_custom: HashMap::with_capacity(1024),
            peer_progress: PeerMap::with_capacity(peer_count),
            joint_peers: None,
            pending_snapshots: HashMap::with_capacity(peer_count),
            scratch_entries: Vec::with_capacity(1024),
            scratch_indexes: Vec::with_capacity(1024),
            scratch_peers: Vec::with_capacity(peer_count),
            scratch_quorum_buf: vec![0u8; 64 * 1024],
            scratch_vectored: Vec::with_capacity(2048),
            scratch_pending: PeerMap::with_capacity(peer_count),
            scratch_started: HashMap::with_capacity(peer_count),
            scratch_outbound: vec![0; 1024 * 1024], // 1MB pre-allocated scratch for outbound encoding
            scratch_payload: vec![0; 16 * 1024 * 1024], // 16MB pre-allocated scratch for storage reads
            scratch_payload_refs: Vec::with_capacity(1024),
            scratch_responders: Vec::with_capacity(peer_count),
            cached_last_log,
            log_metadata: generational::LogMetadataArena::new(8192),
            commit_index_pub: Arc::new(AtomicU64::new(0)),
            // last_applied is volatile — always 0 on restart, mirrors
            // the invariant that commit_index also re-initializes to 0.
            last_applied: LogIndex(0),
        })
    }

    /// Update `soft_state.commit_index` and publish the new value to
    /// any [`CommitIndexObserver`] holding a clone of the atomic mirror.
    ///
    /// This is the single write path for the commit index — call this
    /// instead of assigning `soft_state.commit_index` directly so
    /// external observers (e.g. an apply loop in another task) stay
    /// consistent.
    ///
    /// Callers outside the crate should NOT invoke this directly in
    /// production; the Raft loop drives commit-index progression. It
    /// is exposed (hidden from docs) purely so correctness tests can
    /// simulate commit advances without spinning up a full cluster.
    #[doc(hidden)]
    #[inline]
    pub fn set_commit_index(&mut self, idx: LogIndex) {
        self.soft_state.commit_index = idx;
        self.commit_index_pub.store(idx.0, Ordering::Release);
    }

    /// Cheaply-clonable read-only observer over the committed log index.
    ///
    /// Consumers must clone the observer before the node is moved into
    /// its background task (as with [`ArbitroRaft::client_handle`]).
    #[inline]
    pub fn commit_index_observer(&self) -> CommitIndexObserver {
        CommitIndexObserver {
            inner: self.commit_index_pub.clone(),
        }
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

    /// Voter set this node is currently configured with.
    ///
    /// This is the same slice that `RaftGroupRegistry` uses for quorum math.
    /// The membership-change task (Group 2) mutates this set in place when a
    /// joint-consensus transition applies; readers must not cache the slice
    /// across a `run_once` boundary.
    #[inline]
    pub fn peers(&self) -> &[PeerId] {
        &self.config.peers
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

    /// Highest log index applied to the state machine so far.
    ///
    /// Volatile — resets to `LogIndex(0)` on process restart. Advanced
    /// only by the apply loop in [`ArbitroRaft`], never larger than
    /// [`RaftNode::commit_index`].
    #[inline]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Advance the applied cursor. Callers MUST guarantee
    /// `idx <= self.commit_index()`; the Raft protocol requires
    /// `last_applied <= commit_index` at all times.
    #[inline]
    pub(crate) fn set_last_applied(&mut self, idx: LogIndex) {
        debug_assert!(
            idx <= self.soft_state.commit_index,
            "last_applied ({}) must not exceed commit_index ({})",
            idx.0,
            self.soft_state.commit_index.0,
        );
        self.last_applied = idx;
    }

    /// Read a single log entry into the supplied scratch buffer.
    ///
    /// Thin wrapper around [`crate::RaftStorage::entry_at`]. Exposed so
    /// the apply loop in [`ArbitroRaft`] can read committed entries
    /// without holding a `&mut` borrow of storage while it also holds
    /// `&mut` on the state machine (split-borrow via disjoint fields).
    #[inline]
    pub fn read_entry_payload_into<'a>(
        &self,
        index: LogIndex,
        buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        self.storage.entry_at(index, buf)
    }

    #[inline]
    pub fn peer_progress(&self, peer: PeerId) -> Option<(LogIndex, LogIndex)> {
        self.peer_progress
            .get(&peer)
            .map(|p| (p.next_index, p.match_index))
    }

    /// Forces this node to become leader for the given term bypassing election.
    #[doc(hidden)]
    pub fn become_leader_for_benchmark(&mut self, term: Term) {
        self.soft_state.is_leader = true;
        self.soft_state.role = Role::Leader;
        self.hard_state.current_term = term;
        self.soft_state.leader_id = Some(self.config.node_id);
        let _ = self.initialize_leader_progress();
    }

    /// Receive one raw frame from the transport, decode it, and dispatch to the
    /// appropriate handler. `inbound_buf` is provided by the outer loop to avoid allocs.
    pub async fn handle_once(&mut self, inbound_buf: &mut [u8]) -> Result<(), RaftError> {
        let n = self.transport.recv_frame(inbound_buf).await?;
        let inbound = crate::decode_message(&inbound_buf[..n])?;
        self.handle_inbound(inbound).await
    }

    /// O(1) dispatch — switch compiles to a jump table in optimized builds.
    pub async fn handle_inbound(
        &mut self,
        inbound: InboundRaftMessage<'_>,
    ) -> Result<(), RaftError> {
        let from = inbound.from;
        // Only track contact timestamps for configured members — `from` is
        // caller-controlled and an unbounded peer would otherwise leak entries
        // into scratch_started forever.
        if self.is_leader() && self.config.peers.contains(&from) {
            self.scratch_started.insert(from, std::time::Instant::now());
        }
        match inbound.message {
            RaftMessage::AppendEntries(msg, payload) => {
                self.handle_append_entries(from, msg, payload).await
            }
            RaftMessage::AppendEntriesSeeded {
                ae,
                headers,
                payloads,
            } => {
                self.handle_append_entries_seeded(from, ae, headers, payloads)
                    .await
            }
            RaftMessage::AppendEntriesResp(msg) => {
                self.handle_append_entries_response(from, msg).await
            }
            RaftMessage::Custom(payload) => self.handle_custom_message(from, payload).await,
            RaftMessage::CustomResponse(payload) => {
                self.handle_custom_response(from, payload).await
            }
            RaftMessage::RequestVote(msg) => self.handle_request_vote(from, msg).await,
            RaftMessage::RequestVoteResp(msg) => self.handle_request_vote_response(from, msg).await,
            RaftMessage::PreVote(msg) => self.handle_pre_vote(from, msg).await,
            RaftMessage::PreVoteResp(msg) => self.handle_pre_vote_response(from, msg).await,
            RaftMessage::InstallSnapshot(msg, payload) => {
                self.handle_install_snapshot(from, msg, payload).await
            }
            RaftMessage::InstallSnapshotResp(msg) => {
                self.handle_install_snapshot_response(from, msg).await
            }
            RaftMessage::AppendEntriesVectored(_, _)
            | RaftMessage::AppendEntriesSeededVectored { .. } => {
                // Inbound vectored messages are not expected in v0.1.
                // Protocol only uses vectored for OUTBOUND.
                Err(RaftError::Protocol("unexpected vectored inbound".into()))
            }
        }
    }

    pub(crate) fn step_down(&mut self, new_term: Term) -> Result<(), RaftError> {
        self.hard_state.current_term = new_term;
        self.hard_state.voted_for = None;
        // Persist before any outbound send
        self.storage.save_hard_state(&self.hard_state)?;
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.peer_progress.clear();
        self.pending_snapshots.clear();
        // Dispatch handles are leader-scoped by design: any custom dispatch
        // in flight is aborted at a leader transition, so drop the pending
        // entries here rather than let them accumulate across step-downs.
        // TODO: DispatchHandle (src/dispatch/tx/handle.rs) has no Drop impl,
        // so a handle dropped by the caller without awaiting completion still
        // leaks its tx_id registration independently of this clear.
        self.pending_custom.clear();
        // Stale contact timestamps must not survive a leader transition.
        self.scratch_started.clear();
        Ok(())
    }

    /// Truncate the log and refresh the last-log cache.
    #[inline]
    pub(crate) fn storage_truncate(&mut self, from: LogIndex) -> Result<(), RaftError> {
        self.storage.truncate_suffix(from)?;
        self.log_metadata.truncate_suffix(from);
        self.cached_last_log = self.storage.last_log_position()?;
        Ok(())
    }

    /// Encodes and sends a message using Vectored I/O to avoid payload copies.
    pub(crate) async fn send_message(&mut self, peer: PeerId, msg: &RaftMessage<'_>) -> bool {
        self.scratch_vectored.clear();

        // SAFETY: We temporarily treat our raw pointer vector as a Vec<&[u8]>.
        let vectored_ref = unsafe {
            std::mem::transmute::<&mut Vec<(*const u8, usize)>, &mut Vec<&[u8]>>(
                &mut self.scratch_vectored,
            )
        };

        if crate::protocol::encode_message_vectored(
            self.config.node_id,
            msg,
            &mut self.scratch_outbound,
            vectored_ref,
        )
        .is_err()
        {
            return false;
        }

        let ok = self
            .transport
            .send_vectored(peer, vectored_ref)
            .await
            .is_ok();

        self.scratch_vectored.clear();
        ok
    }
}

pub(crate) fn quorum(nodes: usize) -> usize {
    (nodes / 2) + 1
}

pub(crate) fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

pub(crate) fn trace_log(node_id: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        tracing::trace!(node = node_id.0, "{}", msg.as_ref());
    }
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub(crate) fn check_quorum_active(&mut self) -> bool {
        let timeout = self.election_timeout();
        let now = std::time::Instant::now();
        let mut active_count = 1usize; // self is always active

        for &peer in &self.config.peers {
            if peer != self.config.node_id {
                if let Some(&last_contact) = self.scratch_started.get(&peer) {
                    if now.saturating_duration_since(last_contact) < timeout {
                        active_count += 1;
                    }
                }
            }
        }
        active_count >= quorum(self.config.peers.len())
    }
}
