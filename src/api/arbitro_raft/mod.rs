use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::channel::mpsc;

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role, StateMachine,
};

mod client;
mod run;
mod slot;
mod timers;

use client::{ClientProposal, CommitWaiter};
use slot::{SlotId, SlotRegistry};
use timers::seed;

// Re-export the public types from this module.
pub use client::ClientHandle;

// ---------------------------------------------------------------------------
// ArbitroRaft — execution loop, batching, timers, client backpressure.
// ---------------------------------------------------------------------------

pub struct ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub(crate) node: RaftNode<S, T>,
    /// User-supplied state machine — committed entries are applied to
    /// it in-order from inside the run loop.
    pub(crate) state_machine: SM,
    /// Reusable scratch for `RaftStorage::entry_at` during apply.
    /// Sized to match the storage-read scratch style used elsewhere
    /// (16MB). If a payload exceeds this, `entry_at` returns an error
    /// which is propagated.
    pub(crate) apply_buf: Vec<u8>,
    stopped: bool,
    pub(crate) next_election_at: Instant,
    pub(crate) next_heartbeat_at: Instant,
    pub(crate) election_state: u64,
    client_tx: mpsc::UnboundedSender<ClientProposal>,
    pub(crate) client_rx: mpsc::UnboundedReceiver<ClientProposal>,
    /// Shared registry for commit notifications.
    pub(crate) registry: Arc<SlotRegistry>,
    /// Scratch — payloads drained from client_rx this tick, cleared before each use.
    pub(crate) pending_batch: Vec<Vec<u8>>,
    /// Scratch — slots parallel to pending_batch, drained together.
    pub(crate) pending_slots: Vec<SlotId>,
    /// Entries replicated but not yet committed; resolved as commit_index advances.
    pub(crate) commit_waiters: Vec<CommitWaiter>,
    /// Long-lived inbound buffer to avoid per-frame allocations.
    pub(crate) inbound_buf: Box<[u8]>,
}

// User payloads whose first byte matches the config-change magic would be
// decoded as control entries by the apply path and silently mutate the
// voter set. Reject them at every user-facing propose entry; internal
// config-change proposals call `self.node.propose_once` directly and
// bypass this check.
#[inline]
fn reject_reserved_prefix(payload: &[u8]) -> Result<(), RaftError> {
    // Frame-size contract (P1-4): reject an over-large payload at propose time.
    // Without this, a payload that fits the leader but not a peer's fixed
    // `inbound_buf` would commit locally and then silently never replicate.
    if payload.len() > crate::protocol::codec::wire::MAX_ENTRY_PAYLOAD {
        return Err(RaftError::InvalidPayload(
            "payload exceeds MAX_ENTRY_PAYLOAD and would not fit a peer's frame buffer",
        ));
    }
    if payload.first().copied() == Some(crate::api::node::membership::CONFIG_CHANGE_MAGIC) {
        return Err(RaftError::InvalidPayload(
            "payload first byte collides with reserved config-change magic (0xC0)",
        ));
    }
    Ok(())
}

// --- Public API --------------------------------------------------------------

impl<S, T, SM> ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub fn new(node: RaftNode<S, T>, state_machine: SM) -> Self {
        let (client_tx, client_rx) = mpsc::unbounded();
        // 65k slots = 4MB RAM — absorbs extreme concurrent bursts without backpressure.
        let registry_cap = 65536;
        let mut raft = Self {
            election_state: seed(node.node_id()),
            node,
            state_machine,
            // 16MB matches the payload scratch style used inside RaftNode.
            apply_buf: vec![0u8; 16 * 1024 * 1024],
            stopped: false,
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            client_tx,
            client_rx,
            registry: SlotRegistry::new(registry_cap),
            pending_batch: Vec::with_capacity(4096),
            pending_slots: Vec::with_capacity(4096),
            commit_waiters: Vec::with_capacity(4096),
            inbound_buf: vec![0u8; crate::protocol::codec::wire::MAX_FRAME_SIZE]
                .into_boxed_slice(),
        };
        raft.reset_election_deadline();
        raft.reset_heartbeat_deadline();
        raft
    }

    #[inline]
    pub fn node(&self) -> &RaftNode<S, T> {
        &self.node
    }
    #[inline]
    pub fn node_mut(&mut self) -> &mut RaftNode<S, T> {
        &mut self.node
    }
    /// Shared reference to the state machine.
    #[inline]
    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }
    /// Mutable reference to the state machine. Callers must NOT mutate
    /// applied state directly; use it for snapshot/introspection only.
    #[inline]
    pub fn state_machine_mut(&mut self) -> &mut SM {
        &mut self.state_machine
    }
    #[inline]
    pub fn role(&self) -> Role {
        self.node.role()
    }
    #[inline]
    pub fn commit_index(&self) -> LogIndex {
        self.node.commit_index()
    }
    /// Who this node currently believes leads the cluster (`None` if unknown).
    /// The getter an operator or a `NotLeader`-redirect path calls to locate
    /// the leader.
    #[inline]
    pub fn leader_id(&self) -> Option<PeerId> {
        self.node.leader_id()
    }
    /// Consistent point-in-time snapshot of this node's consensus state for
    /// health checks and dashboards — see [`crate::RaftStatus`].
    #[inline]
    pub fn status(&self) -> crate::RaftStatus {
        self.node.status()
    }

    /// Cheaply-clonable read-only observer over the committed log index.
    ///
    /// Clone this before moving `self` into a background task so external
    /// consumers (for example an apply loop that lives in another task)
    /// can safely poll the current commit boundary without holding the
    /// node.
    #[inline]
    pub fn commit_index_observer(&self) -> crate::CommitIndexObserver {
        self.node.commit_index_observer()
    }
    #[inline]
    pub fn node_id(&self) -> PeerId {
        self.node.node_id()
    }
    /// Stop the run loop and unblock every writer waiting on a commit.
    ///
    /// Safe to call more than once — subsequent calls are no-ops. Without
    /// this, `WriteFuture`s parked on slots for proposals still sitting in
    /// `client_rx` or `commit_waiters` would poll `SLOT_PENDING` forever
    /// once the run loop stops ticking.
    pub fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        // Close the client channel so any late `ClientHandle::write` fails fast
        // with "raft node stopped" instead of parking its slot forever once the
        // run loop no longer ticks (PS10). Buffered proposals are still drainable
        // below.
        self.client_rx.close();
        while let Ok(proposal) = self.client_rx.try_recv() {
            self.registry.get(proposal.slot_id).notify_error();
        }
        self.fail_commit_waiters();
    }

    /// Returns a clonable [`ClientHandle`] for concurrent writes from multiple tasks.
    #[inline]
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle {
            tx: self.client_tx.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Direct single-entry propose — caller holds `&mut self` (e.g. benchmarks, tests).
    #[inline]
    pub async fn propose_once(&mut self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        reject_reserved_prefix(payload)?;
        self.node.propose_once(payload).await
    }

    /// Direct batch propose — caller holds `&mut self`.
    #[inline]
    pub async fn propose_batch_once(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<&[LogIndex], RaftError> {
        for p in payloads {
            reject_reserved_prefix(p)?;
        }
        self.node.propose_batch_once(payloads).await
    }

    /// Propose a joint-consensus membership change transitioning the
    /// cluster from the current voter set to `new_peers`.
    ///
    /// Semantics: appends TWO entries to the log — first the joint
    /// configuration `C_old_new` (safe under both quorums), then, once
    /// that entry commits, the final configuration `C_new`. Returns the
    /// `LogIndex` of the FINAL entry.
    ///
    /// This method drives the transition to completion before returning.
    /// If the leader steps down mid-transition, the underlying
    /// `propose_once` returns `RaftError::NotLeader` and the caller
    /// should retry against the new leader.
    pub async fn propose_config_change(
        &mut self,
        new_peers: Vec<PeerId>,
    ) -> Result<LogIndex, RaftError> {
        use crate::api::node::membership::{ConfigChangeEntry, ConfigChangePhase};

        if !self.node.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .node
                    .hard_state()
                    .voted_for
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }

        // Reject empty new_peers up-front — a Final(C_new) with an empty
        // voter set is unrecoverable (no quorum can ever be formed).
        if new_peers.is_empty() {
            return Err(RaftError::InvalidConfig(
                "config-change new_peers must be non-empty",
            ));
        }

        let old_peers = self.node.peers().to_vec();

        // Require at least one voter to appear in both old and new sets.
        // A fully disjoint transition drops availability during the joint
        // phase to zero on either side and provides no witness carrying
        // committed history across the cut.
        let overlap = new_peers.iter().any(|p| old_peers.contains(p));
        if !overlap {
            return Err(RaftError::InvalidConfig(
                "config-change new_peers must overlap current peers",
            ));
        }

        // Phase 1 — joint entry (C_old_new).
        let joint = ConfigChangeEntry {
            phase: ConfigChangePhase::Joint,
            old_peers: old_peers.clone(),
            new_peers: new_peers.clone(),
        };
        let joint_bytes = joint.encode();
        let _joint_idx = self.node.propose_once(&joint_bytes).await?;

        // Apply the joint entry to THIS node's effective voter set
        // before appending Final. `propose_once` returns once the entry
        // commits under the current (old) quorum, but the apply loop
        // does not run between two awaits on the same task — so without
        // this explicit apply, `config.peers` would still be the OLD set
        // when Final is appended and its commit would be decided under
        // the OLD quorum instead of the joint (union) quorum.
        self.node.apply_config_change(&joint)?;

        // Phase 2 — final entry (C_new). Commit now proceeds under the
        // joint voter set (union of old ∪ new), which is the safety
        // requirement of §4.3.
        let final_entry = ConfigChangeEntry {
            phase: ConfigChangePhase::Final,
            old_peers,
            new_peers,
        };
        let final_bytes = final_entry.encode();
        let final_idx = self.node.propose_once(&final_bytes).await?;
        Ok(final_idx)
    }

    #[inline]
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let elected = self.node.campaign_once(&mut self.inbound_buf).await?;
        self.reset_election_deadline();
        if elected {
            self.reset_heartbeat_deadline();
        }
        Ok(elected)
    }

    #[inline]
    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        let result = self.node.send_heartbeat_once().await;
        self.reset_heartbeat_deadline();
        result
    }

    #[inline]
    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(
                P,
                DispatchContextView<'a>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        self.node.on_with(spec, handler)
    }

    #[inline]
    pub async fn dispatch<P, R>(
        &mut self,
        spec: DispatchSpec<P, R>,
        params: P,
    ) -> Result<DispatchHandle<R>, RaftError>
    where
        P: Send + 'static,
        R: Clone + Send + 'static,
    {
        self.node.dispatch(spec, params).await
    }

    pub async fn run_once(&mut self) -> Result<bool, RaftError> {
        if self.stopped {
            return Ok(false);
        }
        if self.node.is_leader() {
            self.run_leader_once().await?;
        } else {
            self.run_follower_once().await?;
        }
        Ok(!self.stopped)
    }

    pub async fn run(&mut self) -> Result<(), RaftError> {
        loop {
            match self.run_once().await {
                Ok(true) => continue,
                Ok(false) => return Ok(()),
                // A fatal error terminates the loop. It must be logged loudly —
                // callers commonly `tokio::spawn(raft.run())` and drop the
                // JoinHandle, so a silent return would leave a dead node the
                // process never notices.
                Err(e) => {
                    tracing::error!(
                        node_id = self.node.node_id().0,
                        term = self.node.current_term().0,
                        class = ?e.class(),
                        error = %e,
                        "raft run loop terminated"
                    );
                    return Err(e);
                }
            }
        }
    }
}

impl<S, T, SM> Drop for ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    fn drop(&mut self) {
        self.stop();
    }
}
