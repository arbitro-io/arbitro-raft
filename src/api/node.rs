use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::{
    encode_dispatch_response, AppendEntries, AppendEntriesResp, AppendEntriesRespView,
    AppendEntriesView, DispatchContextView, DispatchHandle, DispatchNodeRole, DispatchRequester,
    DispatchResponder, DispatchResponse, DispatchResponseKind, DispatchRoute, DispatchSpec,
    DispatchTx, EntryPayload, HardState, InboundRaftMessageView, InstallSnapshotRespView,
    InstallSnapshotView, LeaderHint, LogEntry, LogIndex, NodeConfig, PeerId, RaftCustomMessage,
    RaftCustomMessageView, RaftCustomRegistry, RaftCustomResponse, RaftCustomResponseView,
    RaftError, RaftMessage, RaftStorage, RaftTransport, RequestVote, RequestVoteResp,
    RequestVoteRespView, RequestVoteView, Role, SoftState, Term, TimingConfig,
};

pub struct RaftNode<S, T> {
    config: NodeConfig,
    storage: S,
    transport: T,
    hard_state: HardState,
    soft_state: SoftState,
    custom_registry: RaftCustomRegistry,
    pending_custom: HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    peer_progress: HashMap<PeerId, PeerProgress>,
    pending_snapshots: HashMap<PeerId, PendingSnapshot>,

    // Scratchpads for Hardware Sympathy (Zero-Allocation on Hot Path)
    scratch_entries: Vec<LogEntry>,
    scratch_indexes: Vec<LogIndex>,
    scratch_peers: Vec<PeerId>,
    scratch_pending: HashMap<PeerId, AppendAttemptState>,
    scratch_started: HashMap<PeerId, Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PeerProgress {
    next_index: LogIndex,
    match_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendAttemptState {
    attempts: u64,
    sent_last_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendAdvance {
    Completed,
    Retry(AppendAttemptState),
    Dropped,
    Ignored,
}

#[derive(Debug, Clone)]
struct PendingSnapshot {
    meta: crate::SnapshotMeta,
    bytes: Vec<u8>,
}

trait PendingCustomDispatch: Send + Sync {
    fn on_response(&self, peer: PeerId, response: DispatchResponse) -> Result<(), RaftError>;
    fn is_ready(&self) -> bool;
}

struct PendingCustomTx<R> {
    handle: DispatchHandle<R>,
    tx: DispatchTx<R>,
}

impl<R: Clone + Send + 'static> PendingCustomDispatch for PendingCustomTx<R> {
    fn on_response(&self, peer: PeerId, response: DispatchResponse) -> Result<(), RaftError> {
        match response.kind {
            DispatchResponseKind::Accepted => self.tx.accept_raw(peer, response.payload),
            DispatchResponseKind::Rejected => self.tx.reject(peer, response.payload),
            DispatchResponseKind::Progress => self.tx.progress(peer, response.payload),
            DispatchResponseKind::Failed => self.tx.fail(peer, response.payload),
        }
    }

    fn is_ready(&self) -> bool {
        self.handle.is_ready()
    }
}

fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

fn trace_log(node_id: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        eprintln!("[raft-trace node={}] {}", node_id.0, msg.as_ref());
    }
}

fn inbound_kind(message: &crate::RaftMessageView) -> &'static str {
    match message {
        crate::RaftMessageView::RequestVote(_) => "request_vote",
        crate::RaftMessageView::RequestVoteResp(_) => "request_vote_resp",
        crate::RaftMessageView::AppendEntries(_) => "append_entries",
        crate::RaftMessageView::AppendEntriesResp(_) => "append_entries_resp",
        crate::RaftMessageView::InstallSnapshot(_) => "install_snapshot",
        crate::RaftMessageView::InstallSnapshotResp(_) => "install_snapshot_resp",
        crate::RaftMessageView::Custom(_) => "custom",
        crate::RaftMessageView::CustomResponse(_) => "custom_response",
    }
}

impl<S, T> RaftNode<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new(config: NodeConfig, storage: S, transport: T) -> Result<Self, RaftError> {
        crate::validate_node_config(&config)?;
        let hard_state = storage.load_hard_state()?;
        let soft_state = SoftState {
            leader_id: None,
            is_leader: false,
            role: Role::Follower,
        };
        Ok(Self {
            config,
            storage,
            transport,
            hard_state,
            soft_state,
            custom_registry: RaftCustomRegistry::new(),
            pending_custom: HashMap::new(),
            peer_progress: HashMap::new(),
            pending_snapshots: HashMap::new(),
            scratch_entries: Vec::new(),
            scratch_indexes: Vec::new(),
            scratch_peers: Vec::new(),
            scratch_pending: HashMap::new(),
            scratch_started: HashMap::new(),
        })
    }

    pub fn custom_registry(&self) -> &RaftCustomRegistry {
        &self.custom_registry
    }

    #[doc(hidden)]
    pub fn become_leader_for_benchmark(&mut self, term: Term) {
        self.soft_state.is_leader = true;
        self.soft_state.role = Role::Leader;
        self.hard_state.current_term = term;
        self.soft_state.leader_id = Some(self.config.node_id);
    }

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
        self.custom_registry.on_with(spec, handler)
    }

    pub async fn dispatch<P, R>(
        &mut self,
        spec: DispatchSpec<P, R>,
        params: P,
    ) -> Result<DispatchHandle<R>, RaftError>
    where
        P: Send + 'static,
        R: Clone + Send + 'static,
    {
        let envelope = spec.dispatch(params).build()?;
        let targets = self.dispatch_targets(envelope.options().scope);
        let (handle, tx) = envelope.begin(targets.iter().copied(), spec.decode_response_fn());

        self.pending_custom.insert(
            envelope.tx_id(),
            Box::new(PendingCustomTx {
                handle: handle.clone(),
                tx,
            }),
        );

        for peer in targets {
            if peer == self.config.node_id {
                let route = DispatchRoute {
                    role: if self.is_leader() {
                        DispatchNodeRole::Leader
                    } else {
                        DispatchNodeRole::Follower
                    },
                    is_origin: true,
                };
                let responder = LocalDispatchResponder::new(
                    self.config.node_id,
                    envelope.command(),
                    &mut self.pending_custom,
                    envelope.tx_id(),
                );
                self.custom_registry
                    .invoke_bytes_scoped(envelope.bytes().clone(), &responder, route)
                    .await?;
                continue;
            }

            self.transport
                .send(
                    peer,
                    RaftMessage::Custom(RaftCustomMessage {
                        bytes: envelope.bytes().clone(),
                    }),
                )
                .await?;
        }

        Ok(handle)
    }

    pub fn role(&self) -> Role {
        self.soft_state.role
    }

    pub fn node_id(&self) -> PeerId {
        self.config.node_id
    }

    pub fn timing(&self) -> TimingConfig {
        self.config.timing
    }

    pub fn current_term(&self) -> Term {
        self.hard_state.current_term
    }

    pub fn is_leader(&self) -> bool {
        self.soft_state.role == Role::Leader
    }

    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn peer_progress(&self, peer: PeerId) -> Option<(LogIndex, LogIndex)> {
        self.peer_progress
            .get(&peer)
            .map(|p| (p.next_index, p.match_index))
    }

    pub async fn recv_once(&mut self) -> Result<InboundRaftMessageView, RaftError> {
        self.transport.recv().await
    }

    pub async fn handle_once(&mut self) -> Result<(), RaftError> {
        let inbound = self.transport.recv().await?;
        self.handle_inbound(inbound).await
    }

    fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timing.election_max_ms.max(1))
    }

    fn rpc_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timing.heartbeat_ms.max(1).saturating_mul(2))
    }

    fn dispatch_targets(&self, scope: crate::DispatchScope) -> Vec<PeerId> {
        self.config
            .peers
            .iter()
            .copied()
            .filter(|peer| {
                let route = if *peer == self.config.node_id {
                    if self.config.peers.len() > 1 && !self.is_leader() {
                        return matches!(scope, crate::DispatchScope::LocalOnly);
                    }
                    DispatchRoute {
                        role: if self.is_leader() {
                            DispatchNodeRole::Leader
                        } else {
                            DispatchNodeRole::Follower
                        },
                        is_origin: true,
                    }
                } else if self.is_leader() {
                    DispatchRoute::follower(false)
                } else {
                    let is_leader = self.soft_state.leader_id == Some(*peer);
                    DispatchRoute {
                        role: if is_leader {
                            DispatchNodeRole::Leader
                        } else {
                            DispatchNodeRole::Follower
                        },
                        is_origin: false,
                    }
                };
                scope.allows(route)
            })
            .collect()
    }

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

    async fn handle_custom_message(&mut self, msg: RaftCustomMessageView) -> Result<(), RaftError> {
        let route = DispatchRoute {
            role: if self.is_leader() {
                DispatchNodeRole::Leader
            } else {
                DispatchNodeRole::Follower
            },
            is_origin: msg.from() == self.config.node_id,
        };
        let responder = NodeDispatchResponder {
            transport: &self.transport,
            target: msg.from(),
        };
        let requester = NodeDispatchRequester {
            transport: &self.transport,
            target: msg.from(),
            timeout: self.rpc_timeout(),
        };
        self.custom_registry
            .invoke_bytes_with_scoped(
                msg.dispatch().frame_bytes().clone(),
                &responder,
                Some(&requester),
                route,
            )
            .await?;
        Ok(())
    }

    async fn handle_custom_response(
        &mut self,
        msg: RaftCustomResponseView,
    ) -> Result<(), RaftError> {
        let response = DispatchResponse {
            tx_id: msg.tx_id(),
            command: msg.command(),
            kind: msg.response().kind(),
            payload: msg.response().body_bytes(),
        };

        let ready = if let Some(pending) = self.pending_custom.get(&msg.tx_id()) {
            pending.on_response(msg.from(), response)?;
            pending.is_ready()
        } else {
            false
        };

        if ready {
            self.pending_custom.remove(&msg.tx_id());
        }

        Ok(())
    }

    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let started = Instant::now();
        self.soft_state.role = Role::Candidate;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.hard_state.current_term = Term(self.hard_state.current_term.0 + 1);
        self.hard_state.voted_for = Some(self.config.node_id);
        self.storage.save_hard_state(&self.hard_state)?;

        let term = self.hard_state.current_term;
        let votes_needed = quorum(self.config.peers.len());
        let mut votes = 1usize;
        let mut possible_votes = 1usize;
        let mut responders = HashSet::new();
        info!(
            node_id = self.config.node_id.0,
            term = term.0,
            "starting election"
        );
        trace_log(
            self.config.node_id,
            format!(
                "campaign_once start term={} votes_needed={} election_timeout_ms={}",
                term.0,
                votes_needed,
                self.election_timeout().as_millis()
            ),
        );

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;

        let req = RequestVote {
            term,
            candidate_id: self.config.node_id,
            last_log_index,
            last_log_term,
        };

        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|peer| *peer != self.config.node_id)
        {
            if self
                .send_best_effort(peer, RaftMessage::RequestVote(req.clone()), "request_vote")
                .await
            {
                possible_votes += 1;
            }
        }

        if possible_votes < votes_needed {
            return Err(RaftError::NoQuorum);
        }

        while votes < votes_needed && responders.len() < possible_votes.saturating_sub(1) {
            let wait_started = Instant::now();
            let Some(inbound) = self.transport.recv_timeout(self.election_timeout()).await? else {
                trace_log(
                    self.config.node_id,
                    format!(
                        "campaign_once timeout after {} us waiting for vote response",
                        wait_started.elapsed().as_micros()
                    ),
                );
                return Err(RaftError::NoQuorum);
            };
            trace_log(
                self.config.node_id,
                format!(
                    "campaign_once recv kind={} from={} wait_us={}",
                    inbound_kind(&inbound.message),
                    inbound.from.0,
                    wait_started.elapsed().as_micros()
                ),
            );
            match inbound.message {
                crate::RaftMessageView::RequestVoteResp(resp) => {
                    if !responders.insert(inbound.from) {
                        continue;
                    }
                    if resp.term().0 > self.hard_state.current_term.0 {
                        self.step_down(resp.term())?;
                        return Ok(false);
                    }
                    if resp.term() == term && resp.vote_granted() {
                        votes += 1;
                        debug!(
                            node_id = self.config.node_id.0,
                            voter = inbound.from.0,
                            votes,
                            needed = votes_needed,
                            "vote granted"
                        );
                    }
                }
                message => {
                    self.handle_inbound(crate::InboundRaftMessageView {
                        from: inbound.from,
                        message,
                    })
                    .await?
                }
            }
        }

        if votes < votes_needed {
            return Err(RaftError::NoQuorum);
        }

        self.soft_state.role = Role::Leader;
        self.soft_state.is_leader = true;
        self.soft_state.leader_id = Some(self.config.node_id);
        self.initialize_leader_progress()?;
        info!(
            node_id = self.config.node_id.0,
            term = term.0,
            "leader elected"
        );
        trace_log(
            self.config.node_id,
            format!(
                "campaign_once elected term={} total_us={}",
                term.0,
                started.elapsed().as_micros()
            ),
        );
        Ok(true)
    }

    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }
        self.ensure_leader_progress_initialized()?;

        self.scratch_peers.clear();
        for peer in self.config.peers.iter().copied() {
            if peer != self.config.node_id {
                self.scratch_peers.push(peer);
            }
        }

        let mut sent = 0usize;
        let mut i = 0;
        while i < self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            i += 1;
            let msg = self.build_append_for_peer(peer)?;
            if self
                .send_best_effort(peer, RaftMessage::AppendEntries(msg), "heartbeat")
                .await
            {
                sent += 1;
            }
        }
        info!(
            node_id = self.config.node_id.0,
            term = self.hard_state.current_term.0,
            commit_index = self.hard_state.commit_index.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }

    pub async fn propose_once(&mut self, payload: Bytes) -> Result<LogIndex, RaftError> {
        Ok(self.propose_batch_once(vec![payload]).await?.pop().unwrap())
    }

    pub async fn propose_batch_once(
        &mut self,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<LogIndex>, RaftError> {
        let started = Instant::now();
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        let mut total_payload_bytes = 0usize;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            total_payload_bytes += payload.len();
            self.scratch_entries.push(LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        let first_index = *self.scratch_indexes.first().unwrap();
        let last_index = *self.scratch_indexes.last().unwrap();
        trace_log(
            self.config.node_id,
            format!(
                "propose_batch_once start first_index={} last_index={} entries={} payload_bytes={}",
                first_index.0,
                last_index.0,
                self.scratch_entries.len(),
                total_payload_bytes
            ),
        );
        let append_started = Instant::now();
        self.storage.append_entries(&self.scratch_entries)?;
        trace_log(
            self.config.node_id,
            format!(
                "propose_batch_once local_append first_index={} last_index={} entries={} append_us={}",
                first_index.0,
                last_index.0,
                self.scratch_entries.len(),
                append_started.elapsed().as_micros()
            ),
        );
        self.ensure_leader_progress_initialized()?;

        let needed = quorum(self.config.peers.len());
        self.scratch_peers.clear();
        for peer in self.config.peers.iter().copied() {
            if peer != self.config.node_id {
                self.scratch_peers.push(peer);
            }
        }

        let mut accepted = 1usize;
        self.scratch_pending.clear();
        self.scratch_started.clear();

        let mut i = 0;
        while i < self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            i += 1;
            self.scratch_started.insert(peer, Instant::now());
            if let Some(sent_last_index) = self.send_append_attempt(peer, 1).await? {
                self.scratch_pending.insert(
                    peer,
                    AppendAttemptState {
                        attempts: 1,
                        sent_last_index,
                    },
                );
            } else {
                trace_log(
                    self.config.node_id,
                    format!(
                        "propose_once peer={} replicated=false replicate_us=0",
                        peer.0
                    ),
                );
            }
        }

        while accepted < needed && !self.scratch_pending.is_empty() {
            let wait_started = Instant::now();
            let Some(inbound) = self.transport.recv_timeout(self.rpc_timeout()).await? else {
                trace_log(
                    self.config.node_id,
                    format!(
                        "propose_batch_once quorum_wait_timeout pending={} wait_us={}",
                        self.scratch_pending.len(),
                        wait_started.elapsed().as_micros()
                    ),
                );
                break;
            };
            trace_log(
                self.config.node_id,
                format!(
                    "propose_batch_once quorum_recv kind={} from={} wait_us={}",
                    inbound_kind(&inbound.message),
                    inbound.from.0,
                    wait_started.elapsed().as_micros()
                ),
            );

            match inbound.message {
                crate::RaftMessageView::AppendEntriesResp(resp) => {
                    let Some(state) = self.scratch_pending.get(&inbound.from).copied() else {
                        self.handle_append_entries_response(resp).await?;
                        continue;
                    };

                    match self
                        .advance_append_replication(inbound.from, last_index, state, resp)
                        .await?
                    {
                        AppendAdvance::Completed => {
                            self.scratch_pending.remove(&inbound.from);
                            accepted += 1;
                            if let Some(started) = self.scratch_started.remove(&inbound.from) {
                                trace_log(
                                    self.config.node_id,
                                    format!(
                                        "propose_once peer={} replicated=true replicate_us={}",
                                        inbound.from.0,
                                        started.elapsed().as_micros()
                                    ),
                                );
                            }
                        }
                        AppendAdvance::Retry(next_state) => {
                            self.scratch_pending.insert(inbound.from, next_state);
                        }
                        AppendAdvance::Dropped => {
                            self.scratch_pending.remove(&inbound.from);
                            if let Some(started) = self.scratch_started.remove(&inbound.from) {
                                trace_log(
                                    self.config.node_id,
                                    format!(
                                        "propose_once peer={} replicated=false replicate_us={}",
                                        inbound.from.0,
                                        started.elapsed().as_micros()
                                    ),
                                );
                            }
                        }
                        AppendAdvance::Ignored => {}
                    }
                }
                message => {
                    self.handle_inbound(crate::InboundRaftMessageView {
                        from: inbound.from,
                        message,
                    })
                    .await?;
                }
            }
        }

        if accepted < needed {
            return Err(RaftError::NoQuorum);
        }

        self.hard_state.commit_index = last_index;
        let commit_started = Instant::now();
        self.storage.save_hard_state(&self.hard_state)?;
        trace_log(
            self.config.node_id,
            format!(
                "propose_batch_once commit_saved first_index={} last_index={} commit_us={}",
                first_index.0,
                last_index.0,
                commit_started.elapsed().as_micros()
            ),
        );
        info!(
            node_id = self.config.node_id.0,
            first_index = first_index.0,
            last_index = last_index.0,
            entries = self.scratch_entries.len(),
            term = self.hard_state.current_term.0,
            "entries committed"
        );

        trace_log(
            self.config.node_id,
            format!(
                "propose_batch_once done first_index={} last_index={} total_us={}",
                first_index.0,
                last_index.0,
                started.elapsed().as_micros()
            ),
        );
        self.drain_inbound_ready().await?;
        Ok(self.scratch_indexes.clone())
    }

    pub async fn install_snapshot_once(
        &mut self,
        peer: PeerId,
        meta: crate::SnapshotMeta,
        snapshot: Bytes,
    ) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }

        let chunk_size = self.config.limits.snapshot_chunk_bytes.max(1);
        let total_len = snapshot.len();
        let mut offset = 0usize;

        loop {
            let end = (offset + chunk_size).min(total_len);
            let done = end == total_len;
            let chunk_bytes = if total_len == 0 {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(&snapshot[offset..end])
            };

            self.transport
                .send(
                    peer,
                    RaftMessage::InstallSnapshot(crate::InstallSnapshot {
                        term: self.hard_state.current_term,
                        leader_id: self.config.node_id,
                        meta: meta.clone(),
                        chunk: crate::SnapshotChunk {
                            offset: offset as u64,
                            bytes: chunk_bytes,
                            done,
                        },
                    }),
                )
                .await?;

            loop {
                let Some(inbound) = self.transport.recv_timeout(self.rpc_timeout()).await? else {
                    return Err(RaftError::NoQuorum);
                };
                match inbound.message {
                    crate::RaftMessageView::InstallSnapshotResp(resp) if inbound.from == peer => {
                        if resp.term() > self.hard_state.current_term {
                            self.step_down(resp.term())?;
                            return Err(RaftError::TermChanged {
                                current: self.hard_state.current_term,
                            });
                        }
                        offset = resp.next_offset() as usize;
                        if resp.accepted() && (done || offset >= total_len) {
                            info!(
                                node_id = self.config.node_id.0,
                                peer = peer.0,
                                bytes = total_len,
                                last_included_index = meta.last_included_index.0,
                                "snapshot installed"
                            );
                            return Ok(());
                        }
                        break;
                    }
                    message => {
                        self.handle_inbound(crate::InboundRaftMessageView {
                            from: inbound.from,
                            message,
                        })
                        .await?
                    }
                }
            }
        }
    }

    fn step_down(&mut self, new_term: Term) -> Result<(), RaftError> {
        self.hard_state.current_term = new_term;
        self.hard_state.voted_for = None;
        self.storage.save_hard_state(&self.hard_state)?;
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.peer_progress.clear();
        self.pending_snapshots.clear();
        info!(
            node_id = self.config.node_id.0,
            new_term = new_term.0,
            "stepped down"
        );
        Ok(())
    }

    fn term_at(&self, index: LogIndex) -> Result<Term, RaftError> {
        if index.0 == 0 {
            return Ok(Term(0));
        }
        self.storage
            .entry_at(index)?
            .map(|entry| entry.term)
            .ok_or_else(|| RaftError::CorruptLog(format!("missing term at index {}", index.0)))
    }

    fn initialize_leader_progress(&mut self) -> Result<(), RaftError> {
        self.peer_progress.clear();
        let last_index = self.storage.last_log_position()?.0;
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|peer| *peer != self.config.node_id)
        {
            self.peer_progress.insert(
                peer,
                PeerProgress {
                    next_index: LogIndex(last_index.0 + 1),
                    match_index: LogIndex(0),
                },
            );
        }
        Ok(())
    }

    fn ensure_leader_progress_initialized(&mut self) -> Result<(), RaftError> {
        if self.peer_progress.is_empty() {
            self.initialize_leader_progress()?;
        }
        Ok(())
    }

    fn build_append_for_peer(&mut self, peer: PeerId) -> Result<AppendEntries, RaftError> {
        let progress =
            self.peer_progress.get(&peer).copied().ok_or_else(|| {
                RaftError::Protocol(format!("missing progress for peer {}", peer.0))
            })?;
        let prev_log_index = LogIndex(progress.next_index.0.saturating_sub(1));
        let prev_log_term = self.term_at(prev_log_index)?;
        self.scratch_entries.clear();
        self.storage.read_entries(
            progress.next_index,
            LogIndex(u64::MAX),
            &mut self.scratch_entries,
        )?;
        let max_entries = self.config.limits.append_batch_entries.max(1);
        let max_bytes = self.config.limits.append_batch_bytes.max(1);
        let mut taken = 0usize;
        let mut payload_bytes = 0usize;
        for entry in &self.scratch_entries {
            let next_bytes = payload_bytes.saturating_add(entry.payload.0.len());
            if taken >= max_entries || (taken > 0 && next_bytes > max_bytes) {
                break;
            }
            payload_bytes = next_bytes;
            taken += 1;
        }
        self.scratch_entries.truncate(taken);
        AppendEntries::new(
            self.hard_state.current_term,
            self.config.node_id,
            prev_log_index,
            prev_log_term,
            self.hard_state.commit_index,
            &self.scratch_entries,
        )
    }

    async fn send_best_effort(&self, peer: PeerId, msg: RaftMessage, phase: &str) -> bool {
        let send_started = Instant::now();
        match self.transport.send(peer, msg).await {
            Ok(()) => {
                trace_log(
                    self.config.node_id,
                    format!(
                        "send_best_effort phase={} peer={} ok send_us={}",
                        phase,
                        peer.0,
                        send_started.elapsed().as_micros()
                    ),
                );
                true
            }
            Err(err) => {
                trace_log(
                    self.config.node_id,
                    format!(
                        "send_best_effort phase={} peer={} err={} send_us={}",
                        phase,
                        peer.0,
                        err,
                        send_started.elapsed().as_micros()
                    ),
                );
                warn!(
                    node_id = self.config.node_id.0,
                    peer = peer.0,
                    phase,
                    error = %err,
                    "peer send failed"
                );
                false
            }
        }
    }

    async fn send_append_attempt(
        &mut self,
        peer: PeerId,
        attempt: u64,
    ) -> Result<Option<LogIndex>, RaftError> {
        let build_started = Instant::now();
        let msg = self.build_append_for_peer(peer)?;
        let entry_count = msg.entry_count();
        let mut payload_bytes = 0usize;
        let mut sent_last_index = msg.prev_log_index();
        if trace_enabled() {
            for entry in msg.entries()? {
                payload_bytes += entry.payload().len();
                sent_last_index = entry.index();
            }
        } else if let Some(last) = msg.entries()?.last() {
            sent_last_index = last.index();
        }
        trace_log(
            self.config.node_id,
            format!(
                "replicate_peer_until peer={} attempt={} build_append prev={} sent_last={} entries={} payload_bytes={} build_us={}",
                peer.0,
                attempt,
                msg.prev_log_index().0,
                sent_last_index.0,
                entry_count,
                payload_bytes,
                build_started.elapsed().as_micros()
            ),
        );
        if !self
            .send_best_effort(peer, RaftMessage::AppendEntries(msg), "append_entries")
            .await
        {
            trace_log(
                self.config.node_id,
                format!(
                    "replicate_peer_until peer={} attempt={} send_failed",
                    peer.0, attempt
                ),
            );
            return Ok(None);
        }
        Ok(Some(sent_last_index))
    }

    async fn advance_append_replication(
        &mut self,
        peer: PeerId,
        target_index: LogIndex,
        state: AppendAttemptState,
        resp: AppendEntriesRespView,
    ) -> Result<AppendAdvance, RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
            return Err(RaftError::TermChanged {
                current: self.hard_state.current_term,
            });
        }

        let progress = self
            .peer_progress
            .get_mut(&peer)
            .ok_or(RaftError::PeerUnknown(peer))?;

        if resp.success() {
            if resp.match_index() < state.sent_last_index {
                trace_log(
                    self.config.node_id,
                    format!(
                        "replicate_peer_until peer={} attempt={} stale_ack match_index={} sent_last={}",
                        peer.0,
                        state.attempts,
                        resp.match_index().0,
                        state.sent_last_index.0
                    ),
                );
                return Ok(AppendAdvance::Ignored);
            }

            progress.match_index = resp.match_index();
            progress.next_index = LogIndex(resp.match_index().0 + 1);
            trace_log(
                self.config.node_id,
                format!(
                    "replicate_peer_until peer={} attempt={} ack match_index={}",
                    peer.0,
                    state.attempts,
                    resp.match_index().0
                ),
            );
            debug!(
                node_id = self.config.node_id.0,
                peer = peer.0,
                match_index = resp.match_index().0,
                next_index = progress.next_index.0,
                "append acknowledged"
            );
            if progress.match_index >= target_index {
                return Ok(AppendAdvance::Completed);
            }
        } else {
            if resp.match_index() < progress.match_index {
                trace_log(
                    self.config.node_id,
                    format!(
                        "replicate_peer_until peer={} attempt={} stale_reject match_index={} known_match={}",
                        peer.0,
                        state.attempts,
                        resp.match_index().0,
                        progress.match_index.0
                    ),
                );
                return Ok(AppendAdvance::Ignored);
            }

            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp.match_index().0.saturating_add(1))
                    .max(1),
            );
            trace_log(
                self.config.node_id,
                format!(
                    "replicate_peer_until peer={} attempt={} reject match_index={} retry_next_index={}",
                    peer.0,
                    state.attempts,
                    resp.match_index().0,
                    progress.next_index.0
                ),
            );
            warn!(
                node_id = self.config.node_id.0,
                peer = peer.0,
                retry_next_index = progress.next_index.0,
                "append rejected, backing off next_index"
            );
        }

        let next_attempt = state.attempts + 1;
        let Some(sent_last_index) = self.send_append_attempt(peer, next_attempt).await? else {
            return Ok(AppendAdvance::Dropped);
        };
        Ok(AppendAdvance::Retry(AppendAttemptState {
            attempts: next_attempt,
            sent_last_index,
        }))
    }

    async fn handle_request_vote(&mut self, msg: RequestVoteView) -> Result<(), RaftError> {
        if msg.term().0 > self.hard_state.current_term.0 {
            self.step_down(msg.term())?;
        }

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;
        let candidate_up_to_date = msg.last_log_term().0 > last_log_term.0
            || (msg.last_log_term() == last_log_term && msg.last_log_index() >= last_log_index);

        let can_vote = msg.term() == self.hard_state.current_term
            && candidate_up_to_date
            && self
                .hard_state
                .voted_for
                .map(|voted| voted == msg.candidate_id())
                .unwrap_or(true);

        if can_vote {
            self.hard_state.voted_for = Some(msg.candidate_id());
            self.storage.save_hard_state(&self.hard_state)?;
        }

        self.transport
            .send(
                msg.from(),
                RaftMessage::RequestVoteResp(RequestVoteResp {
                    term: self.hard_state.current_term,
                    vote_granted: can_vote,
                }),
            )
            .await?;
        Ok(())
    }

    async fn handle_request_vote_response(
        &mut self,
        resp: RequestVoteRespView,
    ) -> Result<(), RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
        }
        Ok(())
    }

    async fn handle_append_entries(&mut self, msg: AppendEntriesView) -> Result<(), RaftError> {
        let started = Instant::now();
        if trace_enabled() {
            let mut payload_bytes = 0usize;
            for entry in msg.entries()? {
                payload_bytes += entry.payload().len();
            }
            trace_log(
                self.config.node_id,
                format!(
                    "handle_append_entries from={} term={} prev={} entries={} payload_bytes={} leader_commit={}",
                    msg.leader_id().0,
                    msg.term().0,
                    msg.prev_log_index().0,
                    msg.entry_count(),
                    payload_bytes,
                    msg.leader_commit().0
                ),
            );
        }
        if msg.term().0 < self.hard_state.current_term.0 {
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::AppendEntriesResp(AppendEntriesResp {
                        term: self.hard_state.current_term,
                        success: false,
                        match_index: self.storage.last_log_position()?.0,
                    }),
                )
                .await?;
            return Ok(());
        }

        if msg.term().0 > self.hard_state.current_term.0 {
            self.step_down(msg.term())?;
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(msg.leader_id());

        let prev_ok = if msg.prev_log_index().0 == 0 {
            true
        } else {
            self.storage
                .entry_at(msg.prev_log_index())?
                .map(|entry| entry.term == msg.prev_log_term())
                .unwrap_or(false)
        };

        if !prev_ok {
            warn!(
                node_id = self.config.node_id.0,
                leader = msg.leader_id().0,
                prev_index = msg.prev_log_index().0,
                "append rejected due to log mismatch"
            );
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::AppendEntriesResp(AppendEntriesResp {
                        term: self.hard_state.current_term,
                        success: false,
                        match_index: self.storage.last_log_position()?.0,
                    }),
                )
                .await?;
            return Ok(());
        }

        let mut appended = 0usize;
        let append_started = Instant::now();
        let entry_count = msg.entry_count();
        let mut append_from = entry_count;
        let mut must_truncate = false;
        for (idx, incoming) in msg.entries()?.enumerate() {
            match self.storage.entry_at(incoming.index())? {
                Some(local) if local.term == incoming.term() => {}
                Some(_) => {
                    self.storage.truncate_suffix(incoming.index())?;
                    append_from = idx;
                    must_truncate = true;
                    break;
                }
                None => {
                    append_from = idx;
                    break;
                }
            }
        }
        if append_from < entry_count {
            self.scratch_entries.clear();
            for entry in msg.entries()?.skip(append_from) {
                self.scratch_entries.push(entry.to_owned());
            }
            appended = self.scratch_entries.len();
            self.storage.append_entries(&self.scratch_entries)?;
        } else if must_truncate {
            appended = 0;
        }
        trace_log(
            self.config.node_id,
            format!(
                "handle_append_entries appended={} append_us={}",
                appended,
                append_started.elapsed().as_micros()
            ),
        );

        let last_log_index = self.storage.last_log_position()?.0;
        if msg.leader_commit() > self.hard_state.commit_index {
            let save_started = Instant::now();
            self.hard_state.commit_index = LogIndex(msg.leader_commit().0.min(last_log_index.0));
            self.storage.save_hard_state(&self.hard_state)?;
            trace_log(
                self.config.node_id,
                format!(
                    "handle_append_entries commit_advance commit_index={} save_us={}",
                    self.hard_state.commit_index.0,
                    save_started.elapsed().as_micros()
                ),
            );
        }

        let respond_started = Instant::now();
        self.transport
            .send(
                msg.from(),
                RaftMessage::AppendEntriesResp(AppendEntriesResp {
                    term: self.hard_state.current_term,
                    success: true,
                    match_index: last_log_index,
                }),
            )
            .await?;
        trace_log(
            self.config.node_id,
            format!(
                "handle_append_entries response_sent match_index={} respond_us={} total_us={}",
                last_log_index.0,
                respond_started.elapsed().as_micros(),
                started.elapsed().as_micros()
            ),
        );
        debug!(
            node_id = self.config.node_id.0,
            leader = msg.leader_id().0,
            entries = appended,
            commit_index = self.hard_state.commit_index.0,
            "append accepted"
        );
        Ok(())
    }

    async fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesRespView,
    ) -> Result<(), RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
            return Ok(());
        }
        if !self.is_leader() {
            return Ok(());
        }
        let Some(progress) = self.peer_progress.get_mut(&resp.from()) else {
            return Ok(());
        };
        if resp.success() {
            if resp.match_index() > progress.match_index {
                progress.match_index = resp.match_index();
            }
            let next_index = LogIndex(resp.match_index().0.saturating_add(1));
            if next_index > progress.next_index {
                progress.next_index = next_index;
            }
        } else if resp.match_index() >= progress.match_index {
            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp.match_index().0.saturating_add(1))
                    .max(1),
            );
        }
        Ok(())
    }

    async fn drain_inbound_ready(&mut self) -> Result<(), RaftError> {
        loop {
            let Some(inbound) = self.transport.recv_timeout(Duration::ZERO).await? else {
                return Ok(());
            };
            self.handle_inbound(inbound).await?;
        }
    }

    async fn handle_install_snapshot(&mut self, msg: InstallSnapshotView) -> Result<(), RaftError> {
        if msg.term() < self.hard_state.current_term {
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::InstallSnapshotResp(crate::InstallSnapshotResp {
                        term: self.hard_state.current_term,
                        accepted: false,
                        next_offset: 0,
                    }),
                )
                .await?;
            return Ok(());
        }

        if msg.term() > self.hard_state.current_term {
            self.step_down(msg.term())?;
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(msg.leader_id());

        let meta = msg.meta();

        let pending = self
            .pending_snapshots
            .entry(msg.from())
            .or_insert_with(|| PendingSnapshot {
                meta: meta.clone(),
                bytes: Vec::new(),
            });

        if msg.offset() == 0 || pending.meta != meta {
            pending.meta = meta.clone();
            pending.bytes.clear();
        }

        if pending.bytes.len() as u64 != msg.offset() {
            let next_offset = pending.bytes.len() as u64;
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::InstallSnapshotResp(crate::InstallSnapshotResp {
                        term: self.hard_state.current_term,
                        accepted: false,
                        next_offset,
                    }),
                )
                .await?;
            return Ok(());
        }

        pending.bytes.extend_from_slice(msg.chunk_bytes().as_ref());
        let next_offset = pending.bytes.len() as u64;

        if msg.done() {
            let completed = self
                .pending_snapshots
                .remove(&msg.from())
                .ok_or_else(|| RaftError::Snapshot("missing pending snapshot".into()))?;
            self.storage
                .save_snapshot(&completed.meta, &completed.bytes)?;
            if self.hard_state.commit_index < completed.meta.last_included_index {
                self.hard_state.commit_index = completed.meta.last_included_index;
                self.storage.save_hard_state(&self.hard_state)?;
            }
        }

        self.transport
            .send(
                msg.from(),
                RaftMessage::InstallSnapshotResp(crate::InstallSnapshotResp {
                    term: self.hard_state.current_term,
                    accepted: true,
                    next_offset,
                }),
            )
            .await?;
        Ok(())
    }

    async fn handle_install_snapshot_response(
        &mut self,
        resp: InstallSnapshotRespView,
    ) -> Result<(), RaftError> {
        if resp.term() > self.hard_state.current_term {
            self.step_down(resp.term())?;
        }
        Ok(())
    }
    pub async fn replicate_batch_async(
        &mut self,
        payloads: &[Bytes],
    ) -> Result<Vec<LogIndex>, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|leader_id| LeaderHint { leader_id }),
            });
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            self.scratch_entries.push(LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload.clone()),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }

        self.storage.append_entries(&self.scratch_entries)?;

        // Broadcast a todos sin esperar quórum
        self.scratch_peers.clear();
        for peer in self.config.peers.iter().copied() {
            if peer != self.config.node_id {
                self.scratch_peers.push(peer);
            }
        }

        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            let _ = self.send_append_attempt(peer, 1).await;
        }

        Ok(self.scratch_indexes.clone())
    }
}

struct NodeDispatchResponder<'a, T> {
    transport: &'a T,
    target: PeerId,
}

#[async_trait::async_trait]
impl<T> DispatchResponder for NodeDispatchResponder<'_, T>
where
    T: RaftTransport,
{
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        self.transport
            .send(
                self.target,
                RaftMessage::CustomResponse(RaftCustomResponse {
                    bytes: encode_dispatch_response(&response),
                }),
            )
            .await
    }
}

struct LocalDispatchResponder<'a> {
    local_peer: PeerId,
    command: u8,
    pending: &'a HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    tx_id: u64,
}

impl<'a> LocalDispatchResponder<'a> {
    fn new(
        local_peer: PeerId,
        command: u8,
        pending: &'a HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
        tx_id: u64,
    ) -> Self {
        Self {
            local_peer,
            command,
            pending,
            tx_id,
        }
    }
}

#[async_trait::async_trait]
impl DispatchResponder for LocalDispatchResponder<'_> {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        if let Some(pending) = self.pending.get(&self.tx_id) {
            pending.on_response(
                self.local_peer,
                DispatchResponse {
                    tx_id: response.tx_id,
                    command: self.command,
                    kind: response.kind,
                    payload: response.payload,
                },
            )?;
        }
        Ok(())
    }
}

struct NodeDispatchRequester<'a, T> {
    transport: &'a T,
    target: PeerId,
    timeout: Duration,
}

#[async_trait::async_trait]
impl<T> DispatchRequester for NodeDispatchRequester<'_, T>
where
    T: RaftTransport,
{
    async fn request(&self, command: u8, payload: Bytes) -> Result<Bytes, RaftError> {
        let spec = DispatchSpec::new(
            command,
            identity_bytes,
            identity_bytes_decode,
            identity_bytes,
            identity_bytes_decode,
        );
        let envelope = spec.dispatch(payload).build()?;
        let tx_id = envelope.tx_id();

        self.transport
            .send(
                self.target,
                RaftMessage::Custom(RaftCustomMessage {
                    bytes: envelope.into_bytes(),
                }),
            )
            .await?;

        loop {
            let Some(inbound) = self.transport.recv_timeout(self.timeout).await? else {
                return Err(RaftError::Dispatch("custom request timed out".into()));
            };

            match inbound.message {
                crate::RaftMessageView::CustomResponse(response)
                    if inbound.from == self.target && response.tx_id() == tx_id =>
                {
                    return Ok(response.response().body_bytes());
                }
                _ => continue,
            }
        }
    }
}

fn identity_bytes(bytes: &Bytes) -> Result<Bytes, RaftError> {
    Ok(bytes.clone())
}

fn identity_bytes_decode(bytes: &[u8]) -> Result<Bytes, RaftError> {
    Ok(Bytes::copy_from_slice(bytes))
}

fn quorum(nodes: usize) -> usize {
    (nodes / 2) + 1
}
