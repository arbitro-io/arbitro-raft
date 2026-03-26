use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread;

use async_trait::async_trait;
use bytes::Bytes;

use crate::dispatch::{
    DispatchAckPolicy, DispatchEnvelope, DispatchFailPolicy, DispatchOptions, DispatchResponder,
    DispatchResponse, DispatchResponseKind,
};
use crate::{PeerId, RaftError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchFailure {
    Timeout,
    Impossible(String),
    Failed(String),
}

impl From<DispatchFailure> for RaftError {
    fn from(value: DispatchFailure) -> Self {
        match value {
            DispatchFailure::Timeout => RaftError::Dispatch("dispatch timed out".into()),
            DispatchFailure::Impossible(msg) | DispatchFailure::Failed(msg) => {
                RaftError::Dispatch(msg)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum DispatchPeerState<R> {
    Pending,
    Progress(Bytes),
    Accepted(R),
    Rejected(Bytes),
    Failed(Bytes),
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct DispatchPeerResult<R> {
    pub peer: PeerId,
    pub state: DispatchPeerState<R>,
}

#[derive(Debug, Clone)]
pub struct DispatchResult<R> {
    pub tx_id: u64,
    pub command: u8,
    pub options: DispatchOptions,
    pub peers: Vec<DispatchPeerResult<R>>,
}

impl<R> DispatchResult<R> {
    pub fn accepted_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| matches!(peer.state, DispatchPeerState::Accepted(_)))
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| {
                matches!(
                    peer.state,
                    DispatchPeerState::Rejected(_) | DispatchPeerState::Failed(_)
                )
            })
            .count()
    }

    pub fn disconnected_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| matches!(peer.state, DispatchPeerState::Disconnected))
            .count()
    }
}

#[derive(Debug, Clone)]
enum DispatchCompletion<R> {
    Succeeded(DispatchResult<R>),
    Failed(DispatchFailure),
}

#[derive(Debug, Clone)]
struct DispatchPeerSlot<R> {
    peer: PeerId,
    state: DispatchPeerState<R>,
}

#[derive(Debug)]
struct DispatchState<R> {
    tx_id: u64,
    command: u8,
    options: DispatchOptions,
    peers: Vec<DispatchPeerSlot<R>>,
    completion: Option<DispatchCompletion<R>>,
    wakers: Vec<Waker>,
}

impl<R: Clone> DispatchState<R> {
    fn result_snapshot(&self) -> DispatchResult<R> {
        DispatchResult {
            tx_id: self.tx_id,
            command: self.command,
            options: self.options,
            peers: self
                .peers
                .iter()
                .map(|peer| DispatchPeerResult {
                    peer: peer.peer,
                    state: peer.state.clone(),
                })
                .collect(),
        }
    }

    fn required_accepts(&self) -> usize {
        let active = self
            .peers
            .iter()
            .filter(|peer| !matches!(peer.state, DispatchPeerState::Disconnected))
            .count();
        if active == 0 {
            return 0;
        }

        match self.options.ack_policy {
            DispatchAckPolicy::All => active,
            DispatchAckPolicy::Quorum => (active / 2) + 1,
            DispatchAckPolicy::AtLeast => {
                usize::min(active, self.options.ack_count.max(1) as usize)
            }
            DispatchAckPolicy::Percent => {
                let percent = usize::from(self.options.ack_percent.clamp(1, 100));
                usize::max(1, (active * percent).div_ceil(100))
            }
            DispatchAckPolicy::BestEffort => 1,
        }
    }

    fn allowed_failures(&self) -> usize {
        let active = self
            .peers
            .iter()
            .filter(|peer| !matches!(peer.state, DispatchPeerState::Disconnected))
            .count();

        match self.options.fail_policy {
            DispatchFailPolicy::AllowFailures => usize::MAX,
            DispatchFailPolicy::NoFailures | DispatchFailPolicy::FailFast => 0,
            DispatchFailPolicy::MaxFailures => self.options.fail_count as usize,
            DispatchFailPolicy::MaxFailurePercent => {
                (active * usize::from(self.options.fail_percent.min(100))) / 100
            }
        }
    }

    fn accepted_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| matches!(peer.state, DispatchPeerState::Accepted(_)))
            .count()
    }

    fn failed_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| {
                matches!(
                    peer.state,
                    DispatchPeerState::Rejected(_) | DispatchPeerState::Failed(_)
                )
            })
            .count()
    }

    fn pending_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|peer| {
                matches!(
                    peer.state,
                    DispatchPeerState::Pending | DispatchPeerState::Progress(_)
                )
            })
            .count()
    }

    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    fn evaluate(&mut self) {
        if self.completion.is_some() {
            return;
        }

        let required = self.required_accepts();
        let accepted = self.accepted_count();
        let failed = self.failed_count();
        let pending = self.pending_count();
        let allowed_failures = self.allowed_failures();
        let possible_accepts = accepted + pending;

        if required == 0 {
            self.completion = Some(DispatchCompletion::Succeeded(self.result_snapshot()));
            self.wake_all();
            return;
        }

        if accepted >= required {
            self.completion = Some(DispatchCompletion::Succeeded(self.result_snapshot()));
            self.wake_all();
            return;
        }

        if failed > allowed_failures {
            self.completion = Some(DispatchCompletion::Failed(DispatchFailure::Failed(
                format!(
                    "dispatch failure policy exceeded: failed={failed}, allowed={allowed_failures}"
                ),
            )));
            self.wake_all();
            return;
        }

        if possible_accepts < required {
            self.completion = Some(DispatchCompletion::Failed(DispatchFailure::Impossible(
                format!(
                    "dispatch can no longer reach required accepts: accepted={accepted}, pending={pending}, required={required}"
                ),
            )));
            self.wake_all();
        }
    }
}

struct DispatchShared<R> {
    inner: Arc<Mutex<DispatchState<R>>>,
}

impl<R> Clone for DispatchShared<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone)]
pub struct DispatchHandle<R> {
    shared: DispatchShared<R>,
}

impl<R: Clone> DispatchHandle<R> {
    pub fn tx_id(&self) -> u64 {
        self.shared.inner.lock().unwrap().tx_id
    }

    pub fn command(&self) -> u8 {
        self.shared.inner.lock().unwrap().command
    }

    pub fn is_ready(&self) -> bool {
        self.shared.inner.lock().unwrap().completion.is_some()
    }

    pub fn try_result(&self) -> Option<Result<DispatchResult<R>, RaftError>> {
        let guard = self.shared.inner.lock().unwrap();
        guard
            .completion
            .as_ref()
            .map(|completion| match completion {
                DispatchCompletion::Succeeded(result) => Ok(result.clone()),
                DispatchCompletion::Failed(failure) => Err(RaftError::from(failure.clone())),
            })
    }

    pub async fn wait(&self) -> Result<DispatchResult<R>, RaftError> {
        poll_fn(|cx| {
            let mut guard = self.shared.inner.lock().unwrap();
            match &guard.completion {
                Some(DispatchCompletion::Succeeded(result)) => {
                    std::task::Poll::Ready(Ok(result.clone()))
                }
                Some(DispatchCompletion::Failed(failure)) => {
                    std::task::Poll::Ready(Err(RaftError::from(failure.clone())))
                }
                None => {
                    guard.wakers.push(cx.waker().clone());
                    std::task::Poll::Pending
                }
            }
        })
        .await
    }
}

pub struct DispatchTx<R> {
    shared: DispatchShared<R>,
    decode_response: fn(&[u8]) -> Result<R, RaftError>,
}

impl<R> Clone for DispatchTx<R> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            decode_response: self.decode_response,
        }
    }
}

impl<R: Clone> DispatchTx<R> {
    fn update_peer_state(
        &self,
        peer: PeerId,
        next_state: impl FnOnce(&DispatchPeerState<R>) -> Result<DispatchPeerState<R>, RaftError>,
    ) -> Result<(), RaftError> {
        let mut guard = self.shared.inner.lock().unwrap();
        if guard.completion.is_some() {
            return Ok(());
        }
        let slot = guard
            .peers
            .iter_mut()
            .find(|entry| entry.peer == peer)
            .ok_or(RaftError::PeerUnknown(peer))?;
        slot.state = next_state(&slot.state)?;
        guard.evaluate();
        Ok(())
    }

    pub fn accept_raw(&self, peer: PeerId, payload: Bytes) -> Result<(), RaftError> {
        let value = (self.decode_response)(payload.as_ref())?;
        self.accept(peer, value)
    }

    pub fn accept(&self, peer: PeerId, value: R) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Accepted(value)))
    }

    pub fn reject(&self, peer: PeerId, reason: Bytes) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Rejected(reason)))
    }

    pub fn progress(&self, peer: PeerId, payload: Bytes) -> Result<(), RaftError> {
        self.update_peer_state(peer, |current| {
            if matches!(
                current,
                DispatchPeerState::Accepted(_)
                    | DispatchPeerState::Rejected(_)
                    | DispatchPeerState::Failed(_)
                    | DispatchPeerState::Disconnected
            ) {
                return Ok(current.clone());
            }
            Ok(DispatchPeerState::Progress(payload))
        })
    }

    pub fn fail(&self, peer: PeerId, error: Bytes) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Failed(error)))
    }

    pub fn disconnect(&self, peer: PeerId) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Disconnected))
    }
}

pub struct DispatchTxResponder<R> {
    peer: PeerId,
    tx: DispatchTx<R>,
}

impl<R> Clone for DispatchTxResponder<R> {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer,
            tx: self.tx.clone(),
        }
    }
}

impl<R> DispatchTxResponder<R> {
    pub fn new(peer: PeerId, tx: DispatchTx<R>) -> Self {
        Self { peer, tx }
    }
}

#[async_trait(?Send)]
impl<R: Clone> DispatchResponder for DispatchTxResponder<R> {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        match response.kind {
            DispatchResponseKind::Accepted => self.tx.accept_raw(self.peer, response.payload),
            DispatchResponseKind::Rejected => self.tx.reject(self.peer, response.payload),
            DispatchResponseKind::Progress => self.tx.progress(self.peer, response.payload),
            DispatchResponseKind::Failed => self.tx.fail(self.peer, response.payload),
        }
    }
}

impl<P, R> DispatchEnvelope<P, R>
where
    R: Clone + Send + 'static,
{
    pub fn begin(
        &self,
        targets: impl IntoIterator<Item = PeerId>,
        decode_response: fn(&[u8]) -> Result<R, RaftError>,
    ) -> (DispatchHandle<R>, DispatchTx<R>) {
        let shared = DispatchShared {
            inner: Arc::new(Mutex::new(DispatchState {
                tx_id: self.tx_id(),
                command: self.command(),
                options: self.options_internal(),
                peers: targets
                    .into_iter()
                    .map(|peer| DispatchPeerSlot {
                        peer,
                        state: DispatchPeerState::Pending,
                    })
                    .collect(),
                completion: None,
                wakers: Vec::new(),
            })),
        };

        if !self.options_internal().timeout.is_zero() {
            let weak = Arc::downgrade(&shared.inner);
            let timeout = self.options_internal().timeout;
            thread::spawn(move || {
                thread::sleep(timeout);
                if let Some(inner) = weak.upgrade() {
                    let mut guard = inner.lock().unwrap();
                    if guard.completion.is_none() {
                        guard.completion =
                            Some(DispatchCompletion::Failed(DispatchFailure::Timeout));
                        guard.wake_all();
                    }
                }
            });
        }

        let handle = DispatchHandle {
            shared: shared.clone(),
        };
        let tx = DispatchTx {
            shared,
            decode_response,
        };

        {
            let mut guard = handle.shared.inner.lock().unwrap();
            guard.evaluate();
        }

        (handle, tx)
    }
}
