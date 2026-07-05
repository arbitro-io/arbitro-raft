use std::future::poll_fn;

use crate::{PeerId, RaftError};

use super::state::{DispatchCompletion, DispatchShared};
use super::types::{DispatchPeerState, DispatchResult};

// ── DispatchHandle ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DispatchHandle<R> {
    pub(crate) shared: DispatchShared<R>,
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
        guard.completion.as_ref().map(|c| match c {
            DispatchCompletion::Succeeded(r) => Ok(r.clone()),
            DispatchCompletion::Failed(f) => Err(RaftError::from(f.clone())),
        })
    }

    pub async fn wait(&self) -> Result<DispatchResult<R>, RaftError> {
        poll_fn(|cx| {
            let mut guard = self.shared.inner.lock().unwrap();
            match &guard.completion {
                Some(DispatchCompletion::Succeeded(r)) => std::task::Poll::Ready(Ok(r.clone())),
                Some(DispatchCompletion::Failed(f)) => {
                    std::task::Poll::Ready(Err(RaftError::from(f.clone())))
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

// ── DispatchTx ────────────────────────────────────────────────────────────────

pub struct DispatchTx<R> {
    pub(crate) shared: DispatchShared<R>,
    pub(crate) decode_response: fn(&[u8]) -> Result<R, RaftError>,
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
            .find(|e| e.peer == peer)
            .ok_or(RaftError::PeerUnknown(peer))?;
        slot.state = next_state(&slot.state)?;
        guard.evaluate();
        Ok(())
    }

    pub fn accept_raw(&self, peer: PeerId, payload: Vec<u8>) -> Result<(), RaftError> {
        let value = (self.decode_response)(&payload)?;
        self.accept(peer, value)
    }

    pub fn accept(&self, peer: PeerId, value: R) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Accepted(value)))
    }

    pub fn reject(&self, peer: PeerId, reason: Vec<u8>) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Rejected(reason)))
    }

    pub fn progress(&self, peer: PeerId, payload: Vec<u8>) -> Result<(), RaftError> {
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

    pub fn fail(&self, peer: PeerId, error: Vec<u8>) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Failed(error)))
    }

    pub fn disconnect(&self, peer: PeerId) -> Result<(), RaftError> {
        self.update_peer_state(peer, |_| Ok(DispatchPeerState::Disconnected))
    }
}

// ── DispatchTxResponder ───────────────────────────────────────────────────────

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

#[async_trait::async_trait]
impl<R: Clone + Send + Sync> crate::dispatch::DispatchResponder for DispatchTxResponder<R> {
    async fn send_response(
        &self,
        response: crate::dispatch::DispatchResponse,
    ) -> Result<(), RaftError> {
        use crate::dispatch::DispatchResponseKind;
        match response.kind {
            DispatchResponseKind::Accepted => self.tx.accept_raw(self.peer, response.payload),
            DispatchResponseKind::Rejected => self.tx.reject(self.peer, response.payload),
            DispatchResponseKind::Progress => self.tx.progress(self.peer, response.payload),
            DispatchResponseKind::Failed => self.tx.fail(self.peer, response.payload),
        }
    }
}
