use std::future::Future;
use std::pin::Pin;
use async_trait::async_trait;
use crate::{
    DispatchContextView, DispatchHandle, DispatchNodeRole, DispatchRequester,
    DispatchResponder, DispatchResponse, DispatchResponseKind, DispatchRoute, DispatchSpec,
    DispatchTx, PeerId, RaftCustomMessageView, RaftCustomResponseView, RaftError, RaftMessage,
    RaftCustomResponse, encode_dispatch_response, RaftCustomMessage,
};
use super::RaftNode;

pub(crate) trait PendingCustomDispatch: Send + Sync {
    fn on_response(&self, peer: PeerId, response: DispatchResponse) -> Result<(), RaftError>;
    fn is_ready(&self) -> bool;
}

pub(crate) struct PendingCustomTx<R> {
    pub(crate) handle: DispatchHandle<R>,
    pub(crate) tx: DispatchTx<R>,
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
    fn is_ready(&self) -> bool { self.handle.is_ready() }
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(P, DispatchContextView<'a>) -> Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>> + Send + Sync + 'static,
    {
        self.custom_registry.on_with(spec, handler)
    }

    pub async fn dispatch<P, R>(&mut self, spec: DispatchSpec<P, R>, params: P) -> Result<DispatchHandle<R>, RaftError>
    where
        P: Send + 'static,
        R: Clone + Send + 'static,
    {
        let envelope = spec.dispatch(params).build()?;
        let targets = self.dispatch_targets(envelope.options().scope);
        let (handle, tx) = envelope.begin(targets.iter().copied(), spec.decode_response_fn());

        self.pending_custom.insert(envelope.tx_id(), Box::new(PendingCustomTx { handle: handle.clone(), tx }));

        for peer in targets {
            if peer == self.config.node_id {
                let route = DispatchRoute { role: if self.is_leader() { DispatchNodeRole::Leader } else { DispatchNodeRole::Follower }, is_origin: true };
                let responder = LocalDispatchResponder { local_peer: self.config.node_id, command: envelope.command(), pending: &self.pending_custom, tx_id: envelope.tx_id() };
                self.custom_registry.invoke_bytes_scoped(envelope.bytes().clone(), &responder, route).await?;
                continue;
            }
            self.transport.send(peer, RaftMessage::Custom(RaftCustomMessage { bytes: envelope.bytes().clone() })).await?;
        }
        Ok(handle)
    }

    pub(crate) async fn handle_custom_message(&mut self, msg: RaftCustomMessageView) -> Result<(), RaftError> {
        let route = DispatchRoute { role: if self.is_leader() { DispatchNodeRole::Leader } else { DispatchNodeRole::Follower }, is_origin: msg.from() == self.config.node_id };
        let responder = NodeDispatchResponder { transport: &self.transport, target: msg.from() };
        let requester = NodeDispatchRequester { transport: &self.transport, target: msg.from(), timeout: self.rpc_timeout() };
        self.custom_registry.invoke_bytes_with_scoped(msg.dispatch().frame_bytes().clone(), &responder, Some(&requester), route).await?;
        Ok(())
    }

    pub(crate) async fn handle_custom_response(&mut self, msg: RaftCustomResponseView) -> Result<(), RaftError> {
        let response = DispatchResponse { tx_id: msg.tx_id(), command: msg.command(), kind: msg.response().kind(), payload: msg.response().body_bytes() };
        let ready = if let Some(pending) = self.pending_custom.get(&msg.tx_id()) {
            pending.on_response(msg.from(), response)?;
            pending.is_ready()
        } else { false };
        if ready { self.pending_custom.remove(&msg.tx_id()); }
        Ok(())
    }

    pub(crate) fn rpc_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2)
    }

    fn dispatch_targets(&self, scope: crate::DispatchScope) -> Vec<PeerId> {
        self.config.peers.iter().copied().filter(|peer| {
            let route = if *peer == self.config.node_id {
                if self.config.peers.len() > 1 && !self.is_leader() { return matches!(scope, crate::DispatchScope::LocalOnly); }
                DispatchRoute { role: if self.is_leader() { DispatchNodeRole::Leader } else { DispatchNodeRole::Follower }, is_origin: true }
            } else if self.is_leader() { DispatchRoute::follower(false) } else {
                let is_leader = self.soft_state.leader_id == Some(*peer);
                DispatchRoute { role: if is_leader { DispatchNodeRole::Leader } else { DispatchNodeRole::Follower }, is_origin: false }
            };
            scope.allows(route)
        }).collect()
    }
}

pub(super) struct NodeDispatchResponder<'a, T> { pub(super) transport: &'a T, pub(super) target: PeerId }

#[async_trait]
impl<T: crate::RaftTransport> DispatchResponder for NodeDispatchResponder<'_, T> {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        self.transport.send(self.target, RaftMessage::CustomResponse(RaftCustomResponse { bytes: encode_dispatch_response(&response) })).await
    }
}

pub(super) struct LocalDispatchResponder<'a> {
    pub(super) local_peer: PeerId,
    pub(super) command: u8,
    pub(super) pending: &'a std::collections::HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    pub(super) tx_id: u64,
}

#[async_trait]
impl DispatchResponder for LocalDispatchResponder<'_> {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        if let Some(pending) = self.pending.get(&self.tx_id) {
            pending.on_response(self.local_peer, DispatchResponse { tx_id: response.tx_id, command: self.command, kind: response.kind, payload: response.payload })?;
        }
        Ok(())
    }
}

pub(super) struct NodeDispatchRequester<'a, T> { pub(super) transport: &'a T, pub(super) target: PeerId, pub(super) timeout: std::time::Duration }

#[async_trait]
impl<T: crate::RaftTransport> DispatchRequester for NodeDispatchRequester<'_, T> {
    async fn request(&self, _command: u8, _payload: bytes::Bytes) -> Result<bytes::Bytes, RaftError> {
        Err(RaftError::Protocol("dispatch request via RaftNode not yet implemented in requester shim".into()))
    }
}
