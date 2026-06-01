use super::RaftNode;
use crate::dispatch::encode_dispatch_response;
use crate::dispatch::DispatchResponseView;
use crate::{
    DispatchContextView, DispatchHandle, DispatchNodeRole, DispatchResponder, DispatchResponse,
    DispatchResponseKind, DispatchRoute, DispatchScope, DispatchSpec, DispatchTx, PeerId,
    RaftError, RaftMessage,
};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

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
    fn is_ready(&self) -> bool {
        self.handle.is_ready()
    }
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
        self.populate_dispatch_targets(envelope.options().scope);
        let (handle, tx) = envelope.begin(
            self.scratch_peers.iter().copied(),
            spec.decode_response_fn(),
        );

        self.pending_custom.insert(
            envelope.tx_id(),
            Box::new(PendingCustomTx {
                handle: handle.clone(),
                tx,
            }),
        );

        let msg = RaftMessage::Custom(envelope.bytes());
        let frame = crate::protocol::encode_message_to_bytes(self.config.node_id, &msg)?;
        let route = self.route_for_peer(self.config.node_id);
        let responder = LocalDispatchResponder {
            local_peer: self.config.node_id,
            command: envelope.command(),
            pending: &self.pending_custom,
            tx_id: envelope.tx_id(),
        };

        let mut sends = Vec::with_capacity(self.scratch_peers.len());
        let mut local_invoke = None;

        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            if peer == self.config.node_id {
                local_invoke = Some(self.custom_registry.invoke_bytes_scoped(
                    envelope.bytes(),
                    &responder,
                    route,
                ));
            } else {
                sends.push(self.transport.send_frame_owned(peer, frame.clone()));
            }
        }

        let net_results = futures::future::join_all(sends);
        if let Some(local) = local_invoke {
            let (local_res, net) = futures::future::join(local, net_results).await;
            local_res?;
            for res in net {
                if let Err(e) = res {
                    tracing::error!(error = %e, "dispatch fan-out failed");
                }
            }
        } else {
            for res in net_results.await {
                if let Err(e) = res {
                    tracing::error!(error = %e, "dispatch fan-out failed");
                }
            }
        }

        Ok(handle)
    }

    pub(crate) async fn handle_custom_message(
        &mut self,
        from: PeerId,
        payload: &[u8],
    ) -> Result<(), RaftError> {
        let route = DispatchRoute {
            role: if self.is_leader() {
                DispatchNodeRole::Leader
            } else {
                DispatchNodeRole::Follower
            },
            is_origin: from == self.config.node_id,
        };
        let responder = NodeDispatchResponder {
            transport: &self.transport,
            target: from,
            node_id: self.config.node_id,
        };
        self.custom_registry
            .invoke_bytes_scoped(payload, &responder, route)
            .await?;
        Ok(())
    }

    pub(crate) async fn handle_custom_response(
        &mut self,
        from: PeerId,
        payload: &[u8],
    ) -> Result<(), RaftError> {
        let view = DispatchResponseView::parse(payload)?;

        let response = DispatchResponse {
            tx_id: view.tx_id(),
            command: view.command(),
            kind: view.kind(),
            payload: view.body_bytes().to_vec(),
        };
        let ready = if let Some(pending) = self.pending_custom.get(&view.tx_id()) {
            pending.on_response(from, response)?;
            pending.is_ready()
        } else {
            false
        };
        if ready {
            self.pending_custom.remove(&view.tx_id());
        }
        Ok(())
    }

    fn route_for_peer(&self, peer: PeerId) -> DispatchRoute {
        if peer == self.config.node_id {
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
            let is_leader_peer = self.soft_state.leader_id == Some(peer);
            DispatchRoute {
                role: if is_leader_peer {
                    DispatchNodeRole::Leader
                } else {
                    DispatchNodeRole::Follower
                },
                is_origin: false,
            }
        }
    }

    fn populate_dispatch_targets(&mut self, scope: DispatchScope) {
        self.scratch_peers.clear();
        for i in 0..self.config.peers.len() {
            let peer = self.config.peers[i];
            let matches = if peer == self.config.node_id && self.config.peers.len() > 1 && !self.is_leader() {
                matches!(scope, DispatchScope::LocalOnly)
            } else {
                scope.allows(self.route_for_peer(peer))
            };
            if matches {
                self.scratch_peers.push(peer);
            }
        }
    }
}

pub(super) struct NodeDispatchResponder<'a, T> {
    pub(super) transport: &'a T,
    pub(super) target: PeerId,
    pub(super) node_id: PeerId,
}

#[async_trait]
impl<T: crate::RaftTransport> DispatchResponder for NodeDispatchResponder<'_, T> {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        let inner = encode_dispatch_response(&response);
        let msg = RaftMessage::CustomResponse(&inner);

        let mut header_buf = [0u8; 128];
        let mut vectors = Vec::with_capacity(4);

        crate::protocol::encode_message_vectored(
            self.node_id,
            &msg,
            &mut header_buf,
            &mut vectors,
        )?;

        self.transport.send_vectored(self.target, &vectors).await
    }
}

pub(super) struct LocalDispatchResponder<'a> {
    pub(super) local_peer: PeerId,
    pub(super) command: u8,
    pub(super) pending:
        &'a std::collections::HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    pub(super) tx_id: u64,
}

#[async_trait]
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
