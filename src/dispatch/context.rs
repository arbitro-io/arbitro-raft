use async_trait::async_trait;
use bytes::Bytes;

use crate::dispatch::DispatchSpec;
use crate::RaftError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchResponseKind {
    Accepted,
    Rejected,
    Progress,
    Failed,
}

#[derive(Debug, Clone)]
pub struct DispatchResponse {
    pub tx_id: u64,
    pub command: u8,
    pub kind: DispatchResponseKind,
    pub payload: Bytes,
}

#[async_trait]
pub trait DispatchResponder: Send + Sync {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError>;
}

#[async_trait]
pub trait DispatchRequester: Send + Sync {
    async fn request(&self, command: u8, payload: Bytes) -> Result<Bytes, RaftError>;
}

#[derive(Clone, Copy)]
pub struct DispatchStreamView<'a> {
    requester: &'a dyn DispatchRequester,
}

impl<'a> DispatchStreamView<'a> {
    pub fn new(requester: &'a dyn DispatchRequester) -> Self {
        Self { requester }
    }

    pub async fn request_bytes(&self, command: u8, payload: Bytes) -> Result<Bytes, RaftError> {
        self.requester.request(command, payload).await
    }

    pub async fn request_with<P, R>(
        &self,
        spec: &DispatchSpec<P, R>,
        params: &P,
    ) -> Result<R, RaftError> {
        let response = self
            .requester
            .request(spec.command(), spec.encode_params(params)?)
            .await?;
        spec.decode_response(response.as_ref())
    }
}

pub struct DispatchContextView<'a> {
    tx_id: u64,
    command: u8,
    responder: &'a dyn DispatchResponder,
    requester: Option<&'a dyn DispatchRequester>,
}

impl<'a> DispatchContextView<'a> {
    pub fn new(tx_id: u64, command: u8, responder: &'a dyn DispatchResponder) -> Self {
        Self {
            tx_id,
            command,
            responder,
            requester: None,
        }
    }

    pub fn with_requester(
        tx_id: u64,
        command: u8,
        responder: &'a dyn DispatchResponder,
        requester: &'a dyn DispatchRequester,
    ) -> Self {
        Self {
            tx_id,
            command,
            responder,
            requester: Some(requester),
        }
    }

    pub fn tx_id(&self) -> u64 {
        self.tx_id
    }

    pub fn command(&self) -> u8 {
        self.command
    }

    pub fn stream(&self) -> Option<DispatchStreamView<'a>> {
        self.requester.map(DispatchStreamView::new)
    }

    pub async fn request_bytes(&self, command: u8, payload: Bytes) -> Result<Bytes, RaftError> {
        let Some(requester) = self.requester else {
            return Err(RaftError::Dispatch("dispatch stream unavailable".into()));
        };
        requester.request(command, payload).await
    }

    pub async fn request_with<P, R>(
        &self,
        spec: &DispatchSpec<P, R>,
        params: &P,
    ) -> Result<R, RaftError> {
        let response = self
            .request_bytes(spec.command(), spec.encode_params(params)?)
            .await?;
        spec.decode_response(response.as_ref())
    }

    pub async fn accept_bytes(&self, payload: Bytes) -> Result<(), RaftError> {
        self.responder
            .send_response(DispatchResponse {
                tx_id: self.tx_id,
                command: self.command,
                kind: DispatchResponseKind::Accepted,
                payload,
            })
            .await
    }

    pub async fn accept_with<P, R>(
        &self,
        spec: &DispatchSpec<P, R>,
        value: &R,
    ) -> Result<(), RaftError> {
        self.accept_bytes(spec.encode_response(value)?).await
    }

    pub async fn reject(&self, reason: impl Into<String>) -> Result<(), RaftError> {
        self.responder
            .send_response(DispatchResponse {
                tx_id: self.tx_id,
                command: self.command,
                kind: DispatchResponseKind::Rejected,
                payload: Bytes::from(reason.into()),
            })
            .await
    }

    pub async fn progress_bytes(&self, payload: Bytes) -> Result<(), RaftError> {
        self.responder
            .send_response(DispatchResponse {
                tx_id: self.tx_id,
                command: self.command,
                kind: DispatchResponseKind::Progress,
                payload,
            })
            .await
    }

    pub async fn fail(&self, error: impl Into<String>) -> Result<(), RaftError> {
        self.responder
            .send_response(DispatchResponse {
                tx_id: self.tx_id,
                command: self.command,
                kind: DispatchResponseKind::Failed,
                payload: Bytes::from(error.into()),
            })
            .await
    }
}
