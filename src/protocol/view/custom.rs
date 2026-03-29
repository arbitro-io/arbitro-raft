use crate::{DispatchResponseView, DispatchView, PeerId, RaftCustomMessage, RaftCustomResponse};

// ── RaftCustomMessageView ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RaftCustomMessageView {
    pub(crate) from:     PeerId,
    pub(crate) dispatch: DispatchView,
}

impl RaftCustomMessageView {
    pub(crate) fn new(from: PeerId, dispatch: DispatchView) -> Self { Self { from, dispatch } }

    #[inline] pub fn from(&self) -> PeerId        { self.from }
    #[inline] pub fn command(&self) -> u8         { self.dispatch.command() }
    #[inline] pub fn tx_id(&self) -> u64          { self.dispatch.tx_id() }
    #[inline] pub fn body(&self) -> &[u8]         { self.dispatch.body() }
    #[inline] pub fn dispatch(&self) -> &DispatchView { &self.dispatch }

    pub fn to_owned(&self) -> RaftCustomMessage {
        RaftCustomMessage { bytes: self.dispatch.frame_bytes().clone() }
    }
}

// ── RaftCustomResponseView ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RaftCustomResponseView {
    pub(crate) from:     PeerId,
    pub(crate) response: DispatchResponseView,
}

impl RaftCustomResponseView {
    pub(crate) fn new(from: PeerId, response: DispatchResponseView) -> Self {
        Self { from, response }
    }

    #[inline] pub fn from(&self) -> PeerId                    { self.from }
    #[inline] pub fn tx_id(&self) -> u64                      { self.response.tx_id() }
    #[inline] pub fn command(&self) -> u8                     { self.response.command() }
    #[inline] pub fn response(&self) -> &DispatchResponseView { &self.response }

    pub fn to_owned(&self) -> RaftCustomResponse {
        RaftCustomResponse { bytes: self.response.frame_bytes().clone() }
    }
}
