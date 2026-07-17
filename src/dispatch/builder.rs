use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::IntoBytes;

use crate::dispatch::spec::{
    DispatchAckPolicy, DispatchFailPolicy, DispatchOptions, DispatchScope,
};
use crate::dispatch::view::{
    DispatchFrameHeader, DispatchView, RAFT_DISPATCH_MAGIC, RAFT_DISPATCH_VERSION,
};
use crate::dispatch::{DispatchHandle, DispatchSpec, DispatchTx};
use crate::{PeerId, RaftError};

pub struct DispatchBuilder<'a, P, R> {
    spec: &'a DispatchSpec<P, R>,
    params: P,
    options: DispatchOptions,
}

pub struct DispatchEnvelope<P, R> {
    tx_id: u64,
    command: u8,
    bytes: Vec<u8>,
    options: DispatchOptions,
    _marker: PhantomData<fn(P) -> R>,
}

impl<P, R> DispatchEnvelope<P, R> {
    pub fn tx_id(&self) -> u64 {
        self.tx_id
    }

    pub(crate) fn options_internal(&self) -> DispatchOptions {
        self.options
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn command(&self) -> u8 {
        self.command
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn options(&self) -> DispatchOptions {
        self.options
    }

    pub fn view(&self) -> Result<DispatchView<'_>, RaftError> {
        DispatchView::parse(&self.bytes)
    }
}

impl<'a, P, R> DispatchBuilder<'a, P, R> {
    pub fn new(spec: &'a DispatchSpec<P, R>, params: P) -> Self {
        Self {
            spec,
            params,
            options: spec.defaults(),
        }
    }

    pub fn scope(mut self, scope: DispatchScope) -> Self {
        self.options.scope = scope;
        self
    }

    pub fn ack_policy(mut self, policy: DispatchAckPolicy) -> Self {
        self.options.ack.policy = policy;
        self
    }

    pub fn ack_count(mut self, count: u16) -> Self {
        self.options.ack.count = count.max(1);
        self
    }

    pub fn ack_percent(mut self, percent: u8) -> Self {
        self.options.ack.percent = percent.clamp(1, 100);
        self
    }

    pub fn fail_policy(mut self, policy: DispatchFailPolicy) -> Self {
        self.options.fail.policy = policy;
        self
    }

    pub fn fail_count(mut self, count: u16) -> Self {
        self.options.fail.count = count;
        self
    }

    pub fn fail_percent(mut self, percent: u8) -> Self {
        self.options.fail.percent = percent.min(100);
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.options.timeout = timeout;
        self
    }

    pub fn trace(mut self, trace: bool) -> Self {
        self.options.trace = trace;
        self
    }

    pub fn build(self) -> Result<DispatchEnvelope<P, R>, RaftError> {
        static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

        let body = self.spec.encode_params(&self.params)?;
        // B6: checked — a body that does not fit the u32 wire field must fail
        // the build, never be silently truncated into a corrupt frame.
        let body_len = u32::try_from(body.len()).map_err(|_| {
            RaftError::Dispatch("dispatch body length exceeds u32 wire field".into())
        })?;
        let tx_id = NEXT_TX_ID.fetch_add(1, Ordering::Relaxed);
        let header = DispatchFrameHeader {
            magic: U32::new(RAFT_DISPATCH_MAGIC),
            version: RAFT_DISPATCH_VERSION,
            command: self.spec.command(),
            scope: self.options.scope as u8,
            ack_policy: self.options.ack.policy as u8,
            fail_policy: self.options.fail.policy as u8,
            trace: self.options.trace as u8,
            _pad: [0; 2],
            tx_id: U64::new(tx_id),
            timeout_ms: U32::new(self.options.timeout.as_millis().min(u32::MAX as u128) as u32),
            body_len: U32::new(body_len),
        };

        let mut out = Vec::with_capacity(std::mem::size_of::<DispatchFrameHeader>() + body.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&body);

        Ok(DispatchEnvelope {
            tx_id,
            command: self.spec.command(),
            bytes: out,
            options: self.options,
            _marker: PhantomData,
        })
    }
}

impl<P, R> DispatchEnvelope<P, R>
where
    R: Clone + Send + 'static,
{
    /// Begin tracking this dispatch transaction across `targets`.
    /// Returns a handle for polling and a sender for recording peer responses.
    pub fn begin(
        &self,
        targets: impl IntoIterator<Item = PeerId>,
        decode_response: fn(&[u8]) -> Result<R, RaftError>,
    ) -> (DispatchHandle<R>, DispatchTx<R>) {
        crate::dispatch::tx::begin_transaction(
            self.tx_id(),
            self.command(),
            self.options_internal(),
            targets,
            decode_response,
        )
    }
}
