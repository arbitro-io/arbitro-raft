use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::IntoBytes;

use crate::dispatch::spec::{
    DispatchAckPolicy, DispatchFailPolicy, DispatchOptions, DispatchScope,
};
use crate::dispatch::view::{
    DispatchFrameHeader, DispatchView, RAFT_DISPATCH_MAGIC, RAFT_DISPATCH_VERSION,
};
use crate::dispatch::DispatchSpec;
use crate::RaftError;

pub struct DispatchBuilder<'a, P, R> {
    spec: &'a DispatchSpec<P, R>,
    params: P,
    options: DispatchOptions,
}

pub struct DispatchEnvelope<P, R> {
    tx_id: u64,
    command: u8,
    bytes: Bytes,
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

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn command(&self) -> u8 {
        self.command
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn options(&self) -> DispatchOptions {
        self.options
    }

    pub fn view(&self) -> Result<DispatchView, RaftError> {
        DispatchView::parse(self.bytes.clone())
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
        self.options.ack_policy = policy;
        self
    }

    pub fn ack_count(mut self, ack_count: u16) -> Self {
        self.options.ack_count = ack_count.max(1);
        self
    }

    pub fn ack_percent(mut self, ack_percent: u8) -> Self {
        self.options.ack_percent = ack_percent.clamp(1, 100);
        self
    }

    pub fn fail_policy(mut self, policy: DispatchFailPolicy) -> Self {
        self.options.fail_policy = policy;
        self
    }

    pub fn fail_count(mut self, fail_count: u16) -> Self {
        self.options.fail_count = fail_count;
        self
    }

    pub fn fail_percent(mut self, fail_percent: u8) -> Self {
        self.options.fail_percent = fail_percent.min(100);
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
        let tx_id = NEXT_TX_ID.fetch_add(1, Ordering::Relaxed);
        let header = DispatchFrameHeader {
            magic: U32::new(RAFT_DISPATCH_MAGIC),
            version: RAFT_DISPATCH_VERSION,
            command: self.spec.command(),
            scope: self.options.scope as u8,
            ack_policy: self.options.ack_policy as u8,
            fail_policy: self.options.fail_policy as u8,
            trace: self.options.trace as u8,
            _pad: [0; 2],
            tx_id: U64::new(tx_id),
            timeout_ms: U32::new(self.options.timeout.as_millis().min(u32::MAX as u128) as u32),
            body_len: U32::new(body.len() as u32),
        };

        let mut out =
            BytesMut::with_capacity(std::mem::size_of::<DispatchFrameHeader>() + body.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(body.as_ref());

        Ok(DispatchEnvelope {
            tx_id,
            command: self.spec.command(),
            bytes: out.freeze(),
            options: self.options,
            _marker: PhantomData,
        })
    }
}
