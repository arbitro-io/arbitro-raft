use bytes::{Bytes, BytesMut};
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref};

use crate::dispatch::{DispatchResponse, DispatchResponseKind, DispatchSpec};
use crate::RaftError;

pub const RAFT_DISPATCH_RESPONSE_MAGIC: u32 = 0x4452_5350;
pub const RAFT_DISPATCH_RESPONSE_VERSION: u8 = 1;

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct DispatchResponseFrameHeader {
    pub magic: U32,
    pub version: u8,
    pub command: u8,
    pub kind: u8,
    pub _pad: u8,
    pub tx_id: U64,
    pub body_len: U32,
}

pub const RAFT_DISPATCH_RESPONSE_FRAME_HEADER_SIZE: usize =
    std::mem::size_of::<DispatchResponseFrameHeader>();

#[derive(Debug, Clone)]
pub struct DispatchResponseView {
    frame: Bytes,
}

impl DispatchResponseView {
    pub fn parse(frame: Bytes) -> Result<Self, RaftError> {
        let (header, rest) = Ref::<_, DispatchResponseFrameHeader>::from_prefix(frame.as_ref())
            .map_err(|_| RaftError::Dispatch("short dispatch response header".into()))?;
        let header = Ref::into_ref(header);
        if header.magic.get() != RAFT_DISPATCH_RESPONSE_MAGIC {
            return Err(RaftError::Dispatch(
                "invalid dispatch response magic".into(),
            ));
        }
        if header.version != RAFT_DISPATCH_RESPONSE_VERSION {
            return Err(RaftError::Dispatch(format!(
                "unsupported dispatch response version {}",
                header.version
            )));
        }
        if rest.len() != header.body_len.get() as usize {
            return Err(RaftError::Dispatch(
                "dispatch response body length mismatch".into(),
            ));
        }
        Ok(Self { frame })
    }

    fn header(&self) -> &DispatchResponseFrameHeader {
        let (header, _) = Ref::<_, DispatchResponseFrameHeader>::from_prefix(self.frame.as_ref())
            .expect("dispatch response view always stores a validated header");
        Ref::into_ref(header)
    }

    #[inline]
    pub fn frame_bytes(&self) -> &Bytes {
        &self.frame
    }

    #[inline]
    pub fn tx_id(&self) -> u64 {
        self.header().tx_id.get()
    }

    #[inline]
    pub fn command(&self) -> u8 {
        self.header().command
    }

    #[inline]
    pub fn kind(&self) -> DispatchResponseKind {
        match self.header().kind {
            0 => DispatchResponseKind::Accepted,
            1 => DispatchResponseKind::Rejected,
            2 => DispatchResponseKind::Progress,
            3 => DispatchResponseKind::Failed,
            _ => DispatchResponseKind::Failed,
        }
    }

    #[inline]
    pub fn body(&self) -> &[u8] {
        &self.frame[RAFT_DISPATCH_RESPONSE_FRAME_HEADER_SIZE..]
    }

    #[inline]
    pub fn body_bytes(&self) -> Bytes {
        self.frame.slice(RAFT_DISPATCH_RESPONSE_FRAME_HEADER_SIZE..)
    }

    pub fn decode_with<P, R>(&self, spec: &DispatchSpec<P, R>) -> Result<R, RaftError> {
        if self.command() != spec.command() {
            return Err(RaftError::Dispatch(format!(
                "dispatch response command mismatch: got {}, expected {}",
                self.command(),
                spec.command()
            )));
        }
        spec.decode_response(self.body())
    }
}

pub fn encode_dispatch_response(response: &DispatchResponse) -> Bytes {
    let header = DispatchResponseFrameHeader {
        magic: U32::new(RAFT_DISPATCH_RESPONSE_MAGIC),
        version: RAFT_DISPATCH_RESPONSE_VERSION,
        command: response.command,
        kind: match response.kind {
            DispatchResponseKind::Accepted => 0,
            DispatchResponseKind::Rejected => 1,
            DispatchResponseKind::Progress => 2,
            DispatchResponseKind::Failed => 3,
        },
        _pad: 0,
        tx_id: U64::new(response.tx_id),
        body_len: U32::new(response.payload.len() as u32),
    };

    let mut out = BytesMut::with_capacity(
        std::mem::size_of::<DispatchResponseFrameHeader>() + response.payload.len(),
    );
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(response.payload.as_ref());
    out.freeze()
}
