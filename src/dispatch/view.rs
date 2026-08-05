use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref};

use crate::dispatch::{DispatchAckPolicy, DispatchFailPolicy, DispatchScope, DispatchSpec};
use crate::RaftError;

pub const RAFT_DISPATCH_MAGIC: u32 = 0x4453_5054;
pub const RAFT_DISPATCH_VERSION: u8 = 1;

pub const RAFT_DISPATCH_RESPONSE_MAGIC: u32 = 0x4452_5350;
pub const RAFT_DISPATCH_RESPONSE_VERSION: u8 = 1;

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct DispatchFrameHeader {
    pub magic: U32,
    pub version: u8,
    pub command: u8,
    pub scope: u8,
    pub ack_policy: u8,
    pub fail_policy: u8,
    pub trace: u8,
    pub _pad: [u8; 2],
    pub tx_id: U64,
    pub timeout_ms: U32,
    pub body_len: U32,
}

pub const RAFT_DISPATCH_FRAME_HEADER_SIZE: usize = std::mem::size_of::<DispatchFrameHeader>();

#[derive(Debug, Clone, Copy)]
pub struct DispatchView<'a> {
    frame: &'a [u8],
}

impl<'a> DispatchView<'a> {
    pub fn parse(frame: &'a [u8]) -> Result<Self, RaftError> {
        let (header_ref, rest) = Ref::<_, DispatchFrameHeader>::from_prefix(frame)
            .map_err(|_| RaftError::Dispatch("short dispatch frame header".into()))?;
        let header = Ref::into_ref(header_ref);
        if header.magic.get() != RAFT_DISPATCH_MAGIC {
            return Err(RaftError::Dispatch("invalid dispatch magic".into()));
        }
        if header.version != RAFT_DISPATCH_VERSION {
            return Err(RaftError::Dispatch(format!(
                "unsupported dispatch version {}",
                header.version
            )));
        }
        if rest.len() < header.body_len.get() as usize {
            return Err(RaftError::Dispatch("dispatch body length mismatch".into()));
        }
        if header.scope > 4 {
            return Err(RaftError::Dispatch("unknown dispatch scope".into()));
        }
        if header.ack_policy > 4 {
            return Err(RaftError::Dispatch("unknown dispatch ack policy".into()));
        }
        if header.fail_policy > 4 {
            return Err(RaftError::Dispatch("unknown dispatch fail policy".into()));
        }
        Ok(Self { frame })
    }

    fn header(&self) -> &DispatchFrameHeader {
        // B13: `frame` was validated at construction (`new` rejects short or
        // malformed frames), so the prefix take cannot fail here.
        #[allow(clippy::expect_used)]
        let (header, _) = Ref::<_, DispatchFrameHeader>::from_prefix(self.frame)
            .expect("dispatch view always stores a validated header");
        Ref::into_ref(header)
    }

    #[inline]
    pub fn frame_bytes(&self) -> &'a [u8] {
        self.frame
    }

    #[inline]
    pub fn command(&self) -> u8 {
        self.header().command
    }

    #[inline]
    pub fn tx_id(&self) -> u64 {
        self.header().tx_id.get()
    }

    #[inline]
    pub fn scope(&self) -> DispatchScope {
        match self.header().scope {
            0 => DispatchScope::All,
            1 => DispatchScope::Others,
            2 => DispatchScope::Followers,
            3 => DispatchScope::Leader,
            4 => DispatchScope::LocalOnly,
            _ => DispatchScope::All,
        }
    }

    #[inline]
    pub fn ack_policy(&self) -> DispatchAckPolicy {
        match self.header().ack_policy {
            0 => DispatchAckPolicy::All,
            1 => DispatchAckPolicy::Quorum,
            2 => DispatchAckPolicy::AtLeast,
            3 => DispatchAckPolicy::Percent,
            4 => DispatchAckPolicy::BestEffort,
            _ => DispatchAckPolicy::Quorum,
        }
    }

    #[inline]
    pub fn fail_policy(&self) -> DispatchFailPolicy {
        match self.header().fail_policy {
            0 => DispatchFailPolicy::AllowFailures,
            1 => DispatchFailPolicy::NoFailures,
            2 => DispatchFailPolicy::MaxFailures,
            3 => DispatchFailPolicy::MaxFailurePercent,
            4 => DispatchFailPolicy::FailFast,
            _ => DispatchFailPolicy::AllowFailures,
        }
    }

    #[inline]
    pub fn trace(&self) -> bool {
        self.header().trace != 0
    }

    #[inline]
    pub fn timeout_ms(&self) -> u32 {
        self.header().timeout_ms.get()
    }

    #[inline]
    pub fn body(&self) -> &'a [u8] {
        &self.frame[RAFT_DISPATCH_FRAME_HEADER_SIZE..]
    }

    pub fn params<P, R>(&self, spec: &DispatchSpec<P, R>) -> Result<P, RaftError> {
        if self.command() != spec.command() {
            return Err(RaftError::Dispatch(format!(
                "dispatch command mismatch: got {}, expected {}",
                self.command(),
                spec.command()
            )));
        }
        spec.decode_params(self.body())
    }
}
