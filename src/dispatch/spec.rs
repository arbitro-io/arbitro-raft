use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;

use crate::dispatch::DispatchBuilder;
use crate::RaftError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchScope {
    All      = 0,
    Others   = 1,
    Followers = 2,
    Leader   = 3,
    LocalOnly = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchNodeRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchRoute {
    pub role:      DispatchNodeRole,
    pub is_origin: bool,
}

impl DispatchRoute {
    pub const fn leader(is_origin: bool) -> Self {
        Self { role: DispatchNodeRole::Leader, is_origin }
    }

    pub const fn follower(is_origin: bool) -> Self {
        Self { role: DispatchNodeRole::Follower, is_origin }
    }
}

impl DispatchScope {
    pub const fn allows(self, route: DispatchRoute) -> bool {
        match self {
            DispatchScope::All      => true,
            DispatchScope::Others   => !route.is_origin,
            DispatchScope::Followers => matches!(route.role, DispatchNodeRole::Follower),
            DispatchScope::Leader   => matches!(route.role, DispatchNodeRole::Leader),
            DispatchScope::LocalOnly => route.is_origin,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchAckPolicy {
    All        = 0,
    Quorum     = 1,
    AtLeast    = 2,
    Percent    = 3,
    BestEffort = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchFailPolicy {
    AllowFailures      = 0,
    NoFailures         = 1,
    MaxFailures        = 2,
    MaxFailurePercent  = 3,
    FailFast           = 4,
}

/// Acknowledgement policy with its associated thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchAckOptions {
    pub policy:  DispatchAckPolicy,
    /// Minimum number of accepts required (used by `AtLeast`).
    pub count:   u16,
    /// Minimum percentage of accepts required (used by `Percent`).
    pub percent: u8,
}

/// Failure policy with its associated thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchFailOptions {
    pub policy:  DispatchFailPolicy,
    /// Maximum number of failures allowed (used by `MaxFailures`).
    pub count:   u16,
    /// Maximum percentage of failures allowed (used by `MaxFailurePercent`).
    pub percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchOptions {
    pub scope:   DispatchScope,
    pub ack:     DispatchAckOptions,
    pub fail:    DispatchFailOptions,
    pub timeout: Duration,
    pub trace:   bool,
}

impl Default for DispatchAckOptions {
    fn default() -> Self {
        Self {
            policy:  DispatchAckPolicy::Quorum,
            count:   1,
            percent: 100,
        }
    }
}

impl Default for DispatchFailOptions {
    fn default() -> Self {
        Self {
            policy:  DispatchFailPolicy::AllowFailures,
            count:   0,
            percent: 0,
        }
    }
}

impl Default for DispatchOptions {
    fn default() -> Self {
        Self {
            scope:   DispatchScope::All,
            ack:     DispatchAckOptions::default(),
            fail:    DispatchFailOptions::default(),
            timeout: Duration::from_secs(2),
            trace:   false,
        }
    }
}

pub struct DispatchSpec<P, R> {
    command:         u8,
    defaults:        DispatchOptions,
    encode_params:   fn(&P) -> Result<Bytes, RaftError>,
    decode_params:   fn(&[u8]) -> Result<P, RaftError>,
    encode_response: fn(&R) -> Result<Bytes, RaftError>,
    decode_response: fn(&[u8]) -> Result<R, RaftError>,
    _marker:         PhantomData<fn(P) -> R>,
}

impl<P, R> Clone for DispatchSpec<P, R> {
    fn clone(&self) -> Self { *self }
}

impl<P, R> Copy for DispatchSpec<P, R> {}

impl<P, R> DispatchSpec<P, R> {
    pub fn new(
        command:         u8,
        encode_params:   fn(&P) -> Result<Bytes, RaftError>,
        decode_params:   fn(&[u8]) -> Result<P, RaftError>,
        encode_response: fn(&R) -> Result<Bytes, RaftError>,
        decode_response: fn(&[u8]) -> Result<R, RaftError>,
    ) -> Self {
        Self {
            command,
            defaults: DispatchOptions::default(),
            encode_params,
            decode_params,
            encode_response,
            decode_response,
            _marker: PhantomData,
        }
    }

    pub fn command(&self) -> u8 { self.command }

    pub fn defaults(&self) -> DispatchOptions { self.defaults }

    pub fn with_scope(mut self, scope: DispatchScope) -> Self {
        self.defaults.scope = scope;
        self
    }

    pub fn with_ack_policy(mut self, policy: DispatchAckPolicy) -> Self {
        self.defaults.ack.policy = policy;
        self
    }

    pub fn with_ack_count(mut self, count: u16) -> Self {
        self.defaults.ack.count = count.max(1);
        self
    }

    pub fn with_ack_percent(mut self, percent: u8) -> Self {
        self.defaults.ack.percent = percent.clamp(1, 100);
        self
    }

    pub fn with_fail_policy(mut self, policy: DispatchFailPolicy) -> Self {
        self.defaults.fail.policy = policy;
        self
    }

    pub fn with_fail_count(mut self, count: u16) -> Self {
        self.defaults.fail.count = count;
        self
    }

    pub fn with_fail_percent(mut self, percent: u8) -> Self {
        self.defaults.fail.percent = percent.min(100);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.defaults.timeout = timeout;
        self
    }

    pub fn with_trace(mut self, trace: bool) -> Self {
        self.defaults.trace = trace;
        self
    }

    pub fn encode_params(&self, params: &P) -> Result<Bytes, RaftError> {
        (self.encode_params)(params)
    }

    pub fn decode_params(&self, bytes: &[u8]) -> Result<P, RaftError> {
        (self.decode_params)(bytes)
    }

    pub fn encode_response(&self, value: &R) -> Result<Bytes, RaftError> {
        (self.encode_response)(value)
    }

    pub fn decode_response(&self, bytes: &[u8]) -> Result<R, RaftError> {
        (self.decode_response)(bytes)
    }

    pub(crate) fn decode_response_fn(&self) -> fn(&[u8]) -> Result<R, RaftError> {
        self.decode_response
    }

    pub fn dispatch<'a>(&'a self, params: P) -> DispatchBuilder<'a, P, R> {
        DispatchBuilder::new(self, params)
    }
}
