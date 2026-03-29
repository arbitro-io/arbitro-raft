use std::sync::{Arc, Mutex};
use std::task::Waker;

use crate::dispatch::spec::{DispatchAckPolicy, DispatchFailPolicy, DispatchOptions};
use crate::PeerId;

use super::types::{DispatchFailure, DispatchPeerResult, DispatchPeerState, DispatchResult};

// ── Internal state types ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) enum DispatchCompletion<R> {
    Succeeded(DispatchResult<R>),
    Failed(DispatchFailure),
}

#[derive(Debug, Clone)]
pub(crate) struct DispatchPeerSlot<R> {
    pub(crate) peer:  PeerId,
    pub(crate) state: DispatchPeerState<R>,
}

#[derive(Debug)]
pub(crate) struct DispatchState<R> {
    pub(crate) tx_id:      u64,
    pub(crate) command:    u8,
    pub(crate) options:    DispatchOptions,
    pub(crate) peers:      Vec<DispatchPeerSlot<R>>,
    pub(crate) completion: Option<DispatchCompletion<R>>,
    pub(crate) wakers:     Vec<Waker>,
}

// ── State queries ─────────────────────────────────────────────────────────────

impl<R: Clone> DispatchState<R> {
    pub(crate) fn result_snapshot(&self) -> DispatchResult<R> {
        DispatchResult {
            tx_id:   self.tx_id,
            command: self.command,
            options: self.options,
            peers:   self
                .peers
                .iter()
                .map(|p| DispatchPeerResult { peer: p.peer, state: p.state.clone() })
                .collect(),
        }
    }

    pub(crate) fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    fn accepted_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| matches!(p.state, DispatchPeerState::Accepted(_)))
            .count()
    }

    fn failed_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| {
                matches!(p.state, DispatchPeerState::Rejected(_) | DispatchPeerState::Failed(_))
            })
            .count()
    }

    fn pending_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| {
                matches!(p.state, DispatchPeerState::Pending | DispatchPeerState::Progress(_))
            })
            .count()
    }

    fn active_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| !matches!(p.state, DispatchPeerState::Disconnected))
            .count()
    }
}

// ── Policy evaluation ─────────────────────────────────────────────────────────

impl<R: Clone> DispatchState<R> {
    fn required_accepts(&self) -> usize {
        let active = self.active_count();
        if active == 0 {
            return 0;
        }
        match self.options.ack.policy {
            DispatchAckPolicy::All        => active,
            DispatchAckPolicy::Quorum     => (active / 2) + 1,
            DispatchAckPolicy::AtLeast    => {
                usize::min(active, self.options.ack.count.max(1) as usize)
            }
            DispatchAckPolicy::Percent    => {
                let pct = usize::from(self.options.ack.percent.clamp(1, 100));
                usize::max(1, (active * pct).div_ceil(100))
            }
            DispatchAckPolicy::BestEffort => 1,
        }
    }

    fn allowed_failures(&self) -> usize {
        let active = self.active_count();
        match self.options.fail.policy {
            DispatchFailPolicy::AllowFailures                              => usize::MAX,
            DispatchFailPolicy::NoFailures | DispatchFailPolicy::FailFast  => 0,
            DispatchFailPolicy::MaxFailures       => self.options.fail.count as usize,
            DispatchFailPolicy::MaxFailurePercent => {
                (active * usize::from(self.options.fail.percent.min(100))) / 100
            }
        }
    }

    pub(crate) fn evaluate(&mut self) {
        if self.completion.is_some() {
            return;
        }
        let required         = self.required_accepts();
        let accepted         = self.accepted_count();
        let failed           = self.failed_count();
        let pending          = self.pending_count();
        let allowed_failures = self.allowed_failures();
        let possible_accepts = accepted + pending;

        if required == 0 || accepted >= required {
            self.completion = Some(DispatchCompletion::Succeeded(self.result_snapshot()));
            self.wake_all();
            return;
        }

        if failed > allowed_failures {
            self.completion = Some(DispatchCompletion::Failed(DispatchFailure::Failed(format!(
                "dispatch failure policy exceeded: failed={failed}, allowed={allowed_failures}"
            ))));
            self.wake_all();
            return;
        }

        if possible_accepts < required {
            self.completion = Some(DispatchCompletion::Failed(DispatchFailure::Impossible(
                format!(
                    "dispatch can no longer reach required accepts: \
                     accepted={accepted}, pending={pending}, required={required}"
                ),
            )));
            self.wake_all();
        }
    }
}

// ── Shared Arc wrapper ────────────────────────────────────────────────────────

pub(crate) struct DispatchShared<R> {
    pub(crate) inner: Arc<Mutex<DispatchState<R>>>,
}

impl<R> Clone for DispatchShared<R> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}
