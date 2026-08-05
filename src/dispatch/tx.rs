mod handle;
mod state;
mod types;

pub use handle::{DispatchHandle, DispatchTx, DispatchTxResponder};
pub use types::{DispatchFailure, DispatchPeerResult, DispatchPeerState, DispatchResult};

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::dispatch::DispatchOptions;
use crate::{PeerId, RaftError};

use state::{DispatchPeerSlot, DispatchShared, DispatchState};
use types::DispatchPeerState as PeerState;

/// Create a new `(DispatchHandle, DispatchTx)` pair for `targets`.
/// Called exclusively by `DispatchEnvelope::begin`.
pub(crate) fn begin_transaction<R>(
    tx_id: u64,
    command: u8,
    options: DispatchOptions,
    targets: impl IntoIterator<Item = PeerId>,
    decode_response: fn(&[u8]) -> Result<R, RaftError>,
) -> (DispatchHandle<R>, DispatchTx<R>)
where
    R: Clone + Send + 'static,
{
    let shared = DispatchShared {
        inner: Arc::new(Mutex::new(DispatchState {
            tx_id,
            command,
            options,
            peers: targets
                .into_iter()
                .map(|peer| DispatchPeerSlot {
                    peer,
                    state: PeerState::Pending,
                })
                .collect(),
            completion: None,
            wakers: Vec::new(),
            start_instant: Instant::now(),
            timeout: options.timeout,
        })),
    };

    let handle = DispatchHandle {
        shared: shared.clone(),
    };
    let tx = DispatchTx {
        shared,
        decode_response,
    };

    {
        let mut guard = handle.shared.lock();
        guard.evaluate();
    }

    (handle, tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::DispatchPeerState;

    fn decode(bytes: &[u8]) -> Result<u8, RaftError> {
        Ok(bytes.first().copied().unwrap_or(0))
    }

    /// B9 — a panicked lock holder must NOT kill the dispatch layer.
    ///
    /// Poison the shared mutex (a thread panics while holding it), then
    /// drive a transaction to completion through the same lock: every
    /// handle/tx entry point must recover the guard instead of cascading
    /// the panic.
    #[test]
    fn poisoned_lock_dispatch_still_completes() {
        let (handle, tx) = begin_transaction(
            7,
            1,
            DispatchOptions::default(),
            [PeerId(1), PeerId(2), PeerId(3)],
            decode as fn(&[u8]) -> Result<u8, RaftError>,
        );

        // Poison: panic while holding the mutex. Silence the panic hook so
        // the intentional panic does not pollute the test log.
        let inner = handle.shared.inner.clone();
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoner = std::thread::spawn(move || {
            let _guard = inner.lock().unwrap();
            panic!("intentional: poison the dispatch mutex");
        });
        assert!(poisoner.join().is_err(), "the poisoner thread must panic");
        std::panic::set_hook(prev_hook);
        assert!(
            handle.shared.inner.is_poisoned(),
            "setup: the mutex must actually be poisoned"
        );

        // Every entry point still works over the poisoned mutex.
        assert_eq!(handle.tx_id(), 7);
        assert_eq!(handle.command(), 1);
        assert!(!handle.is_ready(), "nothing accepted yet");
        assert!(handle.try_result().is_none());

        // Quorum of 3 = 2 accepts → the transaction completes normally.
        tx.accept(PeerId(1), 42).expect("accept over poisoned lock");
        tx.accept(PeerId(2), 43).expect("accept over poisoned lock");
        assert!(handle.is_ready(), "quorum reached despite the poison");
        let result = handle
            .try_result()
            .expect("completion present")
            .expect("dispatch succeeded");
        assert_eq!(result.tx_id, 7);
        let accepted = result
            .peers
            .iter()
            .filter(|p| matches!(p.state, DispatchPeerState::Accepted(_)))
            .count();
        assert_eq!(
            accepted, 2,
            "both accepts recorded through the poisoned lock"
        );
    }
}
