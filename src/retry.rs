//! `RetryingStore`: a `ChunkGet` decorator that absorbs transient retrieval
//! failures so one hard chunk does not abort a whole-file download.
//!
//! The joiner calls `get(addr)` once per tree node and propagates the first
//! error, which aborts the entire reconstruction. On a live network a single
//! `get` often fails transiently: the node may momentarily find too few
//! candidate storers, or a candidate refuses to serve under load. Retrying the
//! `get` with bounded exponential backoff turns those transient misses into
//! eventual hits, so a multi-thousand-chunk download survives the inevitable
//! per-chunk flakiness instead of dying on the first one.
//!
//! The wrapper is store-agnostic: it retries on any `get` error, since the
//! inner store's error type is opaque here. A genuinely unretrievable chunk
//! still fails, but only after the attempt budget is spent.

use std::time::Duration;

use nectar_primitives::{
    AnyChunk, ChunkAddress,
    store::{ChunkGet, ChunkHas, ChunkPut},
};

use crate::store::BS;

/// Total `get` attempts (one initial try plus retries) before the error
/// propagates. Sized so a chunk that is merely contended is almost always
/// recovered while a truly absent chunk still fails in bounded time.
const MAX_ATTEMPTS: u32 = 8;

/// Backoff before the first retry. Doubles each subsequent retry up to
/// [`BACKOFF_CAP`].
const BACKOFF_BASE: Duration = Duration::from_millis(150);

/// Upper bound on a single backoff wait, so late retries stay responsive.
const BACKOFF_CAP: Duration = Duration::from_secs(8);

/// `ChunkGet` decorator that retries transient `get` failures with exponential
/// backoff and jitter. `Clone` is cheap when the inner store is cheap to clone
/// (the joiner needs `G: Clone + Send + Sync`).
#[derive(Clone)]
pub(crate) struct RetryingStore<G> {
    inner: G,
}

impl<G> RetryingStore<G> {
    /// Wrap `inner` so its `get` calls retry transient failures.
    pub(crate) const fn new(inner: G) -> Self {
        Self { inner }
    }
}

/// Backoff for the retry that follows attempt `attempt` (1-based): base doubled
/// `attempt - 1` times, capped, plus up to 50% jitter to spread retries of many
/// chunks that failed together.
fn backoff_for(attempt: u32) -> Duration {
    let shift = (attempt - 1).min(16);
    let scaled = BACKOFF_BASE.saturating_mul(1u32 << shift);
    let capped = scaled.min(BACKOFF_CAP);
    let jitter = capped.mul_f64(0.5 * jitter_unit()).min(BACKOFF_CAP);
    capped.saturating_add(jitter)
}

/// A pseudo-random value in `[0, 1)` derived from the wall clock. Avoids a
/// `rand` dependency; jitter only needs to decorrelate retries, not be secure.
fn jitter_unit() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    f64::from(nanos) / f64::from(u32::from(u16::MAX) + 1) / 65_536.0
}

impl<G: ChunkGet<BS>> ChunkGet<BS> for RetryingStore<G> {
    type Error = G::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        let mut attempt = 1;
        loop {
            match self.inner.get(address).await {
                Ok(chunk) => return Ok(chunk),
                Err(e) => {
                    if attempt >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    tokio::time::sleep(backoff_for(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

impl<G: ChunkPut<BS>> ChunkPut<BS> for RetryingStore<G> {
    type Error = G::Error;

    async fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        self.inner.put(chunk).await
    }
}

impl<G: ChunkHas<BS>> ChunkHas<BS> for RetryingStore<G> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.inner.has(address).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use nectar_primitives::ContentChunk;

    /// A store that fails its first `fail_first` gets, then succeeds.
    struct FlakyStore {
        chunk: AnyChunk<BS>,
        remaining_failures: Mutex<u32>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("transient")]
    struct Transient;

    impl ChunkGet<BS> for FlakyStore {
        type Error = Transient;

        async fn get(&self, _address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
            let mut left = self.remaining_failures.lock().expect("lock");
            if *left > 0 {
                *left -= 1;
                return Err(Transient);
            }
            Ok(self.chunk.clone())
        }
    }

    fn sample_chunk() -> AnyChunk<BS> {
        ContentChunk::<BS>::new("retry probe")
            .expect("build content chunk")
            .into()
    }

    #[tokio::test(start_paused = true)]
    async fn recovers_after_transient_failures() {
        let chunk = sample_chunk();
        let address = *chunk.address();
        let store = RetryingStore::new(FlakyStore {
            chunk,
            remaining_failures: Mutex::new(MAX_ATTEMPTS - 1),
        });

        let got = store.get(&address).await.expect("recovered within budget");
        assert_eq!(got.address(), &address);
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_once_budget_is_spent() {
        let chunk = sample_chunk();
        let address = *chunk.address();
        let store = RetryingStore::new(FlakyStore {
            chunk,
            remaining_failures: Mutex::new(MAX_ATTEMPTS),
        });

        let err = store.get(&address).await;
        assert!(err.is_err(), "budget exhausted, error must propagate");
    }
}
