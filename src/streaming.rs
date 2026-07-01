//! `StreamingChunkGet`: a `ChunkGet` that multiplexes every concurrent
//! `get(addr)` onto one `RetrieveChunks` bidi stream and demuxes responses by
//! address, replacing the per-chunk unary call.
//!
//! # Stream-death handling
//!
//! The shared stream is a single point of failure: a transport fault (or a
//! clean server close) tears down the one bidi stream every in-flight `get`
//! rides. To match the resilience of the unary path (where each `get` rode an
//! auto-reconnecting tonic channel, so a blip cost one chunk and retry
//! recovered it) the stream is *re-openable*.
//!
//! When the demux task observes the stream end it marks the current
//! [`StreamHandle`] closed and drains every pending waiter with
//! [`GrpcStoreError::StreamReset`]. The next `get` sees the closed handle and,
//! under one async mutex so concurrent callers open it exactly once, lazily
//! establishes a fresh `RetrieveChunks` stream. A `get` whose waiter was drained
//! by the reset returns `StreamReset` fast (never a full retry-backoff budget
//! spent against a dead stream); the wrapping `RetryingChunkGet` then re-issues,
//! and that re-issue lands on the freshly re-opened live stream and recovers.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use nectar_primitives::{AnyChunk, ChunkAddress, ContentChunk, store::ChunkGet};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;

use crate::proto::chunk::{
    RetrieveChunkRequest, RetrieveChunkResponse, chunk_client::ChunkClient, retrieve_chunk_response,
};
use crate::store::{BS, GrpcStoreError};

/// Outstanding request-channel depth. The joiner bounds how many `get`s are in
/// flight; a full channel parks `get` (backpressure) rather than growing.
const REQUEST_BUFFER: usize = 1024;

/// Re-open attempts a single `get` makes when it loses a race with a dying
/// stream (the handle closes between acquiring it and registering/sending).
/// Small because the common recovery path is the `RetryingChunkGet` re-issue,
/// not this in-call loop.
const OPEN_ATTEMPTS: usize = 3;

/// Delivered to a waiting `get`: the raw wire body, or the error that ended its
/// wait (a per-address miss, or a stream reset).
type GetResult = Result<Vec<u8>, GrpcStoreError>;

/// The inbound half of a `RetrieveChunks` stream, boxed so a mock transport can
/// stand in for `tonic::Streaming` in tests. An `Err` item (transport fault) or
/// exhaustion (clean close) both end the demux.
type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<RetrieveChunkResponse, tonic::Status>> + Send>>;

/// Opens a fresh `RetrieveChunks` bidi stream, returning the request sender and
/// the response stream. Abstracted so tests can inject a transport that dies on
/// demand; production uses [`GrpcOpener`] over the gRPC client.
pub(crate) trait StreamOpener: Send + Sync + 'static {
    fn open(
        &self,
    ) -> impl std::future::Future<
        Output = Result<(mpsc::Sender<RetrieveChunkRequest>, ResponseStream), GrpcStoreError>,
    > + Send;
}

/// Production opener: calls `RetrieveChunks` on the gRPC client. The client is
/// cheap to clone and connects lazily, so each re-open dials as needed.
pub(crate) struct GrpcOpener {
    client: ChunkClient<Channel>,
}

impl StreamOpener for GrpcOpener {
    async fn open(
        &self,
    ) -> Result<(mpsc::Sender<RetrieveChunkRequest>, ResponseStream), GrpcStoreError> {
        let mut client = self.client.clone();
        let (requests, request_rx) = mpsc::channel::<RetrieveChunkRequest>(REQUEST_BUFFER);
        let responses = client
            .retrieve_chunks(ReceiverStream::new(request_rx))
            .await?
            .into_inner();
        Ok((requests, Box::pin(responses)))
    }
}

/// Per-address FIFO of response waiters plus a closed flag, all under one lock so
/// registering a waiter and the demux shutdown drain cannot race.
#[derive(Default)]
struct Waiters {
    closed: bool,
    map: HashMap<Vec<u8>, VecDeque<oneshot::Sender<GetResult>>>,
}

/// One live `RetrieveChunks` stream: its request sender plus the waiter table
/// its demux task routes into. A handle is closed exactly once, by its demux on
/// stream end or by a `get` that finds the stream already gone.
struct StreamHandle {
    requests: mpsc::Sender<RetrieveChunkRequest>,
    waiters: Mutex<Waiters>,
}

impl StreamHandle {
    fn is_closed(&self) -> bool {
        self.waiters.lock().expect("waiters mutex").closed
    }

    /// Register `tx` to receive the response for `key`. Returns `tx` back when
    /// the handle is already closed, so the caller re-opens instead of waiting
    /// on a dead stream.
    fn register(
        &self,
        key: Vec<u8>,
        tx: oneshot::Sender<GetResult>,
    ) -> Result<(), oneshot::Sender<GetResult>> {
        let mut waiters = self.waiters.lock().expect("waiters mutex");
        if waiters.closed {
            return Err(tx);
        }
        waiters.map.entry(key).or_default().push_back(tx);
        Ok(())
    }

    /// Mark the stream dead and fail every pending waiter with `StreamReset`.
    /// Idempotent: the demux and a racing `get` may both call it.
    fn close_and_drain(&self) {
        let mut waiters = self.waiters.lock().expect("waiters mutex");
        waiters.closed = true;
        for (_, queue) in waiters.map.drain() {
            for waiter in queue {
                let _ = waiter.send(Err(GrpcStoreError::StreamReset));
            }
        }
    }
}

/// Shared state behind an [`Arc`]: the opener plus the current live stream (or
/// `None` before the first `get` / after a reset). The async mutex guards
/// re-open so concurrent callers open a replacement exactly once.
struct Inner<O> {
    opener: O,
    current: tokio::sync::Mutex<Option<Arc<StreamHandle>>>,
}

impl<O: StreamOpener> Inner<O> {
    /// Return a live handle, re-opening if the stored one is absent or closed.
    async fn live_handle(&self) -> Result<Arc<StreamHandle>, GrpcStoreError> {
        let mut current = self.current.lock().await;
        if let Some(handle) = current.as_ref()
            && !handle.is_closed()
        {
            return Ok(Arc::clone(handle));
        }
        let handle = self.open_stream().await?;
        *current = Some(Arc::clone(&handle));
        Ok(handle)
    }

    /// Open a fresh stream and spawn its demux task.
    async fn open_stream(&self) -> Result<Arc<StreamHandle>, GrpcStoreError> {
        let (requests, responses) = self.opener.open().await?;
        let handle = Arc::new(StreamHandle {
            requests,
            waiters: Mutex::new(Waiters::default()),
        });
        tokio::spawn(demux(responses, Arc::clone(&handle)));
        Ok(handle)
    }

    /// Retire a handle discovered dead outside its demux (a lost register/send
    /// race): drain it and clear it from `current` so the next `get` re-opens.
    async fn retire(&self, dead: &Arc<StreamHandle>) {
        dead.close_and_drain();
        let mut current = self.current.lock().await;
        if current.as_ref().is_some_and(|h| Arc::ptr_eq(h, dead)) {
            *current = None;
        }
    }
}

/// A `ChunkGet` backed by one shared, re-openable `RetrieveChunks` bidi stream.
/// Cheap to clone (shares one [`Inner`] by `Arc`), so the joiner's
/// `G: Clone + Send + Sync` bound holds.
pub(crate) struct StreamingChunkGet<O = GrpcOpener> {
    inner: Arc<Inner<O>>,
}

impl<O> Clone for StreamingChunkGet<O> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl StreamingChunkGet<GrpcOpener> {
    /// Build a streaming store over the gRPC client. The stream opens lazily on
    /// the first `get`, and re-opens transparently after any reset.
    pub(crate) fn new(client: ChunkClient<Channel>) -> Self {
        Self::with_opener(GrpcOpener { client })
    }
}

impl<O: StreamOpener> StreamingChunkGet<O> {
    fn with_opener(opener: O) -> Self {
        Self {
            inner: Arc::new(Inner {
                opener,
                current: tokio::sync::Mutex::new(None),
            }),
        }
    }
}

/// Read responses until the stream ends, routing each to the next waiter for its
/// address. Exhaustion (clean close) or an `Err` item (transport fault) both
/// break the loop; the drain then releases every pending waiter with
/// `StreamReset` and marks the handle closed so the next `get` re-opens.
async fn demux(mut responses: ResponseStream, handle: Arc<StreamHandle>) {
    while let Some(Ok(response)) = responses.next().await {
        route(&handle, response);
    }
    handle.close_and_drain();
}

/// Deliver one response to the head waiter registered for its address.
fn route(handle: &StreamHandle, response: RetrieveChunkResponse) {
    let (address, result) = match response.result {
        Some(retrieve_chunk_response::Result::Chunk(chunk)) => (chunk.address, Ok(chunk.data)),
        Some(retrieve_chunk_response::Result::Error(error)) => {
            (error.address, Err(GrpcStoreError::NotFound(error.message)))
        }
        None => return,
    };

    let mut waiters = handle.waiters.lock().expect("waiters mutex");
    if let Some(queue) = waiters.map.get_mut(&address) {
        if let Some(waiter) = queue.pop_front() {
            let _ = waiter.send(result);
        }
        if queue.is_empty() {
            waiters.map.remove(&address);
        }
    }
}

impl<O: StreamOpener> ChunkGet<BS> for StreamingChunkGet<O> {
    type Error = GrpcStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        let key = address.as_bytes().to_vec();

        // A single get makes a bounded number of open attempts so a stream that
        // dies between acquiring the handle and using it re-opens once here. A
        // stream that dies mid-flight surfaces `StreamReset` fast; the wrapping
        // `RetryingChunkGet` re-issues onto the stream the next call re-opens.
        for _ in 0..OPEN_ATTEMPTS {
            let handle = self.inner.live_handle().await?;
            let (tx, rx) = oneshot::channel::<GetResult>();

            // Register THEN send, so a fast response can never arrive before its
            // waiter is in place. A closed handle here lost the race with a
            // dying stream: retire it and re-open.
            if handle.register(key.clone(), tx).is_err() {
                self.inner.retire(&handle).await;
                continue;
            }

            if handle
                .requests
                .send(RetrieveChunkRequest {
                    address: key.clone(),
                })
                .await
                .is_err()
            {
                // The request channel is gone: the stream died. Re-open and try.
                self.inner.retire(&handle).await;
                continue;
            }

            let data = match rx.await {
                Ok(Ok(data)) => data,
                // In-band error: a per-address miss, or a drain-on-reset. Both
                // propagate so the retry decorator decides; a reset re-opens on
                // the re-issue via `live_handle`.
                Ok(Err(e)) => return Err(e),
                // The waiter was dropped without a value: the demux task ended
                // before draining reached it. Treat as a reset.
                Err(_) => return Err(GrpcStoreError::StreamReset),
            };

            if data.is_empty() {
                return Err(GrpcStoreError::NotFound(hex::encode(&key)));
            }

            // Same reconstruction + BMT verification as the unary `get`.
            let bytes = nectar_primitives::bytes::Bytes::from(data);
            let chunk: AnyChunk<BS> = ContentChunk::<BS>::try_from(bytes)?.into();
            chunk.verify(address)?;
            return Ok(chunk);
        }

        Err(GrpcStoreError::StreamReset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use nectar_primitives::{RetryingChunkGet, Sleeper};

    use crate::proto::chunk::{ChunkError, RetrievedChunk};

    /// Build a response carrying a retrieved chunk for `address`.
    fn chunk_response(address: &[u8], data: &[u8]) -> RetrieveChunkResponse {
        RetrieveChunkResponse {
            result: Some(retrieve_chunk_response::Result::Chunk(RetrievedChunk {
                address: address.to_vec(),
                data: data.to_vec(),
                stamp: Vec::new(),
                served_by: Vec::new(),
            })),
        }
    }

    /// Build a response carrying a per-address error for `address`.
    fn error_response(address: &[u8], message: &str) -> RetrieveChunkResponse {
        RetrieveChunkResponse {
            result: Some(retrieve_chunk_response::Result::Error(ChunkError {
                address: address.to_vec(),
                message: message.to_owned(),
            })),
        }
    }

    fn handle() -> Arc<StreamHandle> {
        let (requests, _rx) = mpsc::channel(1);
        Arc::new(StreamHandle {
            requests,
            waiters: Mutex::new(Waiters::default()),
        })
    }

    #[tokio::test]
    async fn route_demuxes_by_address_and_preserves_fifo() {
        let handle = handle();
        let addr_a = vec![0xaa_u8; 32];
        let addr_b = vec![0xbb_u8; 32];

        let (tx_a1, rx_a1) = oneshot::channel();
        let (tx_a2, rx_a2) = oneshot::channel();
        let (tx_b, rx_b) = oneshot::channel();
        handle.register(addr_a.clone(), tx_a1).expect("open");
        handle.register(addr_a.clone(), tx_a2).expect("open");
        handle.register(addr_b.clone(), tx_b).expect("open");

        // Responses arrive out of registration order; each still finds its
        // address, and the two waiters for A resolve in FIFO order.
        route(&handle, chunk_response(&addr_b, b"B"));
        route(&handle, error_response(&addr_a, "miss"));
        route(&handle, chunk_response(&addr_a, b"A2"));

        assert_eq!(rx_b.await.expect("recv").expect("ok"), b"B");
        assert!(
            rx_a1.await.expect("recv").is_err(),
            "first A waiter got miss"
        );
        assert_eq!(rx_a2.await.expect("recv").expect("ok"), b"A2");
    }

    #[tokio::test]
    async fn close_drains_pending_and_blocks_new_registration() {
        let handle = handle();
        let addr = vec![0xcc_u8; 32];
        let (tx, rx) = oneshot::channel();
        handle.register(addr.clone(), tx).expect("open");

        handle.close_and_drain();

        assert!(handle.is_closed());
        match rx.await.expect("recv") {
            Err(GrpcStoreError::StreamReset) => {}
            other => panic!("expected StreamReset, got {other:?}"),
        }
        let (tx2, _rx2) = oneshot::channel();
        assert!(
            handle.register(addr, tx2).is_err(),
            "registration on a closed handle must fail so the caller re-opens"
        );
    }

    /// Instant sleeper so the retry decorator never waits real time in tests.
    #[derive(Clone, Copy)]
    struct NoSleep;

    impl Sleeper for NoSleep {
        fn sleep(&self, _dur: std::time::Duration) -> impl std::future::Future<Output = ()> {
            std::future::ready(())
        }
    }

    /// Opener whose first stream dies immediately (transport fault) and whose
    /// later streams serve the probe chunk, exercising the re-open path.
    struct FlakyOpener {
        opens: Arc<AtomicUsize>,
        address: Vec<u8>,
        body: Vec<u8>,
    }

    impl StreamOpener for FlakyOpener {
        async fn open(
            &self,
        ) -> Result<(mpsc::Sender<RetrieveChunkRequest>, ResponseStream), GrpcStoreError> {
            let n = self.opens.fetch_add(1, Ordering::SeqCst);
            let (req_tx, mut req_rx) = mpsc::channel::<RetrieveChunkRequest>(16);
            let (resp_tx, resp_rx) =
                mpsc::channel::<Result<RetrieveChunkResponse, tonic::Status>>(16);

            if n == 0 {
                // First stream: drop the response side so it ends at once,
                // simulating the shared stream breaking under every waiter.
                drop(resp_tx);
                tokio::spawn(async move { while req_rx.recv().await.is_some() {} });
            } else {
                // Healthy stream: answer each request with the probe chunk.
                let address = self.address.clone();
                let body = self.body.clone();
                tokio::spawn(async move {
                    while req_rx.recv().await.is_some() {
                        let resp = chunk_response(&address, &body);
                        if resp_tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                });
            }

            Ok((req_tx, Box::pin(ReceiverStream::new(resp_rx))))
        }
    }

    #[tokio::test]
    async fn reopens_after_stream_death_and_recovers() {
        let chunk: AnyChunk<BS> = ContentChunk::<BS>::new("re-open probe")
            .expect("build chunk")
            .into();
        let address = *chunk.address();
        let body = chunk.into_bytes().to_vec();

        let opens = Arc::new(AtomicUsize::new(0));
        let streaming = StreamingChunkGet::with_opener(FlakyOpener {
            opens: Arc::clone(&opens),
            address: address.as_bytes().to_vec(),
            body,
        });
        // The retry decorator is what re-issues after the first stream resets;
        // the re-issue lands on the re-opened healthy stream.
        let store = RetryingChunkGet::with_default(streaming, NoSleep);

        let got = store.get(&address).await.expect("recovered after re-open");
        assert_eq!(got.address(), &address);
        assert!(
            opens.load(Ordering::SeqCst) >= 2,
            "the dead stream must have forced at least one re-open"
        );
    }
}
