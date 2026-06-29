//! `StreamingChunkGet`: a `ChunkGet` that multiplexes every concurrent
//! `get(addr)` onto one `RetrieveChunks` bidi stream and demuxes responses by
//! address, eliminating the per-chunk unary call.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use nectar_primitives::{AnyChunk, ChunkAddress, ContentChunk, store::ChunkGet};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::proto::chunk::{
    RetrieveChunkRequest, RetrieveChunkResponse, chunk_client::ChunkClient, retrieve_chunk_response,
};
use crate::store::{BS, GrpcStoreError};

/// Outstanding request-channel depth. The joiner bounds how many `get`s are in
/// flight; a full channel parks `get` (backpressure) rather than growing.
const REQUEST_BUFFER: usize = 1024;

type GetResult = Result<Vec<u8>, GrpcStoreError>;

/// Per-address FIFO of response waiters plus a closed flag, all under one lock so
/// registering a waiter and the demux shutdown drain cannot race.
#[derive(Default)]
struct Waiters {
    closed: bool,
    map: HashMap<Vec<u8>, VecDeque<oneshot::Sender<GetResult>>>,
}

struct Inner {
    requests: mpsc::Sender<RetrieveChunkRequest>,
    waiters: Mutex<Waiters>,
}

/// A `ChunkGet` backed by one shared `RetrieveChunks` bidi stream. Cheap to
/// clone (shares one `Inner` by `Arc`), so the joiner's `G: Clone + Send + Sync`
/// bound holds.
#[derive(Clone)]
pub(crate) struct StreamingChunkGet {
    inner: Arc<Inner>,
}

impl StreamingChunkGet {
    /// Open the bidi stream and spawn the demux task. The request stream stays
    /// open until every clone drops, which ends the server side and the demux.
    pub(crate) async fn open(mut client: ChunkClient<Channel>) -> Result<Self, GrpcStoreError> {
        let (requests, request_rx) = mpsc::channel::<RetrieveChunkRequest>(REQUEST_BUFFER);
        let responses = client
            .retrieve_chunks(ReceiverStream::new(request_rx))
            .await?
            .into_inner();

        let inner = Arc::new(Inner {
            requests,
            waiters: Mutex::new(Waiters::default()),
        });

        tokio::spawn(demux(responses, Arc::clone(&inner)));
        Ok(Self { inner })
    }
}

/// Read responses until the stream ends, routing each to the next waiter for its
/// address. On end or error, mark closed and drain every pending waiter so no
/// `get` hangs.
async fn demux(mut responses: tonic::Streaming<RetrieveChunkResponse>, inner: Arc<Inner>) {
    // Ends on the first `Ok(None)` (clean close) or `Err` (transport fault);
    // either way the drain below releases every pending waiter.
    while let Ok(Some(response)) = responses.message().await {
        route(&inner, response);
    }

    let mut waiters = inner.waiters.lock().expect("waiters mutex");
    waiters.closed = true;
    for (_, queue) in waiters.map.drain() {
        for waiter in queue {
            let _ = waiter.send(Err(GrpcStoreError::NotFound(
                "retrieve stream closed".to_string(),
            )));
        }
    }
}

/// Deliver one response to the head waiter registered for its address.
fn route(inner: &Inner, response: RetrieveChunkResponse) {
    let (address, result) = match response.result {
        Some(retrieve_chunk_response::Result::Chunk(chunk)) => (chunk.address, Ok(chunk.data)),
        Some(retrieve_chunk_response::Result::Error(error)) => {
            (error.address, Err(GrpcStoreError::NotFound(error.message)))
        }
        None => return,
    };

    let mut waiters = inner.waiters.lock().expect("waiters mutex");
    if let Some(queue) = waiters.map.get_mut(&address) {
        if let Some(waiter) = queue.pop_front() {
            let _ = waiter.send(result);
        }
        if queue.is_empty() {
            waiters.map.remove(&address);
        }
    }
}

impl ChunkGet<BS> for StreamingChunkGet {
    type Error = GrpcStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        let key = address.as_bytes().to_vec();
        let (tx, rx) = oneshot::channel::<GetResult>();

        {
            let mut waiters = self.inner.waiters.lock().expect("waiters mutex");
            if waiters.closed {
                return Err(GrpcStoreError::NotFound(hex::encode(&key)));
            }
            waiters.map.entry(key.clone()).or_default().push_back(tx);
        }

        // Register THEN send, so a fast response can never arrive before its
        // waiter is in place.
        if self
            .inner
            .requests
            .send(RetrieveChunkRequest {
                address: key.clone(),
            })
            .await
            .is_err()
        {
            return Err(GrpcStoreError::NotFound(hex::encode(&key)));
        }

        let data = match rx.await {
            Ok(result) => result?,
            Err(_) => return Err(GrpcStoreError::NotFound(hex::encode(&key))),
        };

        if data.is_empty() {
            return Err(GrpcStoreError::NotFound(hex::encode(&key)));
        }

        // Same reconstruction + BMT verification as the unary `get`.
        let bytes = nectar_primitives::bytes::Bytes::from(data);
        let chunk: AnyChunk<BS> = ContentChunk::<BS>::try_from(bytes)?.into();
        chunk.verify(address)?;
        Ok(chunk)
    }
}
