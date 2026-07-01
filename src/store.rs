//! `GrpcStore`: a nectar chunk store backed by the vertex node's gRPC API.
//!
//! Implements nectar's async `ChunkGet`/`ChunkPut`/`ChunkHas`, so the file
//! splitter (`write_file`/`Joiner`) and mantaray manifest can drive this store
//! directly.
//!
//! `put` stamps each chunk (via a single shared [`BatchStamper`] so the issuer
//! tracks per-bucket indices monotonically) and uploads it; `get` retrieves the
//! wire body and reconstructs + verifies a `ContentChunk`.

use std::sync::{Arc, Mutex};

use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
use nectar_primitives::{
    AnyChunk, ChunkAddress, ContentChunk, DEFAULT_BODY_SIZE, Sleeper,
    store::{ChunkGet, ChunkHas, ChunkPut},
};

use crate::proto::chunk::{
    ChunkType, HasChunkRequest, RetrieveChunkRequest, UploadChunkRequest,
    chunk_client::ChunkClient, retrieve_chunk_response, upload_chunk_response,
};
use tonic::transport::Channel;

/// Chunk body size used everywhere in dipper (4096 bytes).
pub(crate) const BS: usize = DEFAULT_BODY_SIZE;

/// Errors surfaced by [`GrpcStore`]'s chunk operations.
///
/// Must be a concrete `std::error::Error + Send + Sync + 'static` to satisfy
/// the `ChunkGet`/`ChunkPut` associated-`Error` bound (so it cannot be
/// `anyhow::Error`).
#[derive(Debug, thiserror::Error)]
pub(crate) enum GrpcStoreError {
    /// Transport / status error from a gRPC call.
    #[error("grpc transport error: {0}")]
    Transport(#[from] tonic::Status),

    /// Chunk reconstruction / verification failure.
    #[error("chunk error: {0}")]
    Chunk(#[from] nectar_primitives::PrimitivesError),

    /// Stamping (postage issuance/signing) failure.
    #[error("stamping error: {0}")]
    Stamp(String),

    /// The requested chunk was not found on the network.
    #[error("chunk not found: {0}")]
    NotFound(String),

    /// The shared `RetrieveChunks` stream broke (transport fault or clean
    /// close) before the response arrived. Distinct from a per-address miss so
    /// the caller re-issues onto a freshly re-opened stream rather than treating
    /// the whole file as unavailable.
    #[error("retrieve stream reset")]
    StreamReset,
}

/// Shared inner state behind an [`Arc`] so [`GrpcStore`] is cheaply `Clone`
/// (required by the joiner's `G: Clone + Send + Sync` bound).
pub(crate) struct GrpcStoreInner {
    /// Async tonic client; cloned per-RPC for `&self` access.
    pub(crate) chunk_client: ChunkClient<Channel>,
    /// One shared stamper; the issuer must be a single instance so per-bucket
    /// indices never collide across chunks. Behind a `Mutex` because `put`
    /// takes `&self` yet stamping mutates issuer state.
    pub(crate) stamper: Mutex<BatchStamper<MemoryIssuer, PrivateKeySigner>>,
    /// Whether the node should re-validate stamps before forwarding.
    pub(crate) validate: bool,
}

/// gRPC-backed chunk store. `Clone` is cheap (shares one [`GrpcStoreInner`]).
#[derive(Clone)]
pub(crate) struct GrpcStore {
    inner: Arc<GrpcStoreInner>,
}

impl GrpcStore {
    /// Build a store from a connected chunk client, a batch's postage geometry,
    /// and a signer. The stamper is constructed once and shared.
    pub(crate) fn new(
        chunk_client: ChunkClient<Channel>,
        batch_id: alloy_primitives::B256,
        depth: u8,
        bucket_depth: u8,
        signer: PrivateKeySigner,
        validate: bool,
    ) -> Self {
        let issuer = MemoryIssuer::new(batch_id, depth, bucket_depth);
        let stamper = BatchStamper::new(issuer, signer);
        Self {
            inner: Arc::new(GrpcStoreInner {
                chunk_client,
                stamper: Mutex::new(stamper),
                validate,
            }),
        }
    }

    /// Build a read-only store (no stamping/upload) for `ls`/`download`.
    ///
    /// The stamper is present but unused on the read path; `get`/`has` never
    /// touch it. A dummy zero batch is fine because reads do not stamp.
    pub(crate) fn connect_read_only(
        chunk_client: ChunkClient<Channel>,
        signer: PrivateKeySigner,
    ) -> Self {
        Self::new(
            chunk_client,
            alloy_primitives::B256::ZERO,
            0,
            0,
            signer,
            false,
        )
    }
}

impl ChunkGet<BS> for GrpcStore {
    type Error = GrpcStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        let mut client = self.inner.chunk_client.clone();

        let resp = client
            .retrieve_chunk(RetrieveChunkRequest {
                address: address.as_bytes().to_vec(),
            })
            .await?
            .into_inner();

        let data = match resp.result {
            Some(retrieve_chunk_response::Result::Chunk(c)) => c.data,
            Some(retrieve_chunk_response::Result::Error(e)) => {
                return Err(GrpcStoreError::NotFound(e.message));
            }
            None => return Err(GrpcStoreError::NotFound(hex::encode(address.as_bytes()))),
        };

        if data.is_empty() {
            return Err(GrpcStoreError::NotFound(hex::encode(address.as_bytes())));
        }

        // The wire body (span || payload) is exactly what ContentChunk consumes.
        let bytes = nectar_primitives::bytes::Bytes::from(data);
        let chunk = ContentChunk::<BS>::try_from(bytes)?;
        let chunk: AnyChunk<BS> = chunk.into();

        // Reject tampered data: the BMT address must match what we asked for.
        chunk.verify(address)?;
        Ok(chunk)
    }
}

impl ChunkPut<BS> for GrpcStore {
    type Error = GrpcStoreError;

    async fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        let address = *chunk.address();

        // Stamp the chunk; the shared issuer assigns the next free per-bucket
        // index for this address. The guard is dropped before the upload await
        // so the returned future stays `Send`.
        let stamp = {
            let mut stamper = self
                .inner
                .stamper
                .lock()
                .map_err(|e| GrpcStoreError::Stamp(format!("stamper mutex poisoned: {e}")))?;
            stamper
                .stamp(&address)
                .map_err(|e| GrpcStoreError::Stamp(e.to_string()))?
        };

        // The wire body (span || payload) goes into UploadChunkRequest.data.
        let body = chunk.into_bytes().to_vec();
        let request = UploadChunkRequest {
            data: body,
            stamp: stamp.to_bytes().to_vec(),
            address: address.as_bytes().to_vec(),
            chunk_type: ChunkType::Content as i32,
            validate: self.inner.validate,
        };

        let mut client = self.inner.chunk_client.clone();
        let resp = client.upload_chunk(request).await?.into_inner();
        match resp.result {
            Some(upload_chunk_response::Result::Receipt(_)) => Ok(()),
            Some(upload_chunk_response::Result::Error(e)) => Err(GrpcStoreError::Stamp(e.message)),
            None => Err(GrpcStoreError::Stamp("upload returned no result".into())),
        }
    }
}

impl ChunkHas<BS> for GrpcStore {
    async fn has(&self, address: &ChunkAddress) -> bool {
        let mut client = self.inner.chunk_client.clone();

        // `has` has no error channel; log and swallow transport failures.
        match client
            .has_chunk(HasChunkRequest {
                address: address.as_bytes().to_vec(),
            })
            .await
        {
            Ok(resp) => resp.into_inner().exists,
            Err(status) => {
                eprintln!("has_chunk RPC failed: {status}");
                false
            }
        }
    }
}

/// Native [`Sleeper`] for [`nectar_primitives::RetryingChunkGet`]: dipper runs
/// on tokio, so the injected delay is `tokio::time::sleep`. `Clone` so the
/// wrapping store stays cheaply cloneable for the joiner.
#[derive(Clone, Copy)]
pub(crate) struct TokioSleeper;

impl Sleeper for TokioSleeper {
    fn sleep(&self, dur: std::time::Duration) -> impl std::future::Future<Output = ()> {
        tokio::time::sleep(dur)
    }
}
