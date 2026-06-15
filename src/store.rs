//! `GrpcStore`: a nectar chunk store backed by the vertex node's gRPC API.
//!
//! Implementing only the *synchronous* `SyncChunkGet`/`SyncChunkPut`/
//! `SyncChunkHas` traits is sufficient — nectar provides a blanket bridge that
//! derives the async `ChunkGet`/`ChunkPut`/`ChunkHas` automatically, so the
//! file splitter (`write_file`/`sync_join`) and mantaray manifest can drive
//! this store directly.
//!
//! `put` stamps each chunk (via a single shared [`BatchStamper`] so the issuer
//! tracks per-bucket indices monotonically) and uploads it; `get` retrieves the
//! wire body and reconstructs + verifies a `ContentChunk`. Both bridge their
//! async RPC over a captured `tokio::runtime::Handle`.
//!
//! ## Sync → async bridging
//!
//! The `SyncChunkGet`/`SyncChunkPut` methods are synchronous, yet the tonic
//! client is async. They are also invoked from the parallel splitter / mantaray
//! worker threads. Each method therefore drives its RPC with
//! [`tokio::task::block_in_place`] plus the captured [`Handle::block_on`], which
//! is the standard pattern for a sync method that must await inside a
//! multi-thread tokio runtime. The callers in `manifest.rs` additionally wrap
//! the CPU-bound split/manifest work in `spawn_blocking` so the BMT hashing
//! runs off the async workers.

use std::sync::{Arc, Mutex};

use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
use nectar_primitives::{
    AnyChunk, ChunkAddress, ContentChunk, DEFAULT_BODY_SIZE,
    store::{SyncChunkGet, SyncChunkHas, SyncChunkPut},
};

use crate::proto::chunk::{
    ChunkType, HasChunkRequest, RetrieveChunkRequest, UploadChunkRequest, chunk_client::ChunkClient,
};
use tonic::transport::Channel;

/// Chunk body size used everywhere in dipper (4096 bytes).
pub(crate) const BS: usize = DEFAULT_BODY_SIZE;

/// Errors surfaced by [`GrpcStore`]'s chunk operations.
///
/// Must be a concrete `std::error::Error + Send + Sync + 'static` to satisfy
/// the `SyncChunkGet`/`SyncChunkPut` associated-`Error` bound (so it cannot be
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
}

/// Shared inner state behind an [`Arc`] so [`GrpcStore`] is cheaply `Clone`
/// (required by `sync_join`'s `G: Clone + Send + Sync` bound).
pub(crate) struct GrpcStoreInner {
    /// Async tonic client; cloned per-RPC for `&self` access.
    pub(crate) chunk_client: ChunkClient<Channel>,
    /// One shared stamper; the issuer must be a single instance so per-bucket
    /// indices never collide across chunks. Behind a `Mutex` because `put`
    /// takes `&self` yet stamping mutates issuer state.
    pub(crate) stamper: Mutex<BatchStamper<MemoryIssuer, PrivateKeySigner>>,
    /// Runtime handle used to drive each async RPC from the sync trait methods.
    pub(crate) handle: tokio::runtime::Handle,
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
        handle: tokio::runtime::Handle,
        validate: bool,
    ) -> Self {
        let issuer = MemoryIssuer::new(batch_id, depth, bucket_depth);
        let stamper = BatchStamper::new(issuer, signer);
        Self {
            inner: Arc::new(GrpcStoreInner {
                chunk_client,
                stamper: Mutex::new(stamper),
                handle,
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
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self::new(
            chunk_client,
            alloy_primitives::B256::ZERO,
            0,
            0,
            signer,
            handle,
            false,
        )
    }

    /// Drive an async future to completion from a sync context.
    ///
    /// `block_in_place` parks the current multi-thread worker so it can block,
    /// then the captured handle drives the future. This is safe because dipper
    /// uses the default multi-thread `#[tokio::main]` runtime and the callers
    /// invoke the splitter/manifest from `spawn_blocking`.
    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        let handle = self.inner.handle.clone();
        tokio::task::block_in_place(move || handle.block_on(fut))
    }
}

impl SyncChunkGet<BS> for GrpcStore {
    type Error = GrpcStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        let address_hex = hex::encode(address.as_bytes());
        let mut client = self.inner.chunk_client.clone();

        let resp = self
            .block_on(async move {
                client
                    .retrieve_chunk(RetrieveChunkRequest {
                        address: address_hex,
                    })
                    .await
            })?
            .into_inner();

        if resp.data.is_empty() {
            return Err(GrpcStoreError::NotFound(hex::encode(address.as_bytes())));
        }

        // The wire body (span || payload) is exactly what ContentChunk consumes.
        let bytes = nectar_primitives::bytes::Bytes::from(resp.data);
        let chunk = ContentChunk::<BS>::try_from(bytes)?;
        let chunk: AnyChunk<BS> = chunk.into();

        // Reject tampered data: the BMT address must match what we asked for.
        chunk.verify(address)?;
        Ok(chunk)
    }
}

impl SyncChunkPut<BS> for GrpcStore {
    type Error = GrpcStoreError;

    fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        let address = *chunk.address();

        // Stamp the chunk; the shared issuer assigns the next free per-bucket
        // index for this address.
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
            address: hex::encode(address.as_bytes()),
            chunk_type: ChunkType::Content as i32,
            validate: self.inner.validate,
        };

        let mut client = self.inner.chunk_client.clone();
        self.block_on(async move { client.upload_chunk(request).await })?;
        Ok(())
    }
}

impl SyncChunkHas<BS> for GrpcStore {
    fn has(&self, address: &ChunkAddress) -> bool {
        let address_hex = hex::encode(address.as_bytes());
        let mut client = self.inner.chunk_client.clone();

        // `has` has no error channel; log and swallow transport failures.
        match self.block_on(async move {
            client
                .has_chunk(HasChunkRequest {
                    address: address_hex,
                })
                .await
        }) {
            Ok(resp) => resp.into_inner().exists,
            Err(status) => {
                eprintln!("has_chunk RPC failed: {status}");
                false
            }
        }
    }
}
