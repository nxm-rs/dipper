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
//! Scaffold note: the store type and its constructors are integration plumbing
//! the upload/download impl consumes. They are allowed-dead here so the stub
//! compiles clippy-clean; the impl phase wires them and this module-level allow
//! is removed.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use alloy_signer_local::PrivateKeySigner;
use nectar_primitives::{
    AnyChunk, ChunkAddress, DEFAULT_BODY_SIZE,
    store::{SyncChunkGet, SyncChunkHas, SyncChunkPut},
};

use crate::proto::chunk::chunk_client::ChunkClient;
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

// The concrete stamper + issuer types live in nectar-postage-issuer.
use nectar_postage_issuer::{BatchStamper, MemoryIssuer};

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
    /// The stamper is unused on the read path; the impl agent may restructure
    /// to avoid requiring a signer for reads (e.g. an `Option<stamper>`), but
    /// the public read methods are `get`/`has` via the trait impls below.
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
}

impl SyncChunkGet<BS> for GrpcStore {
    type Error = GrpcStoreError;

    fn get(&self, _address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        todo!("retrieve_chunk RPC -> ContentChunk::try_from(body) -> verify -> AnyChunk")
    }
}

impl SyncChunkPut<BS> for GrpcStore {
    type Error = GrpcStoreError;

    fn put(&self, _chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        todo!("stamp(address) -> upload_chunk RPC with wire body span||payload")
    }
}

impl SyncChunkHas<BS> for GrpcStore {
    fn has(&self, _address: &ChunkAddress) -> bool {
        todo!("has_chunk RPC; log+swallow transport errors, return false")
    }
}
