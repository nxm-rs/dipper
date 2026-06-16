//! `GrpcStore`: a nectar sync chunk store over the vertex node's gRPC API.
//! Writes flow through one streaming upload; reads bridge async RPCs from the sync traits.

use std::sync::{Arc, Mutex};

use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
use nectar_primitives::{
    AnyChunk, ChunkAddress, ContentChunk, DEFAULT_BODY_SIZE,
    store::{SyncChunkGet, SyncChunkHas, SyncChunkPut},
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::proto::chunk::{
    ChunkType, HasChunkRequest, RetrieveChunkRequest, UploadChunkRequest, chunk_client::ChunkClient,
};
use tonic::transport::Channel;

/// Chunk body size used everywhere in dipper.
pub(crate) const BS: usize = DEFAULT_BODY_SIZE;

/// Bound on the in-flight stamped-chunk channel, applying upload backpressure.
const UPLOAD_CHANNEL_CAPACITY: usize = 256;

/// Errors surfaced by [`GrpcStore`]'s chunk operations.
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

    /// The upload stream closed before a chunk could be enqueued.
    #[error("upload stream closed: {0}")]
    UploadClosed(String),
}

/// State shared with the upload drain task: the sender plus its join handle.
struct UploadStream {
    /// Stamped requests; `None` once [`GrpcStore::finish`] closes the channel.
    sender: Mutex<Option<mpsc::Sender<UploadChunkRequest>>>,
    /// The drain task, joined by [`GrpcStore::finish`].
    task: Mutex<Option<JoinHandle<Result<(), GrpcStoreError>>>>,
}

/// Shared inner state behind an [`Arc`] so [`GrpcStore`] is cheaply `Clone`.
pub(crate) struct GrpcStoreInner {
    /// Async tonic client, cloned per-RPC on the read path.
    pub(crate) chunk_client: ChunkClient<Channel>,
    /// One shared stamper so per-bucket indices never collide. `None` read-only.
    pub(crate) stamper: Mutex<Option<BatchStamper<MemoryIssuer, PrivateKeySigner>>>,
    /// Runtime handle driving each async read RPC from the sync trait methods.
    pub(crate) handle: tokio::runtime::Handle,
    /// Whether the node should re-validate stamps before forwarding.
    pub(crate) validate: bool,
    /// The single upload stream every `put` feeds. `None` read-only.
    upload: Option<UploadStream>,
}

/// gRPC-backed chunk store; `Clone` is cheap.
#[derive(Clone)]
pub(crate) struct GrpcStore {
    inner: Arc<GrpcStoreInner>,
}

impl GrpcStore {
    /// Build a writable store from a client, a batch's postage geometry, and a signer.
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

        let upload = Some(Self::spawn_upload(chunk_client.clone(), &handle));

        Self {
            inner: Arc::new(GrpcStoreInner {
                chunk_client,
                stamper: Mutex::new(Some(stamper)),
                handle,
                validate,
                upload,
            }),
        }
    }

    /// Build a read-only store (no stamping/upload) for `ls`/`download`.
    pub(crate) fn connect_read_only(
        chunk_client: ChunkClient<Channel>,
        _signer: PrivateKeySigner,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner: Arc::new(GrpcStoreInner {
                chunk_client,
                stamper: Mutex::new(None),
                handle,
                validate: false,
                upload: None,
            }),
        }
    }

    /// Spawn the upload drain task owning one client-streaming `upload_chunks` call.
    fn spawn_upload(
        mut client: ChunkClient<Channel>,
        handle: &tokio::runtime::Handle,
    ) -> UploadStream {
        let (tx, rx) = mpsc::channel::<UploadChunkRequest>(UPLOAD_CHANNEL_CAPACITY);

        let task = handle.spawn(async move {
            let request_stream = ReceiverStream::new(rx);
            let mut receipts = client.upload_chunks(request_stream).await?.into_inner();

            // Drain the receipts so the server keeps making progress; the
            // receipt contents are not surfaced per-chunk here, but a transport
            // error or a server-side upload failure aborts the session.
            while let Some(receipt) = receipts.message().await? {
                let _ = receipt;
            }
            Ok(())
        });

        UploadStream {
            sender: Mutex::new(Some(tx)),
            task: Mutex::new(Some(task)),
        }
    }

    /// Close the upload stream and await the drain task, surfacing any upload failure. Idempotent.
    pub(crate) async fn finish(&self) -> Result<(), GrpcStoreError> {
        if let Some(upload) = &self.inner.upload {
            {
                let mut guard = upload.sender.lock().map_err(|e| {
                    GrpcStoreError::UploadClosed(format!("sender mutex poisoned: {e}"))
                })?;
                *guard = None;
            }

            let task = {
                let mut guard = upload.task.lock().map_err(|e| {
                    GrpcStoreError::UploadClosed(format!("task mutex poisoned: {e}"))
                })?;
                guard.take()
            };

            if let Some(handle) = task {
                handle.await.map_err(|e| {
                    GrpcStoreError::UploadClosed(format!("upload task panicked: {e}"))
                })??;
            }
        }

        Ok(())
    }

    /// Drive an async future to completion from a sync context.
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
            let mut guard = self
                .inner
                .stamper
                .lock()
                .map_err(|e| GrpcStoreError::Stamp(format!("stamper mutex poisoned: {e}")))?;
            let stamper = guard.as_mut().ok_or_else(|| {
                GrpcStoreError::Stamp("put on a read-only or finished store".to_owned())
            })?;
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

        // Hand the stamped chunk to the single upload stream. `blocking_send`
        // parks the calling splitter/manifest worker when the channel is full,
        // applying backpressure without needing the runtime handle. A closed
        // channel means the drain task ended early (a server-side abort);
        // `finish` will surface the underlying error.
        let upload =
            self.inner.upload.as_ref().ok_or_else(|| {
                GrpcStoreError::UploadClosed("put on a read-only store".to_owned())
            })?;
        let sender = {
            let guard = upload
                .sender
                .lock()
                .map_err(|e| GrpcStoreError::UploadClosed(format!("sender mutex poisoned: {e}")))?;
            guard
                .as_ref()
                .ok_or_else(|| {
                    GrpcStoreError::UploadClosed("upload stream already finished".to_owned())
                })?
                .clone()
        };
        sender
            .blocking_send(request)
            .map_err(|_| GrpcStoreError::UploadClosed("upload stream closed".to_owned()))?;
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
