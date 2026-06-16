//! gRPC connection helpers: one shared channel per endpoint, clients cloned off it.

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};

use crate::proto::chunk::chunk_client::ChunkClient;
use crate::proto::node::node_client::NodeClient;

/// Open the shared channel to the node, failing fast if it is down.
pub(crate) async fn connect(endpoint: &str) -> Result<Channel> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint URL: {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("failed to connect to {endpoint}"))?;
    Ok(channel)
}

/// Build a [`NodeClient`] over a clone of the shared channel.
pub(crate) fn node_client_on(channel: Channel) -> NodeClient<Channel> {
    NodeClient::new(channel)
}

/// Build a [`ChunkClient`] over a clone of the shared channel.
pub(crate) fn chunk_client_on(channel: Channel) -> ChunkClient<Channel> {
    ChunkClient::new(channel)
}

/// Build a [`NodeClient`], opening the shared channel.
pub(crate) async fn node_client(endpoint: &str) -> Result<NodeClient<Channel>> {
    Ok(node_client_on(connect(endpoint).await?))
}

/// Build a [`ChunkClient`], opening the shared channel.
pub(crate) async fn chunk_client(endpoint: &str) -> Result<ChunkClient<Channel>> {
    Ok(chunk_client_on(connect(endpoint).await?))
}
