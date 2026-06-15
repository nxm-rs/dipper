//! `dipper node ...` command handlers.

use anyhow::Result;

use crate::proto::node::{GetStatusRequest, GetTopologyRequest};
use crate::rpc;

/// `dipper node status` — fetch and pretty-print node status.
pub(crate) async fn status(endpoint: &str) -> Result<()> {
    let mut client = rpc::node_client(endpoint).await?;
    let resp = client.get_status(GetStatusRequest {}).await?.into_inner();

    println!("Overlay address:     0x{}", resp.overlay_address);
    println!("Kademlia depth:      {}", resp.depth);
    println!("Connected peers:     {}", resp.connected_peers);
    println!("Known peers:         {}", resp.known_peers);
    println!("Pending connections: {}", resp.pending_connections);
    println!("Stored peers:        {}", resp.stored_peers);
    Ok(())
}

/// `dipper node topology` — fetch and pretty-print the topology, bin by bin.
pub(crate) async fn topology(endpoint: &str) -> Result<()> {
    let mut client = rpc::node_client(endpoint).await?;
    let resp = client
        .get_topology(GetTopologyRequest {})
        .await?
        .into_inner();

    println!("Overlay address: 0x{}", resp.overlay_address);
    println!("Depth:           {}", resp.depth);
    println!();
    println!("{:>3}  {:>9}  {:>9}", "PO", "connected", "known");
    for bin in &resp.bins {
        println!(
            "{:>3}  {:>9}  {:>9}",
            bin.proximity_order, bin.connected_peers, bin.known_peers
        );
        for peer in &bin.connected_peer_addresses {
            println!("       - 0x{peer}");
        }
    }
    Ok(())
}
