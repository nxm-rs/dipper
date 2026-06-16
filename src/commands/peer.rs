//! `dipper peer ...` handlers over the node's peer-administration gRPC surface.

use anyhow::{Context, Result};

use crate::proto::node::{
    AddPeerRequest, ConnectionDirection, ListPeersRequest, PeerDiagnostics, RemovePeerRequest,
    TrustLevel,
};
use crate::rpc;

/// `dipper peer add <multiaddr>` — dial a peer by multiaddr.
pub(crate) async fn add(endpoint: &str, multiaddr: &str) -> Result<()> {
    let mut client = rpc::node_client(endpoint).await?;
    let resp = client
        .add_peer(AddPeerRequest {
            multiaddr: multiaddr.to_string(),
        })
        .await
        .context("AddPeer RPC failed")?
        .into_inner();

    if resp.accepted {
        println!("Dial queued for {multiaddr}");
    } else {
        println!("Dial rejected for {multiaddr}");
    }
    if let Some(peer) = resp.peer {
        print_long(&peer);
    }
    Ok(())
}

/// `dipper peer remove <overlay>` — disconnect a peer by overlay address.
pub(crate) async fn remove(endpoint: &str, overlay: &str) -> Result<()> {
    let mut client = rpc::node_client(endpoint).await?;
    let resp = client
        .remove_peer(RemovePeerRequest {
            overlay: overlay.to_string(),
        })
        .await
        .context("RemovePeer RPC failed")?
        .into_inner();

    if resp.accepted {
        println!("Disconnect queued for {overlay}");
    } else {
        println!("Disconnect rejected for {overlay}");
    }
    Ok(())
}

/// `dipper peer list [--long]` — list peer diagnostics.
pub(crate) async fn list(endpoint: &str, long: bool, include_known: bool) -> Result<()> {
    let mut client = rpc::node_client(endpoint).await?;
    let resp = client
        .list_peers(ListPeersRequest { include_known })
        .await
        .context("ListPeers RPC failed")?
        .into_inner();

    if resp.peers.is_empty() {
        println!("No peers.");
        return Ok(());
    }

    if long {
        for peer in &resp.peers {
            print_long(peer);
            println!();
        }
        return Ok(());
    }

    // Short table: overlay, PO, connected, direction, trust.
    println!(
        "{:<6}  {:>3}  {:<9}  {:<8}  {:<11}  overlay",
        "STATE", "PO", "direction", "trust", "score"
    );
    for peer in &resp.peers {
        let state = if peer.connected { "up" } else { "known" };
        let score = peer
            .score
            .map(|s| format!("{s:+.1}"))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<6}  {:>3}  {:<9}  {:<8}  {:<11}  0x{}",
            state,
            peer.proximity_order,
            direction_str(peer.direction),
            trust_str(peer.trust),
            score,
            peer.overlay,
        );
    }
    Ok(())
}

/// Print the full diagnostics block for a single peer.
fn print_long(peer: &PeerDiagnostics) {
    println!("Overlay:         0x{}", peer.overlay);
    if let Some(peer_id) = &peer.peer_id {
        println!("Peer ID:         {peer_id}");
    }
    println!("Connected:       {}", peer.connected);
    println!("Proximity order: {}", peer.proximity_order);
    println!("Direction:       {}", direction_str(peer.direction));
    println!("Trust:           {}", trust_str(peer.trust));
    println!("Verified:        {}", peer.verified);
    if let Some(score) = peer.score {
        println!("Score:           {score:+.1}");
    }
    if let Some(ip) = &peer.ip {
        println!("IP:              {ip}");
    }
    if let Some(uptime) = peer.uptime_secs {
        println!("Uptime:          {uptime}s");
    }
    if let Some(since) = peer.connected_since {
        println!("Connected since: {since} (unix)");
    }
    if peer.multiaddrs.is_empty() {
        println!("Multiaddrs:      (none known)");
    } else {
        println!("Multiaddrs:");
        for addr in &peer.multiaddrs {
            println!("  - {addr}");
        }
    }
}

/// Render the proto connection-direction enum.
fn direction_str(direction: i32) -> &'static str {
    match ConnectionDirection::try_from(direction) {
        Ok(ConnectionDirection::Inbound) => "inbound",
        Ok(ConnectionDirection::Outbound) => "outbound",
        Ok(ConnectionDirection::Unspecified) | Err(_) => "-",
    }
}

/// Render the proto trust-level enum.
fn trust_str(trust: i32) -> &'static str {
    match TrustLevel::try_from(trust) {
        Ok(TrustLevel::Normal) => "normal",
        Ok(TrustLevel::LocalSubnet) => "local",
        Ok(TrustLevel::Trusted) => "trusted",
        Err(_) => "-",
    }
}
