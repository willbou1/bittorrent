use anyhow::Result;
use tracing::{trace, warn};
use tokio::{
    net::UdpSocket,
};
use reqwest::Url;
use rand::Rng;
use std::{
    fmt,
    time::{Duration},
};

use crate::{
    bencode::BencodeValue,
    types::*,
};

#[derive(Clone, PartialEq, Eq)]
pub enum Event {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

impl Event {
    fn to_string(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
        }
    }
}

pub struct TrackerResponse {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub peers: Vec<PeerInfo>,
}

impl TrackerResponse {
    fn from_bencode(encoded: &[u8]) -> Result<Self, String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        Ok(Self {
            interval: root.required_unsigned("interval")?,
            min_interval: root.optional_unsigned("min interval")?,
            seeders: root.optional_unsigned("complete")?,
            leechers: root.optional_unsigned("incomplete")?,
            peers: PeerInfo::from_tracker_bencode(&root)?,
        })
    }

    fn from_udp_response(response: &[u8]) -> Self {
        Self {
            interval: i32::from_be_bytes(response[8..12].try_into().unwrap()) as u64,
            min_interval: None,
            leechers: Some(i32::from_be_bytes(response[12..16].try_into().unwrap()) as u64),
            seeders: Some(i32::from_be_bytes(response[16..20].try_into().unwrap()) as u64),
            peers: PeerInfo::from_compact(&response[20..]),
        }
    }
}

impl fmt::Display for TrackerResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Interval: {}", self.interval)?;
        if let Some(min_interval) = self.min_interval {
            writeln!(f, "Minimum interval: {}", min_interval)?;
        }
        if let Some(complete) = self.seeders {
            writeln!(f, "Complete: {}", complete)?;
        }
        if let Some(incomplete) = self.leechers {
            writeln!(f, "Incomplete: {}", incomplete)?;
        }
        writeln!(f, "Peers:")?;
        for peer in &self.peers {
            writeln!(f, "    {}", peer)?;
        }
        Ok(())
    }
}

pub struct Progress {
    pub downloaded: usize,
    pub uploaded: usize,
    pub left: usize,
    pub event: Event,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            downloaded: 0,
            uploaded: 0,
            left: 0,
            event: Event::None,
        }
    }
}

pub async fn request_udp(
    url: &str,
    client_id: &PeerId,
    info_hash: &Hash,
    progress: &Progress,
) -> Result<TrackerResponse> {
    const MAX_ATTEMPTS: usize = 2;

    let mut request = [0u8; 98];
    let mut response = [0u8; 65535];
    let mut connection_id = [0u8; 8];

    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.connect(
        url.split("//").nth(1).unwrap()
            .split("/").next().unwrap()
    ).await?;

    // connect request
    request[..8].copy_from_slice(
        &0x41727101980u64.to_be_bytes()
    );
    request[8..12].copy_from_slice(
        &0i32.to_be_bytes()
    ); // action: connect
    rand::rng().fill_bytes(&mut request[12..16]);

    // connect response
    for n in 0..(MAX_ATTEMPTS + 1) {
        anyhow::ensure!(
            n < MAX_ATTEMPTS,
            "Max attemps for connection"
        );

        sock.send(&request[..16]).await?;

        match tokio::time::timeout(
            Duration::from_secs(15 * (1 << n)),
            sock.recv(&mut response)).await {
                Ok(len) => {
                    let len = len?;
                    if len < 16 {
                        trace!(tracker = %url, "Expected connect response to be at least 16 bytes, got {len}");
                        continue;
                    }
                    let action = u32::from_be_bytes(response[..4].try_into()?);
                    if action != 0 {
                        trace!(tracker = %url, "Expected connect (0) action, got {action}");
                        continue;
                    }
                    if response[4..8] != request[12..16] {
                        trace!(tracker = %url, "Expected connect transaction id to match");
                        continue;
                    }
                    connection_id.copy_from_slice(&response[8..16]);
                    break;
                }
                Err(_) => {
                    trace!(tracker = %url, "Connection timed out");
                    continue;
                }
            }
    }
    trace!(tracker = %url, "Connected");

    // announce request
    request[..8].copy_from_slice(&connection_id);
    request[8..12].copy_from_slice(
        &1i32.to_be_bytes()
    ); // action: announce
    rand::rng().fill_bytes(&mut request[12..16]);
    request[16..36].copy_from_slice(info_hash.as_bytes());
    request[36..56].copy_from_slice(client_id.as_bytes());
    request[56..64].copy_from_slice(
        &(progress.downloaded as i64).to_be_bytes()
    );
    request[64..72].copy_from_slice(
        &(progress.left as i64).to_be_bytes()
    );
    request[72..80].copy_from_slice(
        &(progress.uploaded as i64).to_be_bytes()
    );
    request[80..84].copy_from_slice(
        &(progress.event.clone() as i32).to_be_bytes()
    );
    request[84..88].fill(0); // TODO fill ip
    rand::rng().fill_bytes(&mut request[88..92]);
    request[92..96].copy_from_slice(
        &(-1i32).to_be_bytes()
    ); // num_want
    request[96..98].copy_from_slice(
        &6881u16.to_be_bytes()
    ); // num_want

    // announce response
    for n in 0..(MAX_ATTEMPTS + 1) {
        anyhow::ensure!(
            n < MAX_ATTEMPTS,
            "Max attemps for announce"
        );

        sock.send(&request[..98]).await?;

        match tokio::time::timeout(
            Duration::from_secs(15 * (1 << n)),
            sock.recv(&mut response)).await {
                Ok(len) => {
                    let len = len?;
                    if len < 20 {
                        trace!(tracker = %url, "Expected announce response to be at least 20 bytes, got {len}");
                        continue;
                    }
                    let action = u32::from_be_bytes(response[..4].try_into()?);
                    if action != 1 {
                        trace!(tracker = %url, "Expected announce (1) action, got {action}");
                        continue;
                    }
                    if response[4..8] != request[12..16] {
                        trace!(tracker = %url, "Expected announce transaction id to match");
                        continue;
                    }

                    return Ok(TrackerResponse::from_udp_response(&response[0..len]));
                }
                Err(_) => {
                    trace!(tracker = %url, "Announce timed out");
                    continue;
                }
            }
    }
    Err(anyhow::anyhow!("Unreachable"))
}

pub async fn request_http(
    url: &str,
    client_id: &PeerId,
    info_hash: &Hash,
    progress: &Progress,
) -> Result<TrackerResponse> {
    let mut url = Url::parse(&url)?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("port", "6881");
        query.append_pair("uploaded", &progress.uploaded.to_string());
        query.append_pair("downloaded", &progress.downloaded.to_string());
        query.append_pair("left", &progress.left.to_string());
        if !matches!(progress.event, Event::None) {
            query.append_pair("event", progress.event.to_string());
        }
    }

    let res = reqwest::get(
        format!("{url}&info_hash={}&peer_id={}", info_hash.encode(), client_id.encode())
    ).await?;
    let body = res.bytes().await?;
    let response = TrackerResponse::from_bencode(&body)
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;

    Ok(response)
}

