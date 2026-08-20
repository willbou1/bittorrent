use reqwest::Url;
use anyhow::Result;
use tracing::{info};
use std::{
    fmt,
    time::{Duration, Instant},
};

use crate::{
    bencode::BencodeValue,
    peer::PeerId,
};

pub enum Event {
    Started,
    Completed,
    Stopped,
}

impl Event {
    fn to_string(&self) -> &str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
        }
    }
}

#[derive(Clone)]
pub struct PeerInfo {
    pub id: Option<[u8; 20]>,
    pub host: String,
    pub port: u16,
}

impl PeerInfo {
    fn from_bencode(root: &BencodeValue) -> Result<Vec<Self>, String> {
        match root {
            BencodeValue::List(peers) => {
                peers.iter().map(|peer| Ok(Self {
                    id: Some(peer.required_bytes("peer id")?.try_into().map_err(
                        |_| "'id' needs to be 20 bytes long"
                    )?),
                    host: peer.required_string("ip")?,
                    port: peer.required_unsigned("port")?.try_into().map_err(
                        |_| ""
                    )?,
                }))
                    .collect::<Result<_, _>>()
            },
            BencodeValue::ByteString(peers) => {
                Ok(peers.chunks(6)
                    .map(|peer| Self {
                        id: None,
                        host: peer[0..4].iter().map(u8::to_string)
                            .collect::<Vec<_>>().join("."),
                        port: ((peer[4] as u16) << 8) | peer[5] as u16,
                    })
                    .collect())
            },
            _ => Err(format!("'peers' must be either a list or a byte string")),
        }
    }
}

pub struct Tracker {
    url: String,
    info_hash: String,
    client_id: String,
    last_announce_time: Instant,

    pub interval: Option<u64>,
    pub min_interval: Option<u64>,
    pub complete: Option<u64>,
    pub incomplete: Option<u64>,
    pub peers: Vec<PeerInfo>,
}

impl Tracker {
    pub fn from_url(url: &str, client_id: &PeerId, info_hash: &[u8; 20]) -> Self {
        let info_hash = info_hash
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        let client_id = client_id
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        Self {
            last_announce_time: Instant::now(),
            url: url.to_string(),
            info_hash,
            client_id,

            interval: None,
            min_interval: None,
            complete: None,
            incomplete: None,
            peers: Vec::new(),
        }
    }

    pub async fn tick(
        &mut self,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: Event,
    ) -> bool {
        if let Some(interval) = self.interval {
            // TODO remove fake interval
            if self.last_announce_time.elapsed() > Duration::from_secs(30) {
                self.announce(uploaded, downloaded, left, event).await;
                return true;
            }
        }
        false
    }

    pub async fn announce(
        &mut self,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: Event,
    ) -> bool {
        self.last_announce_time = Instant::now();
        match self.request(uploaded, downloaded, left, event).await {
            Ok(()) => {
                info!(tracker = %self.url, "Successfully announced");
                true
            }
            Err(e) => {
                info!(tracker = %self.url, "Failed to announce {e}");
                false
            }
        }
    }

    async fn request(
        &mut self,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: Event,
    ) -> Result<()> {
        let mut url = Url::parse(&self.url)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("port", "6881");
            query.append_pair("uploaded", &uploaded.to_string());
            query.append_pair("downloaded", &downloaded.to_string());
            query.append_pair("left", &left.to_string());
            query.append_pair("event", &event.to_string());
        }

        let res = reqwest::get(
            format!("{url}&info_hash={}&peer_id={}", self.info_hash, self.client_id)
        ).await?;
        let body = res.bytes().await?;
        self.decode_request(&body)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        Ok(())
    }

    fn decode_request(&mut self, encoded: &[u8]) -> Result<(), String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        self.interval = Some(root.required_unsigned("interval")?);
        self.min_interval = root.optional_unsigned("min interval")?;
        self.complete = root.optional_unsigned("complete")?;
        self.incomplete = root.optional_unsigned("incomplete")?;
        self.peers = PeerInfo::from_bencode(root.required("peers")?)?;

        Ok(())
    }
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)?;
        if let Some(id) = self.id {
            write!(
                f,
                " ({:02x}{:02x}{:02x}{:02x}...{:02x}{:02x}{:02x}{:02x})",
                id[0], id[1], id[2], id[3], id[16], id[17], id[18], id[19],
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for Tracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(interval) = self.interval {
            writeln!(f, "Interval: {}", interval)?;
        }
        if let Some(min_interval) = self.min_interval {
            writeln!(f, "Minimum interval: {}", min_interval)?;
        }
        if let Some(complete) = self.complete {
            writeln!(f, "Complete: {}", complete)?;
        }
        if let Some(incomplete) = self.incomplete {
            writeln!(f, "Incomplete: {}", incomplete)?;
        }
        writeln!(f, "Peers:")?;
        for peer in &self.peers {
            writeln!(f, "    {}", peer)?;
        }
        Ok(())
    }
}
