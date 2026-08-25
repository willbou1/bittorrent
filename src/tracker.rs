use rand::seq::SliceRandom;
use reqwest::Url;
use anyhow::Result;
use tracing::{info, trace};
use std::{
    fmt,
    time::{Duration, Instant},
    collections::HashSet,
};
use tokio::{
    sync::mpsc,
};

use crate::{
    bencode::BencodeValue,
    peer::PeerId,
    torrent,
};
use rand;

#[derive(Clone)]
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

#[derive(Clone, Hash, Eq)]
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

pub struct TrackerTier {
    urls: Vec<String>,
    info_hash: String,
    client_id: String,
    last_announce_time: Instant,
    tier: usize,
    tx: mpsc::Sender<torrent::Event>,

    pub interval: u64,
    pub min_interval: Option<u64>,
    pub complete: Option<u64>,
    pub incomplete: Option<u64>,
    pub peers: Vec<PeerInfo>,
}

impl TrackerTier {
    pub fn from_urls(
        tier: usize,
        tx: mpsc::Sender<torrent::Event>,
        urls: &[String],
        client_id: &PeerId,
        info_hash: &[u8; 20],
    ) -> Self {
        let mut urls = urls.to_vec();
        urls.shuffle(&mut rand::rng());
        
        let info_hash = info_hash
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        let client_id = client_id
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        Self {
            last_announce_time: Instant::now() - Duration::from_secs(60),
            urls,
            info_hash,
            client_id,
            tier,
            tx,

            interval: 60,
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
        if self.last_announce_time.elapsed() > Duration::from_secs(self.interval) {
            self.last_announce_time = Instant::now();
            let mut good_urls = Vec::new();
            for u in 0..self.urls.len() {
                let url = self.urls[u].clone();
                match self.request(&url, uploaded, downloaded, left, &event).await {
                    Ok(()) => {
                        trace!(tracker = %url, "Successfully announced");
                        good_urls.push(u);
                    }
                    Err(e) => trace!(tracker = %url, "Failed to announced {e}"),
                }
            }
            for u in good_urls {
                let url = self.urls[u].clone();
                self.urls.remove(u);
                self.urls.insert(0, url);
            }
            let _ = self.tx.send(torrent::Event::Discovery(self.peers.clone())).await;
        }
        false
    }

    async fn request(
        &mut self,
        url: &str,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: &Event,
    ) -> Result<()> {
        let mut url = Url::parse(&url)?;
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

        let interval = root.required_unsigned("interval")?;
        if interval < self.interval {
            self.interval = interval;
        }

        // TODO check min interval
        self.min_interval = root.optional_unsigned("min interval")?;
        self.complete = root.optional_unsigned("complete")?;
        self.incomplete = root.optional_unsigned("incomplete")?;

        let new_peers = PeerInfo::from_bencode(root.required("peers")?)?;
        let mut peers = HashSet::new();
        peers.extend(self.peers);
        peers.extend(new_peers);
        self.peers = peers.into_iter().collect();

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

impl fmt::Display for TrackerTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Interval: {}", self.interval)?;
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
