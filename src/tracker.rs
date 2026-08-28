use rand::seq::SliceRandom;
use reqwest::Url;
use anyhow::Result;
use tracing::{info, trace, warn, debug};
use std::{
    fmt,
    collections::HashSet,
    time::{Duration},
};
use tokio::{
    sync::mpsc,
    net::UdpSocket,
};

use crate::{
    bencode::BencodeValue,
    metainfo::Metainfo,
    peer::PeerId,
    torrent
};
use rand::Rng;

pub struct Progress {
    pub downloaded: usize,
    pub uploaded: usize,
    pub event: Event,
}

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

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct PeerInfo {
    pub id: Option<[u8; 20]>,
    pub host: String,
    pub port: u16,
}

impl PeerInfo {
    fn from_bencode_value(root: &BencodeValue) -> Result<Vec<Self>, String> {
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
                Ok(Self::from_compact(peers))
            },
            _ => Err(format!("'peers' must be either a list or a byte string")),
        }
    }

    fn from_compact(compact: &[u8]) -> Vec<Self> {
        compact.chunks(6)
            .map(|peer| Self {
                id: None,
                host: peer[0..4].iter().map(u8::to_string)
                    .collect::<Vec<_>>().join("."),
                port: ((peer[4] as u16) << 8) | peer[5] as u16,
            })
            .collect()
    }
}

struct TrackerResponse {
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
            peers: PeerInfo::from_bencode_value(root.required("peers")?)?,
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

pub struct Trackers {
    urls: Vec<Vec<String>>,
    info_hash: [u8; 20],
    client_id: PeerId,
    tx: mpsc::Sender<torrent::Event>,
    rx: mpsc::Receiver<Progress>,
    length: usize,

    pub interval: Option<u64>,
    pub min_interval: Option<u64>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub peers: Vec<PeerInfo>,

    uploaded: usize,
    downloaded: usize,
    event: Event,
}

impl Trackers {
    pub fn from_metainfo(
        tx: mpsc::Sender<torrent::Event>,
        rx: mpsc::Receiver<Progress>,
        client_id: &PeerId,
        metainfo: &Metainfo,
        downloaded: usize,
        uploaded: usize,
    ) -> Self {
        let mut urls = metainfo.announces.to_vec();
        for tier_urls in urls.iter_mut() {
            tier_urls.shuffle(&mut rand::rng());
        }
        
        Self {
            urls,
            info_hash: metainfo.info_hash,
            client_id: client_id.clone(),
            tx,
            rx,
            length: metainfo.length,

            interval: None,
            min_interval: None,
            seeders: None,
            leechers: None,
            peers: Vec::new(),

            downloaded,
            uploaded,
            event: Event::None,
        }
    }

    pub async fn run(mut self) {
        self.announce().await;

        let mut sleep = Box::pin(tokio::time::sleep(Duration::from_secs(
            self.interval.unwrap_or(60)
                .max(self.min_interval.unwrap_or(0))
        )));
        loop {
            tokio::select!(
                _ = &mut sleep => {
                    self.announce().await;

                    sleep.as_mut().reset(
                        tokio::time::Instant::now()
                        + Duration::from_secs(
                            self.interval.unwrap_or(60)
                            .max(self.min_interval.unwrap_or(0))
                        )
                    );
                }

                progress = self.rx.recv() => match progress {
                    Some(progress) => {
                        self.downloaded = progress.downloaded;
                        self.uploaded = progress.uploaded;
                        if progress.event != self.event {
                            self.announce().await;
                            self.event = progress.event;
                        }
                    }

                    None => {
                        trace!("Quitting tracker loop");
                        return;
                    }
                }
            );
        }
    }

    fn reset(&mut self) {
        self.interval = None;
        self.min_interval = None;
        self.seeders = None;
        self.leechers = None;
    }

    async fn announce(&mut self) {
        let mut discovered = false;

        for t in 0..self.urls.len() {
            self.reset();
            
            let mut good = Vec::new();
            let mut bad = Vec::new();
            for u in 0..self.urls[t].len() {
                let url = self.urls[t][u].clone();
                match match url.split(":").next().unwrap() {
                    "http" | "https" => self.request_http(&url).await,
                    "udp" => self.request_udp(&url).await,
                    proto => Err(anyhow::anyhow!("Unsopported protocol {proto}")),
                } {
                    Ok(()) => {
                        debug!(tracker = %url, "Successfully announced");
                        good.push(url);
                        discovered = true;
                    }
                    Err(e) => {
                        debug!(tracker = %url, "Failed to announced {e}");
                        bad.push(url);
                    }
                }
            }
            good.extend(bad);
            self.urls[t] = good;

            if discovered {
                let _ = self.tx.send(torrent::Event::Discovery(self.peers.clone())).await;
                info!(tier = &t, "Successfully announced\n{self}");
                return;
            }
        }

        warn!("No tracker to announce to");
    }

    async fn request_udp(
        &mut self,
        url: &str,
    ) -> Result<()> {
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
        request[16..36].copy_from_slice(&self.info_hash);
        request[36..56].copy_from_slice(&self.client_id);
        request[56..64].copy_from_slice(
            &(self.downloaded as i64).to_be_bytes()
        );
        request[64..72].copy_from_slice(
            &(self.length as i64 - self.downloaded as i64).to_be_bytes()
        );
        request[72..80].copy_from_slice(
            &(self.uploaded as i64).to_be_bytes()
        );
        request[80..84].copy_from_slice(
            &(self.event.clone() as i32).to_be_bytes()
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

                        self.update(TrackerResponse::from_udp_response(&response[0..len]));
                        break;
                    }
                    Err(_) => {
                        trace!(tracker = %url, "Announce timed out");
                        continue;
                    }
                }
        }

        Ok(())
    }

    async fn request_http(
        &mut self,
        url: &str,
    ) -> Result<()> {
        let mut url = Url::parse(&url)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("port", "6881");
            query.append_pair("uploaded", &self.uploaded.to_string());
            query.append_pair("downloaded", &self.downloaded.to_string());
            query.append_pair("left", &(self.length - self.downloaded).to_string());
            if !matches!(self.event, Event::None) {
                query.append_pair("event", self.event.to_string());
            }
        }

        let client_id = self.client_id
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        let info_hash = self.info_hash
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>();

        let res = reqwest::get(
            format!("{url}&info_hash={}&peer_id={}", info_hash, client_id)
        ).await?;
        let body = res.bytes().await?;
        let response = TrackerResponse::from_bencode(&body)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        self.update(response);

        Ok(())
    }

    fn update(&mut self, response: TrackerResponse) {
        trace!("Got tracker response:\n{response}");
        
        if let Some(interval) = self.interval {
            self.interval = Some(response.interval.min(interval));
        } else {
            self.interval = Some(response.interval);
        }

        self.min_interval = response.min_interval.max(self.min_interval);
        self.seeders = response.seeders.max(self.seeders);
        self.leechers = response.leechers.max(self.leechers);

        let mut peers: HashSet<_> = self.peers.drain(..).collect();
        peers.extend(response.peers);
        self.peers = peers.into_iter().collect();
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

impl fmt::Display for Trackers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Interval: {}", self.interval.unwrap_or(60))?;
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
