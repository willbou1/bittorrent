use rand::seq::SliceRandom;
use anyhow::Result;
use tracing::{info, trace, warn, debug};
use std::{
    fmt,
    time::{Duration},
};
use tokio::{
    sync::mpsc,
};

use crate::{
    torrent,
    proto::tracker::*,
    types::*,
};

pub struct Trackers {
    urls: Vec<Vec<String>>,
    info_hash: Hash,
    client_id: PeerId,
    tx: mpsc::Sender<torrent::Event>,
    rx: mpsc::Receiver<Progress>,

    pub interval: Option<u64>,
    pub min_interval: Option<u64>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub peers: Vec<PeerInfo>,

    progress: Progress,
}

impl Trackers {
    pub fn new(
        tx: mpsc::Sender<torrent::Event>,
        rx: mpsc::Receiver<Progress>,
        client_id: PeerId,
        info_hash: Hash,
        mut urls: Vec<Vec<String>>,
    ) -> Self {
        for tier_urls in urls.iter_mut() {
            tier_urls.shuffle(&mut rand::rng());
        }
        
        Self {
            urls,
            info_hash,
            client_id,
            tx,
            rx,

            interval: None,
            min_interval: None,
            seeders: None,
            leechers: None,
            peers: Vec::new(),

            progress: Progress::default(),
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
                    Some(progress) => self.progress = progress,
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
        const ANNOUNCE_TO_ALL_TIERS: bool = true;
        
        let mut discovered = false;

        for t in 0..self.urls.len() {
            if !ANNOUNCE_TO_ALL_TIERS {
                self.reset();
            }
            
            let mut good = Vec::new();
            let mut bad = Vec::new();
            for u in 0..self.urls[t].len() {
                let url = self.urls[t][u].clone();
                match match url.split(":").next().unwrap() {
                    "http" | "https" => request_http(
                        &url,
                        &self.client_id,
                        &self.info_hash,
                        &self.progress,
                    ).await,
                    "udp" => match tokio::time::timeout(
                        Duration::from_secs(5),
                        request_udp(
                            &url,
                            &self.client_id,
                            &self.info_hash,
                            &self.progress,
                        ),
                    ).await {
                        Ok(response) => response,
                        Err(_) => Err(anyhow::anyhow!("Exceeded tracker manager timeout for UDP")),
                    },
                    proto => Err(anyhow::anyhow!("Unsopported protocol {proto}")),
                } {
                    Ok(response) => {
                        self.update(response);
                        debug!(tracker = %url, "Successfully announced");
                        good.push(url);
                        discovered = true;
                    }
                    Err(e) => {
                        debug!(tracker = %url, "Failed to announced {e}");
                        bad.push(url);
                    }
                }
                let _ = self.tx.send(torrent::Event::Tracker(self.peers.clone())).await;
            }
            good.extend(bad);
            self.urls[t] = good;

            if discovered {
                debug!(tier = &t, "Successfully announced tier\n{self}");
                if !ANNOUNCE_TO_ALL_TIERS {
                    return;
                }
            }
        }
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

        for peer in response.peers {
            if let Some(m) = self.peers.iter_mut().find(|m| m.is_same_peer(&peer)) {
                m.merge(peer);
            } else {
                self.peers.push(peer);
            }
        }
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
