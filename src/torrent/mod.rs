mod piece;
mod transfer;
mod peer;
mod connection;

use anyhow::Result;
use tokio::{
    fs,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, debug, Instrument, Span};
use transfer::Transfer;
use url::Url;
use std::{
    collections::HashMap, path::{Path, PathBuf},
    time::{Duration},
};

use crate::{
    metainfo::{Metadata, Metainfo},
    proto::bit_torrent::{Message, BitTorrent},
    proto::metadata::MetadataMessage,
    proto::pex::PEXMessage,
    tracker::{Trackers},
    proto::tracker::Progress,
    types::*,
};
use piece::Piece;
use peer::{Peer, PeerState};

const MAX_DISCOVERY_ATTEMPTS: usize = 3;

#[derive(PartialEq, Eq)]
enum DiscoveryMechanism {
    Tracker,
    PEX,
    DHT,
}

struct DiscoveryAttempt {
    info: PeerInfo,
    num_attempts: usize,
    mechanism: DiscoveryMechanism,
}

impl DiscoveryAttempt {
    fn new(info: PeerInfo, mechanism: DiscoveryMechanism) -> Self {
        Self {
            info,
            mechanism,
            num_attempts: 1,
        }
    }
}

pub enum Event {
    Message(PeerId, Message),

    Connection(BitTorrent, PeerInfo, Option<PeerId>),
    ConnectionFailure(PeerInfo, anyhow::Error, Option<PeerId>),
    Disconnection(PeerId, anyhow::Error),

    Tracker(Vec<PeerInfo>),
}

pub struct Torrent {
    metainfo: Metainfo,

    peers: HashMap<PeerId, Peer>,
    rx: mpsc::Receiver<Event>,
    peer_tx: mpsc::Sender<Event>,
    tracker_tx: mpsc::Sender<Progress>,
    client_id: PeerId,
    span: Span,
    discovery_attemps: Vec<DiscoveryAttempt>,

    transfer: Option<Transfer>,

    metadata: Piece,
}

impl Torrent {
    pub async fn from_magnet(url: &str, client_id: PeerId) -> Result<Self> {
        let url = Url::parse(url).unwrap();
        let mut pairs = url.query_pairs();
        let xt = pairs.find(|(n, _)| n == "xt").unwrap();
        let dn = pairs.find(|(n, _)| n == "dn").unwrap();
        let display_name = dn.1.into_owned();

        let info_hash = Hash::from_xt(&xt.1).unwrap();
        let trackers: Vec<_> = pairs.filter(|(n, _)| n == "tr")
            .map(|(_, v)| v.into_owned()).collect();

        let span = tracing::info_span!(
            "torrent",
            name = %display_name,
        );
        let _enter = span.enter();

        let (tx, rx) = mpsc::channel(100);

        let (tracker_tx, tracker_rx) = mpsc::channel(10);
        let tracker_manager = Trackers::new(
            tx.clone(),
            tracker_rx,
            client_id,
            info_hash,
            vec![trackers.clone()],
        );
        tokio::spawn(tracker_manager.run().instrument(span.clone()));

        Ok(Self {
            peers: HashMap::new(),
            metainfo: Metainfo::from_magnet(info_hash, trackers),
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            transfer: None,
            metadata: Piece::new(None, 0, info_hash, true),
            discovery_attemps: Vec::new(),
        })
    }

    pub async fn from_torrent_file(
        path: &PathBuf,
        client_id: PeerId,
    ) -> Result<Self> {
        let file = fs::read(path).await?;
        let (metainfo, metadata_bytes) = Metainfo::from_bytes(&file)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        {}
        let metadata = Metadata::from_bytes(&metadata_bytes)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        info!("Parsed metainfo:\n{}", metainfo);
        info!("Parsed metadata:\n{}", metadata);
        let span = tracing::info_span!(
            "torrent",
            name = %metadata.name,
        );
        let _enter = span.enter();

        let (tx, rx) = mpsc::channel(100);

        let (tracker_tx, tracker_rx) = mpsc::channel(10);
        let trackers = Trackers::new(
            tx.clone(),
            tracker_rx,
            client_id,
            metainfo.info_hash,
            metainfo.announces.clone(),
        );
        tokio::spawn(trackers.run().instrument(span.clone()));

        Ok(Self {
            peers: HashMap::new(),
            peer_tx: tx,
            transfer: Some(Transfer::new(metadata, tracker_tx.clone()).await?),
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            metadata: Piece::new(None, metadata_bytes.len(), metainfo.info_hash, true),
            discovery_attemps: Vec::new(),
            metainfo,
        })
    }

    pub async fn run(&mut self, token: CancellationToken) -> Result<()> {
        let span = self.span.clone();
        async {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    event = self.rx.recv() => {
                        self.handle_event(event.unwrap()).await?;
                    }
                    _ = interval.tick() => {
                        self.tick().await?;
                    }
                    _ = token.cancelled() => {
                        if let Some(transfer) = &self.transfer {
                            transfer.save_state().await?;
                        }
                        break;
                    }
                }
            }
            Ok(())
        }.instrument(span).await
    }


    fn statistics(&mut self) {
        const VERBOSE_DISCOVERY: bool = false;
        
        let mut status = String::new();

        status.push_str("    Discovery attemps:\n        Tracker: ");
        let tracker_attemps = self.discovery_attemps.iter()
            .filter(|a| a.mechanism == DiscoveryMechanism::Tracker);
        if VERBOSE_DISCOVERY {
            for attempt in tracker_attemps {
                status.push_str(
                    &format!("{} ({}) ", attempt.info, attempt.num_attempts));
            }
        } else {
            status.push_str(&format!("{}", tracker_attemps.count()));
        }

        status.push_str("\n        PEX: ");
        let pex_attemps = self.discovery_attemps.iter()
            .filter(|a| a.mechanism == DiscoveryMechanism::PEX);
        if VERBOSE_DISCOVERY {
            for attempt in pex_attemps {
                status.push_str(
                    &format!("{} ({}) ", attempt.info, attempt.num_attempts));
            }
        } else {
            status.push_str(&format!("{}", pex_attemps.count()));
        }

        status.push_str("\n    Discovered\n");
        for (id, peer) in self.peers.iter_mut() {
            status.push_str(
                &format!("        {} | {} | {}{}{}{} | {}\n            {}\n",
                    match &peer.state {
                        PeerState::Connected { .. } => "⬤",
                        PeerState::Disconnected { reconnecting, .. } =>
                        if *reconnecting {"⟳"} else {"◯"},
                    },
                    id,
                    if peer.supports_fast {"F"} else {" "},
                    if peer.supports_pex {"P"} else {" "},
                    if peer.supports_metadataa {"M"} else {" "},
                    if peer.supports_dht {"D"} else {" "},
                    peer.client.as_ref().unwrap_or(&String::new()),
                    peer.info,
                ));
            if let PeerState::Disconnected { reason, .. } = &peer.state {
                status.push_str(&format!("            {reason}\n"));
            }
        }

        info!("✓ {} ⋯ {} ℹ {:2} {}/{}\n{}",
            self.peers.len(),
            self.discovery_attemps.len(),
            self.metadata.to_bitfield(),
            self.metadata.obtained_blocks(), self.metadata.num_blocks(),
            status);
    }

    async fn tick(&mut self) -> Result<()> {
        for (peer_id, count) in self.metadata.check_timeout() {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.decrement_metadata_requests(count);
            }
        }
        self.dispatch_metadata_request().await?;

        let mut reconnect = Vec::new();
        for peer in self.peers.values_mut() {
            if let PeerState::Disconnected {at, interval, reconnecting, ..} = &mut peer.state {
                if at.elapsed() > *interval && !*reconnecting {
                    *reconnecting = true;
                    reconnect.push(peer.info.clone());
                }
            }
        }
        for info in reconnect {
            self.try_connect(&info, info.id);
        }

        self.statistics();

        if let Some(transfer) = &mut self.transfer {
            transfer.tick().await?;
        }

        Ok(())
    }

    fn try_connect(&self, info: &PeerInfo, known_id: Option<PeerId>) {
        let tx = self.peer_tx.clone();
        let peer_info = info.clone();
        let info_hash = self.metainfo.info_hash.clone();
        let client_id = self.client_id.clone();
        tokio::spawn(async move {
            match BitTorrent::handshake(
                &peer_info,
                &info_hash,
                &client_id,
            ).await {
                Ok(bit_torrent) => tx.send(Event::Connection(bit_torrent, peer_info, known_id)).await,
                Err(e) => tx.send(Event::ConnectionFailure(peer_info, e, known_id)).await,
            }
        }.instrument(self.span.clone()));
    }

    fn discover(&mut self, info: &PeerInfo, mechanism: DiscoveryMechanism) {
        if self.peers.iter().all(|(_, c)| !c.info.is_same_peer(info)) {
            match self.discovery_attemps.iter_mut().find(|a| a.info.is_same_peer(&info)) {
                Some(attempt) => {
                    if attempt.num_attempts < MAX_DISCOVERY_ATTEMPTS {
                        attempt.num_attempts += 1;
                        self.try_connect(info, None);
                    } else {
                        self.discovery_attemps.retain(|a| &a.info != info);
                    }
                },
                None => {
                    self.discovery_attemps.push(DiscoveryAttempt::new(info.clone(), mechanism));
                    self.try_connect(info, None);
                },
            }
        }
    }

    async fn dispatch_metadata_request(&mut self) -> Result<()> {
        for (peer_id, peer) in &self.peers {
            if let PeerState::Connected { tx, .. } = &peer.state {
                while peer.can_request_metadata() {
                    if let Some(index) = self.metadata.find_available_block(&peer_id) {
                        self.metadata.download(index, peer_id.clone());
                        peer.send(Message::Metadata(
                            MetadataMessage::Request { index }
                        )).await;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_metadata_message(&mut self, message: MetadataMessage, peer_id: &PeerId) -> Result<()> {
        match message {
            MetadataMessage::Request { index } => {
                warn!("Got metadata request {index}");
            },
            MetadataMessage::Data { index, piece, total_size } => {
                if self.transfer.is_none() {
                    if let Some(metadata_bytes) = self.metadata.place(index, piece) {
                        let metadata = Metadata::from_bytes(&metadata_bytes)
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        info!("Got metadata:\n{metadata}");
                        self.transfer = Some(Transfer::new(metadata, self.tracker_tx.clone()).await?);
                        if let Some(transfer) = &mut self.transfer {
                            for (peer_id, peer) in self.peers.iter() {
                                if let PeerState::Connected { tx, .. } = &peer.state {
                                    transfer.add_connection(
                                        peer_id.clone(), tx.clone(), peer.supports_fast
                                    ).await?;
                                }
                            }
                        }
                    } else {
                        self.dispatch_metadata_request().await?;
                    }
                    debug!("Got metadata block {index}");
                }
            },
            MetadataMessage::Reject { index } => {
                if self.transfer.is_none() {
                    self.metadata.reject(index, peer_id.clone());
                    self.dispatch_metadata_request().await?;
                }
                debug!("Got metadata reject {index}");
            },
            _ => (),
        }
        Ok(())
    }
    
    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Message(peer_id, message) => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    let span = tracing::info_span!(
                        "connection",
                        info = %peer.info,
                    );
                    let _enter = span.enter();
                    // dispatch to transfer layer
                    match message {
                        Message::Metadata(msg) => self.handle_metadata_message(msg, &peer_id).await?,

                        Message::KeepAlive => {}

                        Message::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                            debug!("Got extension handshake {extensions:?} {client:?} {max_requests:?}");
                            peer.client = client;
                            peer.supports_metadataa |= extensions.contains_key("ut_metadata");
                            peer.supports_pex |= extensions.contains_key("ut_pex");
                            if let Some(metadata_size) = metadata_size && self.transfer.is_none() {
                                self.metadata.set_length_and_reset(metadata_size);
                                self.dispatch_metadata_request().await?;
                            }
                        }

                        Message::PEX(PEXMessage { added, dropped }) => {
                            debug!("Got PEX {added:?} {dropped:?}");
                            for info in added {
                                self.discover(&info, DiscoveryMechanism::PEX);
                            }
                        }

                        Message::DHTPort { port } => {
                            debug!("DHT port {port}");
                        }

                        Message::Unsupported { type_byte } => 
                            warn!("Got unsupported message {type_byte}"),
                        Message::UnsupportedExtension { type_byte } =>
                            warn!("Got unsupported extension message {type_byte}"),

                        msg @ _ => {
                            if let Some(transfer) = &mut self.transfer {
                                transfer.handle_event(&peer_id, msg).await?;
                            }
                        }
                    }
                }
            }

            Event::Connection(bit_torrent, mut info, known_id) => {
                let (tx, rx) = mpsc::channel(100);

                if let Some(id) = known_id {
                    debug!(id = %id,"Reconnected");
                    self.peers.get_mut(&id).unwrap().state.reconnect(tx.clone());
                } else {
                    self.discovery_attemps.retain(|a| a.info != info);
                    info.id = Some(bit_torrent.id);
                    debug!(peer = %info,"Discovered");
                    if let Some((_, m)) = self.peers.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                        m.info.merge(info.clone());
                    } else {
                        self.peers.insert(
                            info.id.unwrap(),
                            Peer::new(info.clone(), tx.clone(), bit_torrent.supports_fast, bit_torrent.supports_dht),
                        );
                    }
                }

                if let Some(transfer) = &mut self.transfer {
                    transfer.add_connection(info.id.unwrap(), tx, bit_torrent.supports_fast).await?;
                }
                tokio::spawn(
                    bit_torrent.run(self.peer_tx.clone(), rx).instrument(self.span.clone())
                );
            }

            Event::ConnectionFailure(peer_info, e, known_id) => {
                self.discover(&peer_info, DiscoveryMechanism::Tracker);
                if let Some(id) = known_id {
                    debug!(peer = %peer_info, "Couldn't reconnect ({e})");
                    self.peers.get_mut(&id).unwrap().state.back_off();
                } else {
                    debug!(peer = %peer_info, "Couldn't discover ({e})");
                }
            }

            Event::Disconnection(peer_id, error) => {
                if let Some(transfer) = &mut self.transfer {
                    transfer.sever_connection(&peer_id);
                }
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.state.disconnect(error);
            }

            Event::Tracker(peer_infos) => {
                debug!("Tracker discovery");
                for info in peer_infos {
                    self.discover(&info, DiscoveryMechanism::Tracker);
                }
            }
        }

        Ok(())
    }
}
