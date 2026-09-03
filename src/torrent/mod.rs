pub mod piece;
pub mod transfer;

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
    collections::HashMap, path::{Path, PathBuf}, time::Duration
};

use crate::{
    bencode::BencodeValue,
    bitfield::PieceBitfield,
    metainfo::{Metadata, Metainfo},
    proto::bit_torrent::{Message, BitTorrent},
    proto::metadata::MetadataMessage,
    proto::pex::PEXMessage,
    tracker::{Progress, Trackers},
    types::*,
};
use piece::Piece;

const MAX_METADATA_REQUESTS: usize = 2;
const BLOCK_SIZE: usize = 16 * 1024;
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

    Connection(BitTorrent, PeerInfo),
    ConnectionFailure(PeerInfo, anyhow::Error),
    Disconnection(PeerId, anyhow::Error),

    Tracker(Vec<PeerInfo>),
}

pub struct Connection {
    info: PeerInfo,
    tx: mpsc::Sender<Message>,

    connected: bool,

    client: Option<String>,
    supports_fast: bool,
    supports_pex: bool,
    supports_metadataa: bool,
    supports_dht: bool,

    metadata_requests: usize,
}

impl Connection {
    fn new(info: PeerInfo, tx: mpsc::Sender<Message>, supports_fast: bool, supports_dht: bool) -> Self {
        Self {
            tx,
            client: None,
            info,
            supports_fast,
            supports_dht,
            supports_metadataa: false,
            supports_pex: false,
            connected: true,
            metadata_requests: 0,
        }
    }

    fn can_request_metadata(&self) -> bool {
        self.metadata_requests < MAX_METADATA_REQUESTS
    }
}

pub struct Torrent {
    metainfo: Metainfo,
    display_name: String,

    connections: HashMap<PeerId, Connection>,
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

        let info_hash = InfoHash::from_xt(&xt.1).unwrap();
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
            connections: HashMap::new(),
            metainfo: Metainfo::from_magnet(info_hash, trackers),
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            transfer: None,
            display_name,
            metadata: Piece::new(None, 0, info_hash.as_bytes().try_into()?, true),
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
            connections: HashMap::new(),
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            display_name: metadata.name.clone(),
            transfer: Some(Transfer::new(metadata).await?),
            metadata: Piece::new(None, metadata_bytes.len(), metainfo.info_hash.as_bytes().try_into()?, true),
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
                        if let Err(e) = self.handle_event(event.unwrap()).await {
                            warn!("Event handling error for: {e}");
                        }
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
        let mut status = String::new();

        status.push_str("    Discovery attemps:\n        Tracker: ");
        for attempt in self.discovery_attemps.iter()
            .filter(|a| a.mechanism == DiscoveryMechanism::Tracker) {
            status.push_str(
                &format!("{} ({}) ", attempt.info, attempt.num_attempts));
        }

        status.push_str("\n        PEX: ");
        for attempt in self.discovery_attemps.iter()
            .filter(|a| a.mechanism == DiscoveryMechanism::PEX) {
            status.push_str(
                &format!("{} ({}) ", attempt.info, attempt.num_attempts));
        }

        status.push_str("\n    Discovered\n");
        for (_, connection) in self.connections.iter_mut() {
            status.push_str(
                &format!("        {} | {} | {} {} {} {} | {}\n            {}\n",
                    if connection.connected {"C"} else {"D"},
                    connection.info.id.unwrap(),
                    if connection.supports_fast {"FAST"} else {"    "},
                    if connection.supports_pex {"PEX"} else {"   "},
                    if connection.supports_metadataa {"META"} else {"    "},
                    if connection.supports_dht {"DHT"} else {"   "},
                    connection.client.as_ref().unwrap_or(&String::new()),
                    connection.info,
                ));
        }

        info!("✓ {} ⋯ {} ℹ {}/{}\n{}",
            self.connections.len(),
            self.discovery_attemps.len(),
            self.metadata.obtained_blocks(), self.metadata.num_blocks(),
            status);
    }

    async fn tick(&mut self) -> Result<()> {
        for (peer_id, count) in self.metadata.check_timeout() {
            if let Some(con) = self.connections.get_mut(&peer_id) {
                con.metadata_requests = con.metadata_requests.saturating_sub(count);
            }
        }
        self.dispatch_metadata_request().await?;
        
        self.statistics();

        if let Some(transfer) = &mut self.transfer {
            transfer.tick().await?;
        }

        Ok(())
    }

    fn try_connect(&mut self, info: &PeerInfo) {
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
                Ok(peer) => tx.send(Event::Connection(peer, peer_info)).await,
                Err(e) => tx.send(Event::ConnectionFailure(peer_info, e)).await,
            }
        }.instrument(self.span.clone()));
    }

    fn discover(&mut self, info: &PeerInfo, mechanism: DiscoveryMechanism) {
        if self.connections.iter().all(|(_, c)| !c.info.is_same_peer(info)) {
            match self.discovery_attemps.iter_mut().find(|a| a.info.is_same_peer(&info)) {
                Some(attempt) => {
                    if attempt.num_attempts < MAX_DISCOVERY_ATTEMPTS {
                        attempt.num_attempts += 1;
                        self.try_connect(info);
                    } else {
                        self.discovery_attemps.retain(|a| &a.info != info);
                    }
                },
                None => {
                    self.discovery_attemps.push(DiscoveryAttempt::new(info.clone(), mechanism));
                    self.try_connect(info);
                },
            }
        }
    }

    async fn dispatch_metadata_request(&mut self) -> Result<()> {
        for (peer_id, con) in &self.connections {
            while con.can_request_metadata() {
                if let Some(index) = self.metadata.find_available_block(&peer_id) {
                    self.metadata.download(index, peer_id.clone());
                    con.tx .send(Message::Metadata(
                        MetadataMessage::Request { index }
                    )).await?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_metadata_message(&mut self, message: MetadataMessage, peer_id: &PeerId) -> Result<()> {
        match message {
            MetadataMessage::Request { .. } => (),
            MetadataMessage::Data { index, piece, total_size } => {
                if self.transfer.is_none() {
                    if let Some(metadata_bytes) = self.metadata.place(index, piece) {
                        let metadata = Metadata::from_bytes(&metadata_bytes)
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                            info!("Got metadata:\n{metadata}");
                            self.transfer = Some(Transfer::new(metadata).await?);
                            for (peer_id, con) in self.connections.iter()
                                .filter(|(k, v)| v.connected) {
                                    if let Some(transfer) = &mut self.transfer {
                                        transfer.add_connection(
                                            peer_id.clone(), con.tx.clone(), con.supports_fast
                                        ).await?;
                                    }
                                }
                    } else {
                        self.dispatch_metadata_request().await?;
                    }
                    info!("Got metadata block {index}");
                }
            },
            MetadataMessage::Reject { index } => {
                if self.transfer.is_none() {
                    self.metadata.reject(index, peer_id.clone());
                    self.dispatch_metadata_request().await?;
                }
                info!("Got metadata reject {index}");
            },
            _ => (),
        }
        Ok(())
    }
    
    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Message(peer_id, message) => {
                if let Some(connection) = self.connections.get_mut(&peer_id) {
                    let span = tracing::info_span!(
                        "connection",
                        info = %connection.info,
                    );
                    let _enter = span.enter();
                    // dispatch to transfer layer
                    match message {
                        Message::Metadata(msg) => self.handle_metadata_message(msg, &peer_id).await?,

                        Message::KeepAlive => {}

                        Message::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                            debug!("Got extension handshake {extensions:?} {client:?} {max_requests:?}");
                            connection.client = client;
                            connection.supports_metadataa |= extensions.contains_key("ut_metadata");
                            connection.supports_pex |= extensions.contains_key("ut_pex");
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

                        msg @ _ => {
                            if let Some(transfer) = &mut self.transfer {
                                transfer.handle_event(&peer_id, msg).await?;
                            }
                        }
                    }
                }
            }

            Event::Connection(peer, mut info) => {
                self.discovery_attemps.retain(|a| a.info != info);
                let (tx, rx) = mpsc::channel(100);
                info.id = Some(peer.id);
                debug!(peer = %info,"Connected with interest");
                if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                    m.info.merge(info.clone());
                } else {
                    self.connections.insert(
                        info.id.unwrap(),
                        Connection::new(info.clone(), tx.clone(), peer.supports_fast, peer.supports_dht),
                    );
                    if let Some(transfer) = &mut self.transfer {
                        transfer.add_connection(info.id.unwrap(), tx, peer.supports_fast).await?;
                    }
                    tokio::spawn(
                        peer.run(self.peer_tx.clone(), rx).instrument(self.span.clone())
                    );
                }
            }

            Event::ConnectionFailure(peer_info, e) => {
                self.discover(&peer_info, DiscoveryMechanism::Tracker);
                debug!(peer = %peer_info, "Couldn't connect ({e})");
            }

            Event::Disconnection(peer_id, e) => {
                if let Some(transfer) = &mut self.transfer {
                    transfer.sever_connection(&peer_id);
                }
                let connection = self.connections.get_mut(&peer_id).unwrap();
                connection.connected = false;

                // TODO keep an eye on this
                let boring = e .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| matches!(e.kind(),
                        std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                    ));

                if !boring {
                    warn!(peer = %connection.info,"Disconnected {e}");
                }
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
