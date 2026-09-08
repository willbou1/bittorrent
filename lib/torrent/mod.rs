mod piece;
mod peer;
mod transfer;
pub mod control;

use anyhow::Result;
use tokio::{
    fs,
    sync::mpsc,
    sync::watch,
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
    bitfield::Bitfield, metainfo::{Metadata, Metainfo}, proto::{bit_torrent::{BitTorrent, Message}, metadata::MetadataMessage, pex::PEXMessage, tracker}, tracker::Trackers, types::*
};
use piece::Piece;
use peer::{Peer, PeerState};
use control::*;

const METADATA_BLOCK_SIZE: usize = 16 * 1024;

const MAX_DISCOVERY_ATTEMPTS: usize = 3;

const PEER_CHANNEL_CAPACITY: usize = 100;
const EVENT_CHANNEL_CAPACITY: usize = 400;

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

pub(crate) enum Event {
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
    tracker_tx: watch::Sender<tracker::Progress>,
    client_id: PeerId,
    span: Span,
    discovery_attemps: Vec<DiscoveryAttempt>,

    transfer: Option<Transfer>,

    metadata: Piece,

    pub command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    pub progress_rx: watch::Receiver<Progress>,
    progress_tx: watch::Sender<Progress>,
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
            display_name = %display_name,
        );
        let _enter = span.enter();

        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        let (tracker_tx, tracker_rx) = watch::channel(tracker::Progress {
            downloaded: 0,
            uploaded: 0,
            left: 0,
            event: tracker::Event::None,
        });
        let tracker_manager = Trackers::new(
            tx.clone(),
            tracker_rx,
            client_id,
            info_hash,
            vec![trackers.clone()],
        );

        tokio::task::Builder::new()
            .name("trackers")
            .spawn(tracker_manager.run().instrument(span.clone()))
            .unwrap();

        let (progress_tx, progress_rx) = watch::channel(Progress {
            display_name,
            num_peers: 0,
            num_connected_peers: 0,
            num_discovery_attempts: 0,
            transfer: None,
            metadata_down_speed: 0,
            metadata_up_speed: 0,
            metadata_bitfield: None,
        });
        let (command_tx, command_rx) = mpsc::channel(10);

        Ok(Self {
            peers: HashMap::new(),
            metainfo: Metainfo::from_magnet(info_hash, trackers),
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            transfer: None,
            metadata: Piece::new(None, METADATA_BLOCK_SIZE, 0, info_hash, false),
            discovery_attemps: Vec::new(),

            progress_rx,
            progress_tx,
            command_rx,
            command_tx
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
            display_name = %metadata.name,
        );
        let _enter = span.enter();

        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        let (tracker_tx, tracker_rx) = watch::channel(tracker::Progress {
            downloaded: 0,
            uploaded: 0,
            left: 0,
            event: tracker::Event::None,
        });
        let trackers = Trackers::new(
            tx.clone(),
            tracker_rx,
            client_id,
            metainfo.info_hash,
            metainfo.announces.clone(),
        );
        tokio::task::Builder::new()
            .name("trackers")
            .spawn(trackers.run().instrument(span.clone()))
            .unwrap();

        let (progress_tx, progress_rx) = watch::channel(Progress {
            display_name: metadata.name.clone(),
            num_peers: 0,
            num_connected_peers: 0,
            num_discovery_attempts: 0,
            transfer: None,
            metadata_bitfield: None,
            metadata_down_speed: 0,
            metadata_up_speed: 0,
        });
        let (command_tx, command_rx) = mpsc::channel(10);

        Ok(Self {
            peers: HashMap::new(),
            peer_tx: tx,
            transfer: Some(Transfer::new(metadata, tracker_tx.clone(), progress_tx.clone()).await?),
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            metadata: Piece::from_slice(METADATA_BLOCK_SIZE, metainfo.info_hash, &metadata_bytes),
            discovery_attemps: Vec::new(),
            metainfo,

            progress_rx,
            progress_tx,
            command_rx,
            command_tx
        })
    }

    pub async fn run(&mut self,) -> Result<()> {
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
                    command = self.command_rx.recv() => {
                        match command {
                            Some(Command::Stop) | None => {
                                if let Some(transfer) = &mut self.transfer {
                                    transfer.save_state().await?;
                                }
                                break;
                            }
                            Some(command) => self.handle_command(command),
                        }
                    }
                }
            }
            Ok(())
        }.instrument(span).await
    }

    fn handle_command(&mut self, command: Command) {
        
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

        let mut uploaded_metadata_this_second = 0;
        let mut downloaded_metadata_this_second = 0;
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
                    if peer.supports_metadata {"M"} else {" "},
                    if peer.supports_dht {"D"} else {" "},
                    peer.client.as_ref().unwrap_or(&String::new()),
                    peer.info,
                ));
            if let PeerState::Disconnected { reason, .. } = &peer.state {
                status.push_str(&format!("            {reason}\n"));
            }
            uploaded_metadata_this_second += peer.uploaded_metadata_this_second();
            downloaded_metadata_this_second += peer.downloaded_metadata_this_second();
            peer.reset_statistics();
        }

        self.progress_tx.send_modify(|p| {
            p.metadata_bitfield = Some(self.metadata.to_bitfield());
            p.metadata_down_speed = METADATA_BLOCK_SIZE * downloaded_metadata_this_second;
            p.metadata_up_speed = METADATA_BLOCK_SIZE * uploaded_metadata_this_second;
        });

        info!("\n{}",
            status);
    }

    async fn tick(&mut self) -> Result<()> {
        if self.transfer.is_none() {
            for (peer_id, count) in self.metadata.check_timeout() {
                self.peers.entry(peer_id)
                    .and_modify(|p| p.timeout_metadata(count));
            }
            self.dispatch_metadata_request().await?;
        }

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
        tokio::task::Builder::new()
            .name(if known_id.is_some() {
                "Reconnection"
            } else {
                "Discovery"
            })
            .spawn(async move {
                match BitTorrent::handshake(
                    &peer_info,
                    &info_hash,
                    &client_id,
                ).await {
                    Ok(bit_torrent) => tx.send(Event::Connection(bit_torrent, peer_info, known_id)).await,
                    Err(e) => tx.send(Event::ConnectionFailure(peer_info, e, known_id)).await,
                }
            }.instrument(self.span.clone()))
            .unwrap();
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
        for (peer_id, peer) in &mut self.peers {
            while peer.can_request_metadata() {
                if let Some(index) = self.metadata.find_available_block(&peer_id) {
                    let span = tracing::info_span!(
                        "peer",
                        id = %peer_id,
                    );
                    let _enter = span.enter();
                    debug!("Requested metadata {index}");
                    self.metadata.download(index, peer_id.clone());
                    peer.request_metadata(index).await;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_metadata_message(&mut self, message: MetadataMessage, peer_id: &PeerId) -> Result<()> {
        match message {
            MetadataMessage::Request { index } => {
                debug!("Got metadata request {index}");
                if let Some(peer) = self.peers.get_mut(peer_id) && self.metadata.num_blocks() != 0 {
                    if let Some(piece) = self.metadata.get(index) {
                        debug!("Sent metadata {index}");
                        peer.send_metadata(index, self.metadata.num_blocks(), piece.to_vec()).await;
                    } else {
                        debug!("Rejected metadata {index}");
                        peer.send_metadata_reject(index).await;
                    }
                }
            },
            MetadataMessage::Data { index, piece, total_size } => {
                // TODO fix races here like for normal scheduling
                if self.transfer.is_none() && !self.metadata.has_obtrined(index) {
                    debug!("Got metadata block {index}");
                    let who_downloading = self.metadata.who_downloading(index);
                    self.peers.entry(*peer_id).and_modify(
                        |p| p.receive_metadata(who_downloading.map(|w| w == peer_id).unwrap_or(false))
                    );
                    if let Some(who) = who_downloading && who != peer_id {
                        self.peers.get_mut(who).map(|p| p.sub_sent_metadata_requests(1));
                    }

                    if let Some(metadata_bytes) = self.metadata.place(index, piece, false) {
                        let metadata = Metadata::from_bytes(&metadata_bytes)
                            .map_err(|e| anyhow::anyhow!("Error parsing metadata: {e}"))?;
                        info!("Got metadata:\n{metadata}");
                        let mut transfer = Transfer::new(
                            metadata,
                            self.tracker_tx.clone(),
                            self.progress_tx.clone(),
                        ).await?;
                        for (peer_id, peer) in self.peers.iter_mut() {
                            if let PeerState::Connected { tx, initial_transfer_messages, .. } = &mut peer.state {
                                transfer.add_connection(
                                    peer_id.clone(),
                                    tx.clone(),
                                    peer.supports_fast,
                                    Some(initial_transfer_messages),
                                ).await?;
                            }
                        }
                        self.transfer = Some(transfer);
                    } else {
                        self.dispatch_metadata_request().await?;
                    }
                }
            },
            MetadataMessage::Reject { index } => {
                if self.transfer.is_none() {
                    let who_downloading = self.metadata.who_downloading(index);
                    if let Some(who) = who_downloading && who == peer_id {
                        self.metadata.reject(index, peer_id.clone());
                        self.peers.entry(*peer_id)
                            .and_modify(|p| p.reject_metadata());
                        self.dispatch_metadata_request().await?;
                    }
                }
                debug!("Got metadata reject {index}");
            },
            _ => (),
        }
        Ok(())
    }

    fn update_progress(&self) {
        self.progress_tx.send_modify(|p| {
            p.num_peers = self.peers.len();
            p.num_connected_peers = self.peers.iter()
                .filter(|p| p.1.state.is_connected()).count();
            p.num_discovery_attempts = self.discovery_attemps.len();
        });
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Message(peer_id, message) => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    let span = tracing::info_span!(
                        "peer",
                        id = %peer_id,
                    );
                    let guard = span.enter();
                    // dispatch to transfer layer
                    match message {
                        Message::Metadata(msg) => self.handle_metadata_message(msg, &peer_id).await?,

                        Message::KeepAlive => {}

                        Message::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                            debug!("Got extension handshake {extensions:?} {client:?} {max_requests:?}");
                            peer.receive_extension_handshake(client, extensions);
                            if let Some(metadata_size) = metadata_size && self.transfer.is_none() && metadata_size / (16 * 1024) > self.metadata.num_blocks() {
                                warn!("Resetting metadata");
                                self.metadata.set_length_and_reset(metadata_size);
                                drop(guard);
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
                            } else {
                                self.peers.get_mut(&peer_id)
                                    .map(|p| p.queue_transfer_message(msg));
                            }
                        }
                    }
                }
            }

            Event::Connection(bit_torrent, mut info, known_id) => {
                let (tx, rx) = mpsc::channel(PEER_CHANNEL_CAPACITY);

                if let Some(id) = known_id {
                    debug!(id = %id,"Reconnected");
                    self.peers.entry(id)
                        .and_modify(|p| p.state.reconnect(tx.clone()));
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
                    transfer.add_connection(
                        info.id.unwrap(),
                        tx,
                        bit_torrent.supports_fast,
                        None,
                    ).await?;
                }
                tokio::task::Builder::new()
                    .name("BitTorrent")
                    .spawn(
                        bit_torrent.run(self.peer_tx.clone(), rx).instrument(self.span.clone())
                    )
                    .unwrap();
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
                self.peers.entry(peer_id)
                    .and_modify(|p| p.state.disconnect(error));
            }

            Event::Tracker(peer_infos) => {
                debug!("Tracker discovery");
                for info in peer_infos {
                    self.discover(&info, DiscoveryMechanism::Tracker);
                }
            }
        }

        self.update_progress();

        Ok(())
    }
}
