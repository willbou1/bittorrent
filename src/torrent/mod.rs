mod piece;
mod transfer;

use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, debug, Instrument, Span};
use transfer::Transfer;
use url::Url;
use std::{
    collections::HashMap,
    path::{PathBuf, Path},
    time::Duration,
};

use crate::{
    bencode::BencodeValue, bitfield::PieceBitfield, metainfo::{Metadata, Metainfo}, peer::{Message, Peer}, tracker::{Progress, Trackers}, types::{InfoHash, PeerId, *}
};

const BLOCK_SIZE: usize = 16 * 1024;

pub enum Event {
    Message(PeerId, Message),

    Connection(Peer, PeerInfo),
    ConnectionFailure(PeerInfo, anyhow::Error),
    Disconnection(PeerId, anyhow::Error),

    Tracker(Vec<PeerInfo>),
}

pub struct Connection {
    info: PeerInfo,
    tx: mpsc::Sender<Message>,

    connected: bool,

    client: Option<String>,
    supports_pex: bool,
    supports_metadataa: bool,
}

impl Connection {
    fn new(info: PeerInfo, tx: mpsc::Sender<Message>) -> Self {
        Self {
            tx,
            client: None,
            info,
            supports_metadataa: false,
            supports_pex: false,
            connected: true,
        }
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

    transfer: Option<Transfer>,

    metadata_bitfield: Option<PieceBitfield>,
    metadata: Vec<u8>,
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
            metadata_bitfield: None,
            metadata: Vec::new(),
        })
    }

    pub async fn from_torrent_file(
        path: &PathBuf,
        client_id: PeerId,
    ) -> Result<Self> {
        let file = fs::read(path).await?;
        let (metainfo, metadata) = Metainfo::from_bytes(&file)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        {}
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
            metainfo,
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id,
            display_name: metadata.name.clone(),
            transfer: Some(Transfer::new(metadata).await?),
            metadata_bitfield: None,
            metadata: Vec::new(),
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
        let mut status = String::new();
        let mut downloaded_this_second = 0;

        for (peer_id, connection) in self.connections.iter_mut() {
            status.push_str(
                &format!("    {} {} {} {}: {} \n",
                    if connection.connected {"C"} else {"D"},
                    if connection.supports_pex {"P"} else {"-"},
                    if connection.supports_pex {"M"} else {"-"},
                    connection.info,
                    connection.client.as_ref().unwrap_or(&String::new())));
        }

        info!("♟ {} \n{}",
            self.connections.len(),
            status);
    }

    async fn tick(&mut self) -> Result<()> {
        if self.transfer.is_none() && let Some(bitfield) = &mut self.metadata_bitfield {
            for (_, connection) in self.connections.iter_mut() {
                if connection.supports_metadataa {
                    for b in 0..bitfield.len() {
                        connection.tx.send(Message::MetadataRequest {
                            index: b
                        }).await?;
                    }
                    break;
                }
            }
        }
        
        if let Some(transfer) = &mut self.transfer {
            transfer.tick().await?;
        }

        self.statistics();
        
        Ok(())
    }

    fn begin_connect(&self, info: &PeerInfo) {
        let tx = self.peer_tx.clone();
        let peer_info = info.clone();
        let info_hash = self.metainfo.info_hash.clone();
        let client_id = self.client_id.clone();
        tokio::spawn(async move {
            match Peer::handshake(
                &peer_info,
                &info_hash,
                &client_id,
            ).await {
                Ok(peer) => tx.send(Event::Connection(peer, peer_info)).await,
                Err(e) => tx.send(Event::ConnectionFailure(peer_info, e)).await,
            }
        }.instrument(self.span.clone()));
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
                        Message::MetadataRequest { .. } => (),
                        Message::Metadata { index, piece, total_size } => {
                            if let Some(bitfield) = &mut self.metadata_bitfield {
                                self.metadata.resize(total_size, 0);
                                bitfield.set_piece(index);
                                let begin = index * BLOCK_SIZE;
                                self.metadata[begin..(begin + BLOCK_SIZE.min(piece.len()))].copy_from_slice(&piece);
                                let mut complete = true;
                                for b in 0..bitfield.len() {
                                    complete &= bitfield.has_piece(b);
                                }
                                if complete {
                                    let metadata = Metadata::from_bencode(
                                        &BencodeValue::from_bytes(&self.metadata).unwrap().0.unwrap()
                                    ).unwrap();
                                    warn!("{metadata}");
                                }
                            }
                        },
                        Message::MetadataReject { .. } => (),
                        Message::KeepAlive => {}

                        Message::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                            debug!("Got extension handshake {extensions:?} {client:?} {max_requests:?}");
                            connection.client = client;
                            connection.supports_metadataa |= extensions.contains_key("ut_metadata");
                            connection.supports_pex |= extensions.contains_key("ut_pex");
                            if let Some(metadata_size) = metadata_size && self.metadata_bitfield.is_none() {
                                self.metadata_bitfield = Some(
                                    PieceBitfield::new(metadata_size.div_ceil(BLOCK_SIZE))
                                );
                            }
                        }

                        Message::Pex { added, dropped } => {
                            info!("Got PEX {added:?} {dropped:?}");
                            for info in added {
                                if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                                    m.info.merge(info);
                                } else {
                                    self.begin_connect(&info);
                                }
                            }
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
                let (tx, rx) = mpsc::channel(100);
                info.id = Some(peer.id);
                debug!(peer = %info,"Connected with interest");
                if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                    m.info.merge(info.clone());
                } else {
                    self.connections.insert(
                        info.id.unwrap(),
                        Connection::new(info.clone(), tx.clone()),
                    );
                    tokio::spawn(
                        peer.run(self.peer_tx.clone(), rx).instrument(self.span.clone())
                    );
                    if let Some(transfer) = &mut self.transfer {
                        transfer.add_connection(info.id.unwrap(), tx).await?;
                    }
                }
            }

            Event::ConnectionFailure(peer_info, e) => {
                info!(peer = %peer_info, "Couldn't connect ({e})");
            }

            Event::Disconnection(peer_id, e) => {
                if let Some(transfer) = &mut self.transfer {
                    transfer.sever_connection(&peer_id);
                }
                let connection = self.connections.get_mut(&peer_id).unwrap();
                connection.connected = false;
                info!(peer = %connection.info,"Disconnected {e}");
            }


            Event::Tracker(peer_infos) => {
                info!("Discovered peers");
                for info in peer_infos {
                    if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                        m.info.merge(info);
                    } else {
                       self.begin_connect(&info);
                    }
                }
            }
        }

        Ok(())
    }
}
