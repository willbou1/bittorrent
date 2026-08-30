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
use url::Url;
use std::{
    collections::HashMap,
    path::{PathBuf, Path},
    time::Duration,
};

use crate::{
    bitfield::PieceBitfield,
    metainfo::{Metainfo, Metadata},
    types::{InfoHash, PeerId},
    peer::{Message, Peer},
    tracker::{Progress, Trackers},
    util::pretty_size,
    types::*,
};
use piece::Piece;
use download::Download;

pub enum Event {
    Message(PeerId, Message),
    Connection(Peer, PeerInfo),
    ConnectionFailure(PeerInfo, anyhow::Error),
    Disconnection(PeerId, anyhow::Error),
    Discovery(Vec<PeerInfo>),
}

pub struct Connection {
    info: PeerInfo,
    client: Option<String>,
}

pub struct Torrent {
    metainfo: Metainfo,
    connections: HashMap<PeerId, Connection>,
    rx: mpsc::Receiver<Event>,
    peer_tx: mpsc::Sender<Event>,
    tracker_tx: mpsc::Sender<Progress>,
    client_id: PeerId,
    span: Span,
}

impl Torrent {
    pub async fn from_magnet(url: &str, client_id: PeerId) -> Result<Self> {
        let url = Url::parse(url).unwrap();
        let mut pairs = url.query_pairs();
        let xt = pairs.find(|(n, _)| n == "xt").unwrap();
        let dn = pairs.find(|(n, _)| n == "dn").unwrap();
        let display_name = dn.1;

        let info_hash = InfoHash::from_xt(&xt.1).unwrap();
        let trackers: Vec<_> = pairs.filter(|(n, _)| n == "tr")
            .map(|(_, v)| v.into_owned()).collect();

        let span = tracing::info_span!(
            "torrent",
            name = %display_name,
        );
        let _enter = span.enter();
        warn!("{info_hash}");

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
        })
    }

    pub async fn from_torrent_file(
        path: &PathBuf,
        client_id: PeerId,
    ) -> Result<Self> {
        let file = fs::read(path).await?;
        let metainfo = Metainfo::from_bytes(&file)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        {}
        info!("Parsed metainfo:\n{}", metainfo);
        let span = tracing::info_span!(
            "torrent",
            name = %metainfo.name,
        );
        let _enter = span.enter();


        let (tx, rx) = mpsc::channel(100);

        let (tracker_tx, tracker_rx) = mpsc::channel(10);
        let trackers = Trackers::new(
            tx.clone(),
            tracker_rx,
            client_id,
            metainfo.info_hash,
            metainfo.announces,
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
        })
    }

    pub async fn run(&mut self, token: CancellationToken) -> Result<()> {
        let span = tracing::info_span!(
            "torrent",
            name = %self.metainfo.name,
        );


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
                        self.save_state().await?;
                        break;
                    }
                }
            }
            Ok(())
        }.instrument(span).await
    }


    fn statistics(&mut self, timeouts_this_second: &HashMap<PeerId, usize>) {
        let mut status = String::new();
        let mut downloaded_this_second = 0;

        for (peer_id, connection) in self.connections.iter_mut() {
            status.push_str(
                &format!("    {}: {} {} |{} ⇢ {} ⇣ {}/s ⏱ {:.2} ms {} to/s {} c/s {} \n",
                    connection.info,
                    if connection.choked {'C'} else {'U'},
                    if connection.interested {'I'} else {'-'},
                    connection.max_requests,
                    connection.sent_requests,
                    pretty_size(connection.downloaded_this_second),
                    connection.response_times_sum.as_secs_f64() * 1000.
                        / connection.num_response_times as f64,
                    timeouts_this_second.get(peer_id).unwrap_or(&0),
                    connection.chokes_this_second,
                    connection.client.as_ref().unwrap_or(&String::new())));

            if (connection.last_download_rate as f64) > connection.downloaded_this_second as f64 * 1.3f64 {
                // restrict pipeline
                connection.max_requests = (connection.max_requests - MAX_REQUESTS_STEP)
                    .max(MIN_MAX_REQUESTS);
            }
            else {
                // open pipeline
                connection.max_requests = (connection.max_requests + MAX_REQUESTS_STEP)
                    .min(MAX_MAX_REQUESTS);
            }

            connection.last_download_rate = connection.downloaded_this_second;
            downloaded_this_second += connection.downloaded_this_second;
            connection.downloaded_this_second = 0;
            connection.num_response_times = 0;
            connection.response_times_sum = Duration::default();
            connection.chokes_this_second = 0;
        }

        info!("{:.2}% ({}/{}) ♟ {} ⇣ {}/s ⏱ {} to/s \n{}",
            self.downloaded_pieces as f64 * 100. / self.pieces.len() as f64,
            self.downloaded_pieces, self.pieces.len(),
            self.connections.len(),
            pretty_size(downloaded_this_second),
            timeouts_this_second.iter().fold(0, |acc, (k, v)| acc + v),
            status);
    }

    async fn tick(&mut self) -> Result<()> {
        // tick download
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
                }
            }

            Message::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                debug!("Got extension handshake {extensions:?} {client:?} {max_requests:?}");
                connection.client = client;
            }

            Message::Pex { added, dropped } => {
                warn!("Got PEX {added:?} {dropped:?}");
                for info in added {
                    if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                        m.info.merge(info);
                    } else {
                        self.begin_connect(&info);
                    }
                }
            }

            Message::MetadataRequest { .. } => (),
            Message::Metadata { .. } => (),
            Message::MetadataReject { .. } => (),

            Message::KeepAlive => {}

            Event::Connection(peer, mut info) => {
                let (tx, rx) = mpsc::channel(100);
                tx.send(Message::Interested(true)).await?;
                info.id = Some(peer.id);
                debug!(peer = %info,"Connected with interest");
                if let Some((_, m)) = self.connections.iter_mut().find(|(_, c)| c.info.is_same_peer(&info)) {
                    m.info.merge(info.clone());
                } else {
                    self.connections.insert(
                        info.id.unwrap(),
                        Connection::new(tx, self.metainfo.num_pieces, &info),
                    );
                    tokio::spawn(peer.run(self.peer_tx.clone(), rx).instrument(self.span.clone()));
                }
            }

            Event::ConnectionFailure(peer_info, e) => {
                info!(peer = %peer_info, "Couldn't connect ({e})");
            }

            Event::Disconnection(peer_id, e) => {
                let connection = self.connections.remove(&peer_id).unwrap();
                info!(peer = %connection.info,"Disconnected {e}");
            }

            Event::Discovery(peer_infos) => {
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
        self.dispatch_requests().await
    }
}
