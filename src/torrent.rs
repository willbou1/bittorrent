use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, debug, Instrument, Span};
use std::{
    collections::HashMap,
    path::{PathBuf, Path},
    sync::mpsc::RecvTimeoutError::Disconnected,
    time::Duration
};

use crate::{
    bitfield::PieceBitfield,
    metainfo::Metainfo,
    types::{InfoHash, PeerId},
    peer::{Message::{self, Bitfield}, Peer},
    piece::Piece,
    timer::Timer,
    tracker::{Progress, Trackers},
    util::pretty_size,
    types::*,
};

const BLOCK_SIZE: usize = 16 * 1024;

const DEFAULT_MAX_REQUESTS: usize = 20;
const MIN_MAX_REQUESTS: usize = 5;
const MAX_MAX_REQUESTS: usize = 70;
const MAX_REQUESTS_STEP: usize = 2;

pub enum Event {
    Message(PeerId, Message),
    Connection(Peer, PeerInfo),
    ConnectionFailure(PeerInfo, anyhow::Error),
    Disconnection(PeerId, anyhow::Error),
    Discovery(Vec<PeerInfo>),
}

pub struct Connection {
    info: PeerInfo,
    choked: bool,
    interested: bool,
    piece_bitfield: PieceBitfield,
    piece_cursor: Option<usize>,
    sent_requests: usize,
    tx: mpsc::Sender<Message>,
    max_requests: usize,

    client: Option<String>,
    downloaded_this_second: usize,
    last_download_rate: usize,
    response_times_sum: Duration,
    num_response_times: usize,
    chokes_this_second: usize,
}

impl Connection {
    fn new(tx: mpsc::Sender<Message>, num_pieces: usize, info: &PeerInfo) -> Self {
        Self {
            sent_requests: 0,
            piece_cursor: None,
            choked: true,
            interested: false,
            piece_bitfield: PieceBitfield::new(num_pieces),
            tx,
            info: info.clone(),
            max_requests: DEFAULT_MAX_REQUESTS,

            client: None,
            downloaded_this_second: 0,
            last_download_rate: 0,
            num_response_times: 0,
            response_times_sum: Duration::default(),
            chokes_this_second: 0,
        }
    }

    fn can_request(&self) -> bool {
        self.sent_requests < self.max_requests && !self.choked
    }
}

pub struct Torrent {
    metainfo: Metainfo,
    connections: HashMap<PeerId, Connection>,
    pieces: Vec<Piece>,
    rx: mpsc::Receiver<Event>,
    peer_tx: mpsc::Sender<Event>,
    tracker_tx: mpsc::Sender<Progress>,
    client_id: PeerId,
    span: Span,

    downloaded_pieces: usize,
}

impl Torrent {
    pub async fn from_torrent_file(
        path: &PathBuf,
        client_id: &PeerId,
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

        let mut pieces = vec![];
        let path = Path::new("torrents").join(format!("{}.state", metainfo.name));
        let mut downloaded_pieces = 0;
        if fs::try_exists(&path).await? {
            info!("Recovering");
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path).await?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;
            let bitfield = PieceBitfield::from_vec(bytes);
            for p in 0..metainfo.num_pieces {
                let written = bitfield.has_piece(p);
                if written {
                    downloaded_pieces += 1;
                }
                pieces.push(Piece::new(&metainfo, p, written));
            }
        }

        let (tx, rx) = mpsc::channel(100);


        let (tracker_tx, tracker_rx) = mpsc::channel(10);
        let trackers = Trackers::from_metainfo(
            tx.clone(),
            tracker_rx,
            client_id,
            &metainfo,
            0,
            0,
        );
        tokio::spawn(trackers.run().instrument(span.clone()));

        Ok(Self {
            connections: HashMap::new(),
            pieces,
            metainfo,
            peer_tx: tx,
            tracker_tx,
            rx,
            span: span.clone(),
            client_id: client_id.clone(),

            downloaded_pieces,
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

    pub async fn save_state(&self) -> Result<()> {
        info!("Saving");
        let mut bitfield = PieceBitfield::new(self.metainfo.num_pieces);
        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_written() {
                bitfield.set_piece(p);
            }
        }
        let path = Path::new("torrents").join(format!("{}.state", self.metainfo.name));
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path).await?;
        file.write_all(bitfield.as_bytes()).await?;
        Ok(())
    }

    pub async fn request_block(
        &mut self,
        peer_id: &PeerId,
        piece_index: usize,
        block_index: usize,
    ) -> Result<()> {
        let connection = self.connections.get(peer_id).unwrap();
        let span = tracing::info_span!(
            "connection",
            info = %connection.info,
        );
        let _enter = span.enter();

        anyhow::ensure!(
            piece_index < self.metainfo.num_pieces,
            "piece index {} out of range ({} pieces)",
            piece_index,
            self.metainfo.num_pieces,
        );

        let piece_length = self.metainfo.piece_length(piece_index);

        let num_blocks = piece_length.div_ceil(BLOCK_SIZE);
        anyhow::ensure!(
            block_index < num_blocks,
            "block index {} out of range ({} blocks)",
            block_index,
            num_blocks,
        );

        let begin = BLOCK_SIZE * block_index;
        let length = (piece_length - begin).min(BLOCK_SIZE);
        connection.tx.send(Message::Request {
            index: piece_index,
            begin: begin,
            length: length,
        }).await?;
        debug!("Requested block {piece_index}:{block_index}");

        self.connections.get_mut(peer_id).unwrap().sent_requests += 1;

        Ok(())
    }

    fn find_or_keep_piece(&self, target_connection: &Connection) -> Option<usize> {
        if let Some(cursor) = target_connection.piece_cursor
            && self.pieces[cursor].is_available() {
                return Some(cursor);
        }
        
        for (_, connection) in &self.connections {
            if let Some(cursor) = connection.piece_cursor {
                if self.pieces[cursor].is_available()
                    && target_connection.piece_bitfield.has_piece(cursor) {
                        return Some(cursor);
                }
            }
        }

        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_available() && target_connection.piece_bitfield.has_piece(p) {
                return Some(p);
            }
        }

        None
    }

    async fn dispatch_requests(&mut self) -> Result<()> {
        let peer_ids: Vec<_> = self.connections.keys().copied().collect();
        for peer_id in peer_ids {
            while self.connections[&peer_id].can_request() {
                match self.find_or_keep_piece(&self.connections[&peer_id]) {
                    Some(cursor) => {
                        self.connections.get_mut(&peer_id).unwrap().piece_cursor = Some(cursor);
                        if let Some(b) = self.pieces[cursor].find_available_block() {
                            self.pieces[cursor].download(b, peer_id);
                            self.request_block(&peer_id, cursor, b).await?;
                        }
                    }
                    None => {
                        self.connections.get_mut(&peer_id).unwrap().piece_cursor = None;
                        break;
                    }
                }
            }
        }
        Ok(())
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
        let mut timeouts_this_second = HashMap::new();
        for piece in self.pieces.iter_mut() {
            for (peer_id, count) in piece.check_timeout() {
                *timeouts_this_second.entry(peer_id).or_insert(0) += count;
                
                if let Some(connection) = self.connections.get_mut(&peer_id) {
                    connection.sent_requests =
                        connection.sent_requests.saturating_sub(count);
                }
            }
        }

        self.statistics(&timeouts_this_second);
        
        self.dispatch_requests().await
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
                    match message {
                        Message::Bitfield(bitfield) => {
                            anyhow::ensure!(
                                bitfield.num_bytes() == connection.piece_bitfield.num_bytes(),
                                "The received bitfield should be {} bytes long, got {}",
                                connection.piece_bitfield.num_bytes(), bitfield.num_bytes()
                            );
                            connection.piece_bitfield.set_bytes(bitfield.as_bytes());
                            debug!("Set bitfield {:?}", bitfield.as_bytes());
                        }
                        Message::Have { index } => {
                            anyhow::ensure!(
                                index < self.metainfo.num_pieces,
                                "The index of have leads outside the number of pieces, got {}",
                                index
                            );
                            connection.piece_bitfield.set_piece(index);
                            debug!("Handled have");
                        }
                        Message::Request { index, begin, length } => {
                            anyhow::ensure!(
                                index < self.metainfo.num_pieces,
                                "The index of request leads outside the number of pieces, got {}",
                                index
                            );
                            // TODO let's queue that later
                            debug!("Handled request");
                        }
                        Message::Cancel { index, begin, length } => {
                            anyhow::ensure!(
                                index < self.metainfo.num_pieces,
                                "The index of piece leads outside the number of pieces, got {}",
                                index
                            );
                            anyhow::ensure!(
                                begin % BLOCK_SIZE == 0,
                                "Begin is not at block boundary, got {}",
                                begin
                            );
                            debug!("Handled cancel");
                        }
                        Message::Piece { index, begin, piece: block, response_time } => {
                            anyhow::ensure!(
                                index < self.metainfo.num_pieces,
                                "The index of piece leads outside the number of pieces, got {}",
                                index
                            );
                            anyhow::ensure!(
                                begin % BLOCK_SIZE == 0,
                                "Begin is not at block boundary, got {}",
                                begin
                            );
                            let block_index = begin / BLOCK_SIZE;
                            debug!("Got block {index}:{block_index}");
                            connection.downloaded_this_second += block.len();
                            if let Some(response_time) = response_time {
                                connection.response_times_sum += response_time;
                                connection.num_response_times += 1;
                            }
                            if self.pieces[index].place(block_index, block).await? {
                                self.downloaded_pieces += 1;
                            }

                            connection.sent_requests = connection.sent_requests.saturating_sub(1);
                        }

                        Message::Choked(choked) => {
                            connection.choked = choked;
                            debug!("Set choke {choked}");
                            if choked {
                                connection.chokes_this_second += 1;
                                connection.sent_requests = 0;
                                for piece in self.pieces.iter_mut() {
                                    piece.reset(&peer_id);
                                }
                            }
                        }

                        // extensions
                        Message::Reject { index, begin, length } => {
                            anyhow::ensure!(
                                index < self.metainfo.num_pieces,
                                "The index of request leads outside the number of pieces, got {}",
                                index
                            );
                            let block_index = begin / BLOCK_SIZE;
                            connection.sent_requests =
                                connection.sent_requests.saturating_sub(1);
                            debug!("Got rejection for block {index}:{block_index}");
                        }

                        Message::Interested(interested) => connection.interested = interested,
                        Message::KeepAlive => {}
                        Message::Unsupported { type_byte } => {
                            warn!("Handle unsupported message {type_byte}");
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
                    }
                }
            }

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
                for piece in self.pieces.iter_mut() {
                    piece.reset(&peer_id);
                }
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
