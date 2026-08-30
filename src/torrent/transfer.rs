use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use std::{
    path::{Path},
    collections::HashMap,
    time::Duration,
};
use tracing::{info, warn, debug};

use crate::{
    bitfield::PieceBitfield,
    metainfo::{Metainfo, Metadata},
    peer::{Message, Peer},
    util::pretty_size,
    types::*,
};
use super::{
    piece::Piece,
    Torrent,
    Connection,
};

const BLOCK_SIZE: usize = 16 * 1024;

const DEFAULT_MAX_REQUESTS: usize = 20;
const MIN_MAX_REQUESTS: usize = 5;
const MAX_MAX_REQUESTS: usize = 70;
const MAX_REQUESTS_STEP: usize = 2;

pub struct TransferConnection {
    choked: bool,
    interested: bool,
    piece_bitfield: PieceBitfield,
    piece_cursor: Option<usize>,
    sent_requests: usize,
    tx: mpsc::Sender<Message>,
    max_requests: usize,

    downloaded_this_second: usize,
    last_download_rate: usize,
    response_times_sum: Duration,
    num_response_times: usize,
    chokes_this_second: usize,
}

impl TransferConnection {
    fn new(tx: mpsc::Sender<Message>, num_pieces: usize) -> Self {
        Self {
            sent_requests: 0,
            piece_cursor: None,
            choked: true,
            interested: false,
            piece_bitfield: PieceBitfield::new(num_pieces),
            tx,
            max_requests: DEFAULT_MAX_REQUESTS,

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

pub struct Transfer {
    metadata: Metadata,
    pieces: Vec<Piece>,
    downloaded_pieces: usize,
    connections: HashMap<PeerId, TransferConnection>,
}

impl Transfer {
    pub async fn new(metadata: Metadata) -> Result<Self> {
        let (pieces, downloaded_pieces) = Self::load_state(&metadata).await?;
        Ok(Self {
            pieces,
            metadata,
            downloaded_pieces,
            connections: HashMap::new(),
        })
    }

    async fn load_state(metadata: &Metadata) -> Result<(Vec<Piece>, usize)> {
        let mut pieces = vec![];
        let path = Path::new("torrents").join(format!("{}.state", metadata.name));
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
            for p in 0..metadata.num_pieces {
                let written = bitfield.has_piece(p);
                if written {
                    downloaded_pieces += 1;
                }
                pieces.push(Piece::new(metadata, p, written));
            }
        } else {
            for p in 0..metadata.num_pieces {
                pieces.push(Piece::new(metadata, p, false));
            }
        }
        Ok((pieces, downloaded_pieces))
    }

    pub async fn save_state(&self) -> Result<()> {
        info!("Saving");
        let mut bitfield = PieceBitfield::new(self.metadata.num_pieces);
        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_written() {
                bitfield.set_piece(p);
            }
        }
        let path = Path::new("torrents").join(format!("{}.state", self.metadata.name));
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path).await?;
        file.write_all(bitfield.as_bytes()).await?;
        Ok(())
    }

    pub async fn add_connection(&mut self, peer_id: PeerId, tx: mpsc::Sender<Message>) -> Result<()> {
        tx.send(Message::Interested(true)).await?;
        self.connections.insert(
            peer_id,
            TransferConnection::new(tx, self.metadata.num_pieces),
        );
        Ok(())
    }

    pub fn sever_connection(&mut self, peer_id: &PeerId) {
        for piece in self.pieces.iter_mut() {
            piece.reset(&peer_id);
        }
        self.connections.remove(peer_id);
    }
    
    pub async fn tick(&mut self) -> Result<()> {
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

    async fn request_block(
        &mut self,
        peer_id: &PeerId,
        piece_index: usize,
        block_index: usize,
    ) -> Result<()> {
        let connection = self.connections.get(peer_id).unwrap();

        anyhow::ensure!(
            piece_index < self.metadata.num_pieces,
            "piece index {} out of range ({} pieces)",
            piece_index,
            self.metadata.num_pieces,
        );

        let piece_length = self.metadata.piece_length(piece_index);

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

    fn find_or_keep_piece(&self, target_connection: &TransferConnection) -> Option<usize> {
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
                &format!("    {}: {} {} |{} ⇢ {} ⇣ {}/s ⏱ {:.2} ms {} to/s {} c/s \n",
                    peer_id,
                    if connection.choked {'C'} else {'U'},
                    if connection.interested {'I'} else {'-'},
                    connection.max_requests,
                    connection.sent_requests,
                    pretty_size(connection.downloaded_this_second),
                    connection.response_times_sum.as_secs_f64() * 1000.
                        / connection.num_response_times as f64,
                    timeouts_this_second.get(peer_id).unwrap_or(&0),
                    connection.chokes_this_second));

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

    fn check_piece_index(&self, index: usize, op: &str) -> Result<()> {
        anyhow::ensure!(
            index < self.metadata.num_pieces,
            "The index of {op} leads outside the number of pieces, got {}",
            index
        );
        Ok(())
    }

    fn check_piece_begin(begin: usize, op: &str) -> Result<()> {
        anyhow::ensure!(
            begin % BLOCK_SIZE == 0,
            "Begin of {op} is not at block boundary, got {}",
            begin
        );
        Ok(())
    }

    pub async fn handle_event(
        &mut self,
        peer_id: &PeerId,
        message: Message
    ) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(peer_id) {
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
                    //self.check_piece_index(index, "have")?;
                    connection.piece_bitfield.set_piece(index);
                    debug!("Handled have");
                }

                Message::Request { index, begin, length } => {
                    self.check_piece_index(index, "request")?;
                    Self::check_piece_begin(begin, "request")?;
                    // TODO let's queue that later
                    debug!("Handled request");
                }

                Message::Cancel { index, begin, length } => {
                    self.check_piece_index(index, "cancel")?;
                    debug!("Handled cancel");
                }

                Message::Piece { index, begin, piece: block, response_time } => {
                    //self.check_piece_index(index, "piece")?;
                    Self::check_piece_begin(begin, "piece")?;
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
                Message::Interested(interested) => connection.interested = interested,

                // extensions
                Message::Reject { index, begin, length } => {
                    //self.check_piece_index(index, "reject")?;
                    Self::check_piece_begin(begin, "reject")?;
                    let block_index = begin / BLOCK_SIZE;
                    connection.sent_requests =
                        connection.sent_requests.saturating_sub(1);
                    debug!("Got rejection for block {index}:{block_index}");
                }

                Message::Unsupported { type_byte } => {
                    warn!("Handle unsupported message {type_byte}");
                }

                _ => (),
            }

            self.dispatch_requests().await?;
        }

        Ok(())
    }
}
