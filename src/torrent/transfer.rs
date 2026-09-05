use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use std::{
    path::{Path},
    collections::{HashMap},
    time::Duration,
    io::SeekFrom,
};
use tracing::{info, warn, debug};

use crate::{
    bitfield::Bitfield,
    metainfo::{Metadata},
    proto::{
        bit_torrent::Message,
        tracker::{Progress, Event},
    },
    util::*,
    types::*,
};
use super::{
    piece::Piece,
    connection::Connection,
};

const BLOCK_SIZE: usize = 16 * 1024;
fn block_index(begin: usize) -> usize {begin / BLOCK_SIZE}

pub struct Transfer {
    metadata: Metadata,
    tracker_tx: mpsc::Sender<Progress>,
    pieces: Vec<Piece>,
    piece_bitfield: Bitfield,
    connections: HashMap<PeerId, Connection>,

    downloaded_pieces: usize,
}

impl Transfer {
    pub async fn new(metadata: Metadata, tracker_tx: mpsc::Sender<Progress>) -> Result<Self> {
        let (pieces, downloaded_pieces, piece_bitfield) = Self::load_state(&metadata).await?;
        let downloaded = (downloaded_pieces * metadata.piece_length).min(metadata.length);
        let left = metadata.length - downloaded;
        let _ = tracker_tx.send(Progress {
            downloaded,
            left,
            event: Event::Started,
            uploaded: 0,
        }).await;
        Ok(Self {
            pieces,
            metadata,
            tracker_tx,
            downloaded_pieces,
            piece_bitfield,
            connections: HashMap::new(),
        })
    }

    async fn load_state(metadata: &Metadata) -> Result<(Vec<Piece>, usize, Bitfield)> {
        let mut pieces = vec![];
        let path = Path::new("torrents").join(format!("{}.state", metadata.name));
        let mut downloaded_pieces = 0;
        let mut bitfield = Bitfield::new(metadata.num_pieces);
        if fs::try_exists(&path).await? {
            info!("Recovering");
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path).await?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;
            bitfield.set_bytes(&bytes);
            for p in 0..metadata.num_pieces {
                let written = bitfield.has(p);
                if written {
                    downloaded_pieces += 1;
                }
                pieces.push(Piece::new(Some(p), metadata.piece_length(p), metadata.pieces[p], written));
            }
        } else {
            for p in 0..metadata.num_pieces {
                pieces.push(Piece::new(Some(p), metadata.piece_length(p), metadata.pieces[p], false));
            }
        }
        Ok((pieces, downloaded_pieces, bitfield))
    }

    pub async fn save_state(&self) -> Result<()> {
        info!("Saving");
        let mut bitfield = Bitfield::new(self.metadata.num_pieces);
        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_written() {
                bitfield.set(p);
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

    pub async fn add_connection(
        &mut self,
        peer_id: PeerId,
        tx: mpsc::Sender<Message>,
        supports_fast: bool,
    ) -> Result<()> {
        let mut con = Connection::new(tx, self.metadata.num_pieces, supports_fast);
        if self.downloaded_pieces == 0 && supports_fast {
            con.send(Message::HaveNone).await;
        } else if self.downloaded_pieces == self.metadata.num_pieces && supports_fast {
            con.send(Message::HaveAll).await;
        } else if self.downloaded_pieces != 0 {
            con.send(Message::Bitfield(self.piece_bitfield.clone())).await;
        }
        con.set_am_interested(true).await;
        self.connections.insert(peer_id, con);
        Ok(())
    }

    pub fn sever_connection(&mut self, peer_id: &PeerId) {
        for piece in self.pieces.iter_mut() {
            piece.reset(&peer_id);
        }
        self.connections.remove(peer_id);
    }
    
    pub async fn tick(&mut self) -> Result<()> {
        for piece in self.pieces.iter_mut() {
            for (peer_id, count) in piece.check_timeout() {
                self.connections.get_mut(&peer_id) .map(|c| c.timeout(count));
            }
        }

        self.statistics();
        self.dispatch_requests().await
    }

    async fn write_piece(&self, index: usize, piece: &[u8]) -> Result<()> {
        for piece_file in &self.metadata.piece_files[index] {
            let file = &self.metadata.files[piece_file.file_index];
            let path = Path::new("torrents").join(file.path.clone());
            fs::create_dir_all(&path.parent().unwrap()).await?;
            let mut dst_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path).await?;
            dst_file.set_len(file.length as u64).await?;
            dst_file.seek(SeekFrom::Start(piece_file.file_offset as u64)).await?;
            dst_file.write_all(
                &piece[piece_file.piece_offset..(piece_file.piece_offset + piece_file.length)],
            ).await?;
            debug!(piece = %index, "Written to {}", &path.to_string_lossy());
        }
        Ok(())
    }

    async fn request_block(
        &mut self,
        peer_id: &PeerId,
        piece_index: usize,
        block_index: usize,
    ) -> Result<()> {
        let piece_length = self.metadata.piece_length(piece_index);
        let begin = BLOCK_SIZE * block_index;
        let length = (piece_length - begin).min(BLOCK_SIZE);
        self.connections.get_mut(peer_id).unwrap()
            .request(piece_index, begin, length).await;
        debug!("Requested block {piece_index}:{block_index}");
        Ok(())
    }

    fn find_or_keep_piece(&self, target_connection: &Connection) -> Option<usize> {
        if let Some(cursor) = target_connection.piece_cursor
            && self.pieces[cursor].is_available() {
                return Some(cursor);
        }
        
        for con in self.connections.values() {
            if let Some(cursor) = con.piece_cursor {
                if self.pieces[cursor].is_available() && target_connection.has_piece(cursor) {
                        return Some(cursor);
                }
            }
        }

        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_available() && target_connection.has_piece(p) {
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
                        if let Some(b) = self.pieces[cursor].find_available_block(&peer_id) {
                            self.pieces[cursor].download(b, peer_id);
                            self.request_block(&peer_id, cursor, b).await?;
                        } else {
                            break;
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

    fn downloaded_left(&self) -> (usize, usize) {
        let downloaded = (self.downloaded_pieces * self.metadata.piece_length)
            .min(self.metadata.length);
        let left = self.metadata.length - downloaded;
        (downloaded, left)
    }

    fn eta(&self, downloaded_this_second: usize) -> Duration {
        let (_, left) = self.downloaded_left();
        Duration::from_secs(
            if downloaded_this_second == 0 {
                0
            } else {
                (left / downloaded_this_second) as u64
            }
        )
    }

    fn statistics(&mut self) {
        let mut status = String::new();
        let mut downloaded_this_second = 0;
        let mut timeouts_this_second = 0;

        for (peer_id, con) in self.connections.iter_mut() {
            status.push_str(&format!("    {peer_id}: {con}\n"));
            downloaded_this_second += con.downloaded_this_second();
            timeouts_this_second += con.timeouts_this_second();
            con.reset_stats();
        }

        let mut pieces_bitfield = Bitfield::new(self.pieces.len());
        for (p, piece) in self.pieces.iter().enumerate() {
            if piece.is_written() {
                pieces_bitfield.set(p);
            }
        }

        // global
        status.push_str(&format!(" {:90} {:.2}% ({}/{}) ⇣ {}/s ETA {}\n",
            pieces_bitfield,
            self.downloaded_pieces as f64 * 100. / self.pieces.len() as f64,
            self.downloaded_pieces, self.pieces.len(),
            pretty_size(downloaded_this_second),
            pretty_duration(self.eta(downloaded_this_second)),
        ));

        // blocks
        for (p, piece) in self.pieces.iter().enumerate().filter(|(_, p)|  p.is_active()) {
            status.push_str(&format!("{}: {:30} {:.2}% ({}/{})\n",
                p,
                piece.to_bitfield(),
                piece.obtained_blocks() as f64 * 100. / piece.num_blocks() as f64,
                piece.obtained_blocks(), piece.num_blocks(),
            ));
        }

        info!("♟ {} ⏱ {} t/s \n{}",
            self.connections.len(),
            timeouts_this_second,
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

    fn check_piece_begin(&self, index: usize, begin: usize, op: &str) -> Result<()> {
        self.check_piece_index(index, op)?;
        anyhow::ensure!(
            begin % BLOCK_SIZE == 0,
            "Begin of {op} is not at block boundary, got {}",
            begin
        );
        let piece_length = self.metadata.piece_length(index);
        anyhow::ensure!(
            begin < piece_length,
            "Begin of {op} is past piece {index} of length {piece_length}, got {}",
            begin
        );
        Ok(())
    }

    fn check_piece_length(&self, index: usize, begin: usize, length: usize, op: &str) -> Result<()> {
        self.check_piece_begin(index, begin, op)?;
        let piece_length = self.metadata.piece_length(index);
        anyhow::ensure!(
            begin + length <= piece_length,
            "Length of {op} is past piece {index} of length {piece_length}, got {} + {}",
            begin, length
        );
        Ok(())
    }

    pub async fn handle_event(
        &mut self,
        peer_id: &PeerId,
        message: Message
    ) -> Result<()> {
        match message {
            Message::Bitfield(bitfield) => {
                debug!("Set bitfield {:?}", bitfield.as_bytes());
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_bitfield(bitfield)?;
                }
            }

            Message::Have { index } => {
                self.check_piece_index(index, "have")?;
                self.connections.get_mut(peer_id).map(|c| c.set_piece(index));
                debug!("Handled have");
            }

            Message::Request { index, begin, length } => {
                self.check_piece_length(index, begin, length, "request")?;
                // TODO let's queue that later
                warn!("Got request");
            }

            Message::Cancel { index, begin, length } => {
                self.check_piece_length(index, begin, length, "cancel")?;
                debug!("Handled cancel");
            }

            Message::Piece { index, begin, piece: block, response_time } => {
                self.check_piece_begin(index, begin, "piece")?;
                let block_index = block_index(begin);

                if !self.pieces[index].has_obtrined(block_index) {
                    self.connections.get_mut(peer_id)
                        .map(|c| c.piece(block.len(), response_time));

                    if let Some(piece) = self.pieces[index].place(block_index, block) {
                        self.write_piece(index, &piece).await?;
                        self.piece_bitfield.set(index);
                        self.downloaded_pieces += 1;
                        for con in self.connections.values() {
                            con.send(Message::Have {index}).await;
                        }
                        let (downloaded, left) = self.downloaded_left();
                        let _ = self.tracker_tx.send(Progress {
                            downloaded,
                            left,
                            event: Event::Started,
                            uploaded: 0,
                        }).await;
                    }
                    debug!("Got block {index}:{block_index}");
                }
            }

            Message::Choked(choked) => {
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_peer_choking(choked);
                    if choked && !con.supports_fast {
                        // All pending requests are considered invalidated
                        for piece in self.pieces.iter_mut() {
                            piece.reset(&peer_id);
                        }
                        con.reset_sent_requests();
                    }
                }
                debug!("Set choke {choked}");
            }
            Message::Interested(interested) => {
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_peer_interested(interested);
                    //con.set_am_choking(false).await;
                }
            }

            // extensions
            Message::Reject { index, begin, length } => {
                self.check_piece_length(index, begin, length, "reject")?;
                let block_index = block_index(begin);
                self.connections.get_mut(peer_id).map(|c| c.reject());
                self.pieces[index].reject(block_index, peer_id.clone());
                debug!("Got rejection for block {index}:{block_index}");
            }
            Message::HaveAll => {
                self.connections.get_mut(peer_id).map(|c| c.set_pieces());
                debug!("Got have all");
            },
            Message::HaveNone => {
                self.connections.get_mut(peer_id).map(|c| c.unset_pieces());
                debug!("Got have none");
            },
            Message::Suggest { index } => {
                warn!("Got suggest for block {index}");
            },
            Message::AllowedFast { index } => (),

            _ => (),
        }

        self.dispatch_requests().await?;

        Ok(())
    }
}
