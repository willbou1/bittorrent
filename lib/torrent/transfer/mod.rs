mod connection;
mod piece_cache;

use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, watch},
};
use std::{
    path::{Path},
    collections::{HashMap, VecDeque},
    time::Duration,
    io::SeekFrom,
};
use tracing::{info, warn, debug, trace};

use crate::{
    bitfield::Bitfield,
    metainfo::Metadata,
    proto::{
        bit_torrent::Message,
        tracker,
    },
    torrent::PieceProgress,
    types::*,
    util::*
};
use super::piece::Piece;
use super::control::*;
use connection::Connection;
use piece_cache::PieceCache;

const PIEC_CACHE_CAPACITY: usize = 8;
const BLOCK_SIZE: usize = 16 * 1024;
fn block_index(begin: usize) -> usize {begin / BLOCK_SIZE}

pub struct Transfer {
    metadata: Metadata,
    tracker_tx: watch::Sender<tracker::Progress>,
    progress_tx: watch::Sender<Progress>,
    pieces: Vec<Piece>,
    piece_bitfield: Bitfield,
    connections: HashMap<PeerId, Connection>,
    piece_cache: PieceCache,

    downloaded_pieces: usize,
    uploaded: usize,
}

impl Transfer {
    pub async fn new(
        metadata: Metadata,
        tracker_tx: watch::Sender<tracker::Progress>,
        progress_tx: watch::Sender<Progress>,
    ) -> Result<Self> {
        let (pieces, downloaded_pieces, piece_bitfield) = Self::load_state(&metadata).await?;
        let downloaded = (downloaded_pieces * metadata.piece_length).min(metadata.length);
        let left = metadata.length - downloaded;
        let _ = tracker_tx.send(tracker::Progress {
            downloaded,
            left,
            event: tracker::Event::Started,
            uploaded: 0,
        });
        Ok(Self {
            piece_cache: PieceCache::new(PIEC_CACHE_CAPACITY),
            pieces,
            metadata,
            tracker_tx,
            progress_tx,
            downloaded_pieces,
            piece_bitfield,
            connections: HashMap::new(),
            uploaded: 0,
        })
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
        fs::create_dir_all(&path.parent().unwrap()).await?;
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
        initial_messages: Option<&mut VecDeque<Message>>,
    ) -> Result<()> {
        let mut con = Connection::new(peer_id, tx, self.metadata.num_pieces, supports_fast);
        if self.downloaded_pieces == 0 && supports_fast {
            con.send(Message::HaveNone).await;
        } else if self.downloaded_pieces == self.metadata.num_pieces && supports_fast {
            con.send(Message::HaveAll).await;
        } else if self.downloaded_pieces != 0 {
            con.send(Message::Bitfield(self.piece_bitfield.clone())).await;
        }
        con.set_am_interested(true).await;
        self.connections.insert(peer_id, con);

        if let Some(initial_messages) = initial_messages {
            for msg in initial_messages.drain(..) {
                trace!("Read initial transfer message {msg:?}");
                self.handle_event(&peer_id, msg).await?;
            }
        }
        Ok(())
    }

    pub fn sever_connection(&mut self, peer_id: &PeerId) {
        for piece in self.pieces.iter_mut() {
            piece.reset(&peer_id);
        }
        self.connections.remove(peer_id);
    }
    
    pub async fn tick(&mut self) -> Result<()> {
        self.check_request_accounting("Before timeouts");
        for piece in self.pieces.iter_mut() {
            for (peer_id, count) in piece.check_timeout() {
                self.connections.entry(peer_id).and_modify(|c| c.timeout(count));
            }
        }
        self.check_request_accounting("After timeouts");

        self.statistics();

        self.check_request_accounting("Before tick dispatch");
        self.dispatch_requests().await?;
        self.check_request_accounting("After tick dispatch");
        Ok(())
    }

    // private
    fn check_request_accounting(&self, when: &str) {
        // This where request leaking gets caught like a lil bitch
        #[cfg(debug_assertions)] { 
            let mut nums: HashMap<PeerId, usize> =
                self.connections.keys().map(|id| (*id, 0)).collect();
            for piece in &self.pieces {
                for (peer_id, count) in piece.num_downloading() {
                    *nums.entry(peer_id).or_insert(0) += count;
                }
            }
            for (peer_id, count) in nums {
                if let Some(con) = self.connections.get(&peer_id) {
                    debug_assert_eq!(
                        con.sent_requests(), count,
                        "{peer_id}: connection RQ={}, piece downloading={count}, when={when}",
                        con.sent_requests(),
                    );
                }
            }
        }
    }
    
    pub async fn handle_event(
        &mut self,
        peer_id: &PeerId,
        message: Message
    ) -> Result<()> {
        let span = tracing::info_span!(
            "connection",
            id = %peer_id,
        );
        let guard = span.enter();
        match message {
            Message::Bitfield(bitfield) => {
                debug!("Set bitfield {:?}", bitfield.as_bytes());
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_bitfield(bitfield)?;
                }
            }

            Message::Have { index } => {
                debug!("Got have {index}");
                self.check_piece_index(index, "have")?;
                self.connections.entry(*peer_id).and_modify(|c| c.set_piece(index));
            }

            Message::Request { index, begin, length } => {
                self.check_piece_length(index, begin, length, false, "request")?;
                let block_index = block_index(begin);
                debug!("Got request {index}:{block_index}");
                if self.connections.get(peer_id).map(|c| c.can_upload()).unwrap_or(false)
                    && self.piece_bitfield.has(index) {
                        let piece = self.read_piece(index, begin, length).await?;
                        debug!("Sent block {index}:{block_index} ({})", piece.len());
                        if let Some(c) = self.connections.get_mut(peer_id) {
                            c.send_piece(index, begin, piece).await;
                        }
                    } else if let Some(c) = self.connections.get_mut(peer_id) && c.supports_fast {
                        c.send(Message::Reject { index, begin, length }).await;
                    } else {
                        // maybe queue this shit to send it later
                    }
            }

            Message::Cancel { index, begin, length } => {
                self.check_piece_length(index, begin, length, true, "cancel")?;
                let block_index = block_index(begin);
                debug!("Got cancel {index}:{block_index}");
            }

            Message::Piece { index, begin, piece: block, response_time } => {
                self.check_piece_begin(index, begin, true, "piece")?;
                let block_index = block_index(begin);

                // maybe keep track of later pieces just for response time
                if !self.pieces[index].has_obtrined(block_index) {
                    debug!("Got block {index}:{block_index}");
                    let who_downloading = self.pieces[index].who_downloading(block_index);
                    self.connections.entry(*peer_id).and_modify(
                        |c| c.receive_piece(
                            block.len(),
                            response_time,
                            who_downloading.map(|w| w == peer_id).unwrap_or(false))
                    );

                    if let Some(who) = who_downloading && who != peer_id {
                        self.connections.get_mut(who).map(|c| c.sub_sent_requests(1));
                    }

                    if let Some(piece) = self.pieces[index].place(block_index, block, true) {
                        self.write_piece(index, &piece).await?;
                        self.piece_bitfield.set(index);
                        self.downloaded_pieces += 1;
                        for con in self.connections.values() {
                            con.send(Message::Have {index}).await;
                        }
                        let (downloaded, left) = self.downloaded_left();
                        let _ = self.tracker_tx.send(tracker::Progress {
                            downloaded,
                            left,
                            event: tracker::Event::Started,
                            uploaded: self.uploaded,
                        });
                    }
                }
            }

            Message::Choked(choked) => {
                debug!("Set choke {choked}");
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_peer_choking(choked);
                    if choked && !con.supports_fast {
                        // All pending requests are considered invalidated
                        for piece in self.pieces.iter_mut() {
                            piece.reset(&peer_id);
                        }
                    }
                }
            }
            Message::Interested(interested) => {
                if let Some(con) = self.connections.get_mut(peer_id) {
                    con.set_peer_interested(interested);
                    con.set_am_choking(false).await;
                }
            }

            // extensions
            Message::Reject { index, begin, length } => {
                self.check_piece_length(index, begin, length, true, "reject")?;
                let block_index = block_index(begin);
                debug!("Got rejection for block {index}:{block_index}");
                // TODO implement BEP semantics properly
                let who_downloading = self.pieces[index].who_downloading(block_index);
                if let Some(who) = who_downloading && who == peer_id {
                    self.connections.entry(*peer_id).and_modify(|c| c.reject());
                    self.pieces[index].reject(block_index, peer_id.clone());
                }
            }
            Message::HaveAll => {
                debug!("Got have all");
                self.connections.entry(*peer_id).and_modify(|c| c.set_pieces());
            },
            Message::HaveNone => {
                debug!("Got have none");
                self.connections.entry(*peer_id).and_modify(|c| c.unset_pieces());
            },
            Message::Suggest { index } => {
                debug!("Got suggest for block {index}");
            },
            Message::AllowedFast { index } => (),

            _ => (),
        }

        self.check_request_accounting("Before dispatch");
        drop(guard);
        self.dispatch_requests().await?;
        self.check_request_accounting("After dispatch");

        Ok(())
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
                pieces.push(Piece::new(Some(p), BLOCK_SIZE, metadata.piece_length(p), metadata.pieces[p], written));
            }
        } else {
            for p in 0..metadata.num_pieces {
                pieces.push(Piece::new(Some(p), BLOCK_SIZE, metadata.piece_length(p), metadata.pieces[p], false));
            }
        }
        Ok((pieces, downloaded_pieces, bitfield))
    }

    async fn read_piece(&mut self, index: usize, begin: usize, length: usize) -> Result<Vec<u8>> {
        if let Some(piece) = self.piece_cache.get(index) {
            return Ok(piece[begin..(begin + length)].to_vec());
        }
        let mut piece = vec![0; self.metadata.piece_length(index)];

        for piece_file in &self.metadata.piece_files[index] {
            let file = &self.metadata.files[piece_file.file_index];
            let path = Path::new("torrents").join(file.path.clone());
            let mut src_file = fs::OpenOptions::new()
                .read(true)
                .open(&path).await?;
            src_file.seek(SeekFrom::Start(piece_file.file_offset as u64)).await?;
            src_file.read_exact(
                &mut piece[piece_file.piece_offset..(piece_file.piece_offset + piece_file.length)]
            ).await?;
            debug!(piece = %index, "Read from {}", &path.to_string_lossy());
        }

        let read = piece[begin..(begin + length)].to_vec();
        self.piece_cache.insert(index, piece);
        Ok(read)
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
        let span = tracing::info_span!(
            "connection",
            id = %peer_id,
        );
        let _enter = span.enter();
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
                        self.connections.entry(peer_id)
                            .and_modify(|p| p.piece_cursor = Some(cursor));
                        if let Some(b) = self.pieces[cursor].find_available_block(&peer_id) {
                            self.request_block(&peer_id, cursor, b).await?;
                            self.pieces[cursor].download(b, peer_id);
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

    fn statistics(&mut self) {
        let mut downloaded_this_second = 0;
        let mut uploaded_this_second = 0;
        let mut timeouts_this_second = 0;

        let mut connections = String::new();
        connections.push_str(&format!("{:<56}│{:<54}│{}\n", "", " Download", " Upload"));
        connections.push_str(
            &format!("{:<56}│ I C {:>4} {:>3} {:>3} {:>9}/s {:>8} {:>4} {:>4} {:>4} │ I C {:>9}/s\n",
                "", "PC", "PL", "RQ", "", "Time", "t/s", "c/s", "r/s", "")
        );
        for (peer_id, con) in self.connections.iter_mut() {
            connections.push_str(&format!("{peer_id} {con}\n"));
            downloaded_this_second += con.downloaded_this_second();
            uploaded_this_second += con.uploaded_this_second();
            timeouts_this_second += con.timeouts_this_second();
            con.reset_stats();
        }
        connections.push_str(
            &format!("{:>56}│     {:>4} {:>3} {:>3} {:>9}/s {:>8} {:>4} {:>4} {:>4} │     {:>9}/s\n",
                format!("Total ({}) ", self.connections.len()),
                "", "", "", pretty_size(downloaded_this_second), "", timeouts_this_second, "", "", pretty_size(uploaded_this_second))
        );
        info!("\n{}", connections,);

        self.uploaded += uploaded_this_second;
        self.progress_tx.send_modify(|p| p.transfer = Some(TransferProgress {
            size: self.metadata.length,
            down_speed: downloaded_this_second,
            up_speed: uploaded_this_second,
            downloaded: self.downloaded_left().0,
            uploaded: self.uploaded,
            piece_bitfield: self.piece_bitfield.clone(),
            active_pieces: self.pieces.iter().enumerate()
                .filter(|(_, p)| p.is_active())
                .map(|(p, piece)| PieceProgress {
                    index: p,
                    block_bitfield: piece.to_bitfield(),
                    num_blocks: piece.num_blocks(),
                    num_obtained_blocks: piece.obtained_blocks(),
                })
                .collect(),
        }));
    }

    fn check_piece_index(&self, index: usize, op: &str) -> Result<()> {
        anyhow::ensure!(
            index < self.metadata.num_pieces,
            "The index of {op} leads outside the number of pieces, got {index}",
        );
        Ok(())
    }

    fn check_piece_begin(&self, index: usize, begin: usize, boundary: bool, op: &str) -> Result<()> {
        self.check_piece_index(index, op)?;
        if boundary {
            anyhow::ensure!(
                begin % BLOCK_SIZE == 0,
                "Begin of {op} is not at block boundary, got {begin}",
            );
        }
        let piece_length = self.metadata.piece_length(index);
        anyhow::ensure!(
            begin < piece_length,
            "Begin of {op} is past piece {index} of length {piece_length}, got {begin}",
        );
        Ok(())
    }

    fn check_piece_length(&self, index: usize, begin: usize, length: usize, boundary: bool, op: &str) -> Result<()> {
        self.check_piece_begin(index, begin, boundary, op)?;
        let piece_length = self.metadata.piece_length(index);
        if boundary {
            anyhow::ensure!(
                length == BLOCK_SIZE,
                "Length does not match block, got {length}",
            );
        }
        anyhow::ensure!(
            begin + length <= piece_length,
            "Length of {op} is past piece {index} of length {piece_length}, got {begin} + {length}",
        );
        Ok(())
    }
}
