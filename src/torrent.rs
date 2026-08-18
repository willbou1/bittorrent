use std::{collections::HashMap, path::PathBuf};
use sha1::{Sha1, Digest};
use anyhow::Result;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use tracing::{Instrument, info};
use std::{
    path::Path,
    io::SeekFrom,
    time::{Instant, Duration},
};
use rand;

use crate::{
    bitfield::PieceBitfield,
    metainfo::Metainfo,
    peer::{Peer,Message,PeerId},
    tracker::{PeerInfo},
    tracker
};

const BLOCK_SIZE: usize = 16 * 1024;
const MAX_REQUESTS: usize = 2;

pub enum Event {
    PeerMessage(PeerId, Message),
    PeerConnection(Peer, PeerInfo),
    PeerDisconnection(PeerId),
}

pub struct Connection {
    pub info: PeerInfo,
    pub choked: bool,
    pub interested: bool,
    pub piece_bitfield: PieceBitfield,
    pub sent_requests: usize,
    pub tx: mpsc::Sender<Message>,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<Message>, num_pieces: usize, info: &PeerInfo) -> Self {
        Self {
            sent_requests: 0,
            choked: true,
            interested: false,
            piece_bitfield: PieceBitfield::new(num_pieces),
            tx,
            info: info.clone(),
        }
    }
}

struct Piece {
    blocks: Vec<Block>,
}

#[derive(Clone)]
enum Block {
    Unobtained,
    Downloading {
        peer_id: PeerId,
        requested_at: Instant,
    },
    Downloaded(Vec<u8>),
    Written,
}

impl Piece {
    fn new(num_blocks: usize) -> Self {
        Self {
            blocks: vec![Block::Unobtained; num_blocks]
        }
    }
    fn is_downloaded(&self) -> bool {
        self.blocks.iter().all(|b| matches!(b, Block::Downloaded(_)))
    }
    fn is_written(&self) -> bool {
        self.blocks.iter().all(|b| matches!(b, Block::Written))
    }
}

pub struct Torrent {
    pub metainfo: Metainfo,
    pub connections: HashMap<PeerId, Connection>,
    pub pieces: Vec<Piece>,
    pub rx: mpsc::Receiver<Event>,
    pub peer_tx: mpsc::Sender<Event>,
}

impl Torrent {
    pub async fn from_torrent_file(path: &PathBuf) -> Result<Self> {
        let file = fs::read(path).await?;
        let metainfo = Metainfo::from_bytes(&file)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("Parsed metainfo:\n{}", metainfo);

        let (tx, rx) = mpsc::channel(100);

        let mut pieces = vec![];
        for p in 0..metainfo.num_pieces {
            let num_blocks = metainfo.piece_length(p).div_ceil(BLOCK_SIZE);
            pieces.push(Piece::new(num_blocks));
        }

        Ok(Self {
            connections: HashMap::new(),
            pieces,
            metainfo,
            peer_tx: tx,
            rx
        })
    }

    pub async fn download(&mut self, client_id: &PeerId) -> Result<()> {
        let tracker_response = tracker::tracker_request(
            client_id,
            &self.metainfo.announces[0][0],
            &self.metainfo.info_hash, 0, 0, 100,
            tracker::Event::Started
        ).await?;
        println!("Got tracker response:\n{}", tracker_response);

        for peer_info in tracker_response.peers {
            let info_hash = self.metainfo.info_hash;
            let tx = self.peer_tx.clone();
            let client_id = client_id.clone();

            tokio::spawn(async move {
                match Peer::handshake(
                    &peer_info,
                    &info_hash,
                    &client_id,
                ).await {
                    Ok(peer) => {
                        let _ = tx.send(Event::PeerConnection(peer, peer_info)).await;
                    }
                    Err(e) => {
                        info!(peer = %peer_info, "Couldn't connect ({e})");
                    }
                }
            });
        }

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    self.handle_event(event.unwrap()).await?
                }
                _ = interval.tick() => {
                    self.tick().await?
                }
            }
        }
    }

    pub fn assemble_piece(&self, index: usize) -> Vec<u8> {
        let piece_length = self.metainfo.piece_length(index);
        let mut piece = vec![0; piece_length];
        for (b, block) in self.pieces[index].blocks.iter().enumerate() {
            match block {
                Block::Downloaded(block) => {
                    let begin = BLOCK_SIZE * b;
                    piece[begin..begin + block.len()].copy_from_slice(&block);
                }
                _ => panic!("shit is downloading biatch"),
            }
        }
        println!("Assembled piece {}", index);
        piece
    }

    pub fn verify_piece(&self, index: usize, piece: &[u8]) -> bool {
        let hash = Sha1::digest(&piece);
        let correct = hash == self.metainfo.pieces[index].into();
        println!("Verified piece {} to be {}", index,
            if correct {"correct"} else {"incorrect"});
        correct
    }

    async fn write_piece(&self, index: usize, piece: Vec<u8>) -> Result<()> {
        for piece_file in &self.metainfo.piece_files[index] {
            let metainfo_file = &self.metainfo.files[piece_file.file_index];
            let path = Path::new("torrents").join(&metainfo_file.path);
            fs::create_dir_all(&path.parent().unwrap()).await?;
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path).await?;
            file.set_len(metainfo_file.length as u64).await?;
            file.seek(SeekFrom::Start(piece_file.file_offset as u64)).await?;
            file.write_all(
                &piece[piece_file.piece_offset..(piece_file.piece_offset + piece_file.length)],
            ).await?;
            println!("Wrote piece {} to {}", index, &path.to_string_lossy());
        }
        Ok(())
    }

    pub async fn request_block(
        &self,
        peer_id: &PeerId,
        piece_index: usize,
        block_index: usize,
    ) -> Result<()> {
        let connection = self.connections.get(peer_id).unwrap();

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

        Ok(())
    }

    async fn schedule(&mut self) -> Result<()> {
        let peer_ids: Vec<_> = self.connections.keys().cloned().collect();

        for t in 0..MAX_REQUESTS {
            for peer_id in &peer_ids {
                let is_choked = self.connections.get(peer_id).map_or(true, |c| c.choked);
                if is_choked {
                    continue;
                }
            
                let mut written_pieces = 0;
                'outer: for p in 0..self.pieces.len() {
                    let can_continue = if let Some(conn) = self.connections.get(peer_id) {
                        conn.piece_bitfield.has_piece(p) && conn.sent_requests < MAX_REQUESTS
                    } else {
                        false
                    };
                    if !can_continue {
                        continue;
                    }

                    let piece = &self.pieces[p];
                    if piece.is_downloaded() {
                        let assembled = self.assemble_piece(p);
                        if self.verify_piece(p, &assembled) {
                            self.write_piece(p, assembled).await?;
                            for (_, connection) in &self.connections {
                                connection.tx.send(Message::Have(p)).await?;
                            }
                        }
                        self.pieces[p].blocks.fill(Block::Written);
                        continue;
                    } else if piece.is_written() {
                        written_pieces += 1;
                        continue;
                    }

                    for (b, block) in piece.blocks.iter().enumerate() {
                        if matches!(block, Block::Unobtained) {
                            self.request_block(&peer_id, p, b).await?;
                            let connection = self.connections.get_mut(peer_id).unwrap();
                            self.pieces[p].blocks[b] = Block::Downloading {
                                peer_id: peer_id.clone(),
                                requested_at: Instant::now(),
                            };
                            connection.sent_requests += 1;
                            break 'outer;
                        }
                    }
                }

                if written_pieces == self.pieces.len() {
                    let connection = self.connections.get(peer_id).unwrap();
                    info!(peer = %connection.info, "Downloaded biatch");
                     std::process::exit(0);
                }
            }
        }

        Ok(())
    }

    async fn tick(&mut self) -> Result<()> {
        for (p, piece) in self.pieces.iter_mut().enumerate() {
            if piece.is_downloaded() || piece.is_written() {
                continue;
            }

            for (b, block) in piece.blocks.iter_mut().enumerate() {
                match block {
                    Block::Downloading { requested_at, peer_id } => {
                        if requested_at.elapsed() > Duration::from_secs(3) {
                            match self.connections.get_mut(peer_id) {
                                Some(connection) => connection.sent_requests -= 1,
                                None => (),
                            }
                            *block = Block::Unobtained;
                            info!("Block {p}:{b} timed out");
                        }
                    }
                    _ => (),
                }
            }
        }
        self.schedule().await
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::PeerMessage(peer_id, message) => {
                let connection = self.connections.get_mut(&peer_id).unwrap();
                match message {
                    Message::Bitfield(bitfield) => {
                        anyhow::ensure!(
                            bitfield.num_bytes() == connection.piece_bitfield.num_bytes(),
                            "The received bitfield should be {} bytes long, got {}",
                            connection.piece_bitfield.num_bytes(), bitfield.num_bytes()
                        );
                        connection.piece_bitfield.set_bytes(bitfield.as_bytes());
                        info!(peer = %connection.info,"Handled bitfield");
                    }
                    Message::Have(index) => {
                        anyhow::ensure!(
                            index < self.metainfo.num_pieces,
                            "The index of have leads outside the number of pieces, got {}",
                            index
                        );
                        connection.piece_bitfield.set_piece(index);
                        info!(peer = %connection.info,"Handled have");
                    }
                    Message::Request { index, begin, length } => {
                        anyhow::ensure!(
                            index < self.metainfo.num_pieces,
                            "The index of request leads outside the number of pieces, got {}",
                            index
                        );
                        // TODO let's queue that later
                        info!(peer = %connection.info,"Handled request");
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
                        info!(peer = %connection.info,"Handled cancel");
                    }
                    Message::Piece { index, begin, piece } => {
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
                        info!(peer = %connection.info,"Handled piece");
                        let block_index = begin / BLOCK_SIZE;
                        self.pieces[index].blocks[block_index] = Block::Downloaded(piece);
                        self.connections.get_mut(&peer_id).unwrap().sent_requests -= 1;
                    }

                    Message::Choked(choked) => connection.choked = choked,
                    Message::Interested(interested) => connection.interested = interested,
                    Message::KeepAlive => {}
                    _ => info!(peer = %connection.info, "Handle unsupported message {:?}", message),
                }
                self.schedule().await?
            }

            Event::PeerConnection(peer, info) => {
                let (tx, rx) = mpsc::channel(100);
                tx.send(Message::Interested(true)).await?;
                self.connections.insert(
                    peer.id,
                    Connection::new(tx, self.metainfo.num_pieces, &info),
                );
                tokio::spawn(peer.run(self.peer_tx.clone(), rx));
                info!(peer = %info,"Handled connection");
            }

            Event::PeerDisconnection(peer_id) => {
                let connection = self.connections.remove(&peer_id).unwrap();
                info!(peer = %connection.info,"Handled disconnect");
            }
        }
        Ok(())
    }
}
