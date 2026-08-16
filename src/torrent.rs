use std::{collections::HashMap, path::PathBuf};
use sha1::{Sha1, Digest};
use anyhow::Result;
use std::path::{Path};
use std::io::{SeekFrom};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::{
    bitfield::PieceBitfield,
    metainfo::Metainfo,
    peer::{Peer,Message,PeerId},
    tracker,
};

const BLOCK_SIZE: usize = 16 * 1024;

type TorrentMessage = (PeerId, Message);

pub struct Connection {
    pub choked: bool,
    pub interested: bool,
    pub piece_bitfield: PieceBitfield,
    pub tx: mpsc::Sender<Message>,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<Message>, num_pieces: usize) -> Self {
        Self {
            choked: true,
            interested: false,
            piece_bitfield: PieceBitfield::new(num_pieces),
            tx,
        }
    }
}

pub struct Torrent {
    pub metainfo: Metainfo,
    pub connections: HashMap<PeerId, Connection>,
    pub blocks: HashMap<(usize, usize), Vec<u8>>,
    pub rx: mpsc::Receiver<TorrentMessage>,
    pub peer_tx: mpsc::Sender<TorrentMessage>,
}

impl Torrent {
    pub async fn from_torrent_file(path: &PathBuf) -> Result<Self> {
        let file = fs::read(path).await?;
        let metainfo = Metainfo::from_bytes(&file)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("Parsed metainfo:\n{}", metainfo);

        let (tx, rx) = mpsc::channel(100);

        Ok(Self {
            connections: HashMap::new(),
            blocks: HashMap::new(),
            metainfo,
            peer_tx: tx,
            rx
        })
    }

    pub async fn download(&mut self) -> Result<()> {
        let tracker_response = tracker::tracker_request(
            &self.metainfo.announces[0][0],
            &self.metainfo.info_hash, 0, 0, 100,
            tracker::Event::Started
        ).await?;
        println!("Got tracker response:\n{}", tracker_response);

        for peer_info in tracker_response.peers {
            let (tx, rx) = mpsc::channel(100);

            let peer = Peer::handshake(
                &peer_info,
                &self.metainfo.info_hash,
                b"00000000000000000000",
                self.peer_tx.clone(),
                rx,
            ).await?;

            self.connections.insert(peer.id, Connection::new(tx, self.metainfo.num_pieces));
            tokio::spawn(peer.run());
        }

        self.handle_messages().await
    }

    pub fn assemble_piece(&mut self, index: usize) -> Vec<u8> {
        let piece_length = self.metainfo.piece_length(index);
        let mut piece = vec![0; piece_length];
        for (key, val) in self.blocks.extract_if(|(p, _), _| *p == index) {
            let begin = BLOCK_SIZE * key.1;
            piece[begin..begin + val.len()].copy_from_slice(&val);
        }
        println!("Assembled piece {}", index);
        piece
    }

    pub fn verify_piece(&mut self, index: usize) {
        let piece = self.assemble_piece(index);
        let hash = Sha1::digest(&piece);
        let correct = hash == self.metainfo.pieces[index].into();
        println!("Verified piece {} to be {}", index,
            if correct {"correct"} else {"incorrect"});
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
        &mut self,
        peer_id: [u8; 20],
        piece_index: usize,
        block_index: usize,
    ) -> Result<()> {
        let connection = self.connections.get_mut(&peer_id).unwrap();

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

    async fn handle_messages(&mut self) -> Result<()> {
        while let Some((peer_id, message)) = self.rx.recv().await {
            let connection = self.connections.get_mut(&peer_id).unwrap();
            match message {
                Message::Bitfield(bitfield) => {
                    anyhow::ensure!(
                        bitfield.num_bytes() == connection.piece_bitfield.num_bytes(),
                        "The received bitfield should be {} bytes long, got {}",
                        connection.piece_bitfield.num_bytes(), bitfield.num_bytes()
                    );
                    connection.piece_bitfield.set_bytes(bitfield.as_bytes());
                }
                Message::Have(index) => {
                    anyhow::ensure!(
                        index < self.metainfo.num_pieces,
                        "The index of have leads outside the number of pieces, got {}",
                        index
                    );
                    connection.piece_bitfield.set_piece(index);
                }
                Message::Request { index, begin, length } => {
                    anyhow::ensure!(
                        index < self.metainfo.num_pieces,
                        "The index of request leads outside the number of pieces, got {}",
                        index
                    );
                    // TODO let's queue that later
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
                    let block_index = begin / BLOCK_SIZE;
                    self.blocks.insert((index, block_index), piece);
                }

                Message::Choked(choked) => connection.choked = choked,
                Message::Interested(interested) => connection.interested = interested,
                _ => panic!("unsupported message"),
            }
        }
        Ok(())
    }
}
