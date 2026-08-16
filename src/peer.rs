use anyhow::Result;
use sha1::{Sha1, Digest};
use std::net::TcpStream;
use std::io::prelude::*;
use std::collections::{HashMap, hash_map};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::fs;
use std::path::{Path};

use crate::tracker::PeerInfo;
use crate::metainfo::{Metainfo, MetainfoFile};

const BLOCK_SIZE: usize = 16 * 1024;

pub struct PieceBitfield {
    buffer: Vec<u8>,
}

impl PieceBitfield {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; (size + 7) / 8],
        }
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            buffer: vec,
        }
    }

    pub fn num_bytes(&self) -> usize {
        self.buffer.len()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    fn bit(index: usize) -> u8 {
        0b1000_0000 >> (index % 8)
    }

    pub fn has_piece(&self, index: usize) -> bool {
        self.buffer[index / 8] & Self::bit(index) != 0
    }

    pub fn set_piece(&mut self, index: usize) {
        self.buffer[index / 8] |= Self::bit(index);
    }

    pub fn unset_piece(&mut self, index: usize) {
        self.buffer[index / 8] &= !Self::bit(index);
    }

    pub fn set_bytes(&mut self, bitfield: &[u8]) {
        self.buffer.copy_from_slice(bitfield);
    }
}

pub enum Message {
    KeepAlive,
    Choked(bool),
    Interested(bool),
    Bitfield(PieceBitfield),
    Have(usize),
    Request {
        index: usize,
        begin: usize,
        length: usize,
    },
    Cancel {
        index: usize,
        begin: usize,
        length: usize,
    },
    Piece {
        index: usize,
        begin: usize,
        piece: Vec<u8>,
    },
}

pub struct Peer {
    pub choked: bool,
    pub interested: bool,
    pub stream: TcpStream,
    pub piece_bitfield: PieceBitfield,
    pub blocks: HashMap<(usize, usize), Vec<u8>>,
    pub metainfo: Arc<Metainfo>,
    pub name: String,
}

impl Peer {
    pub fn from_info(
        peer_info: &PeerInfo,
        metainfo: Arc<Metainfo>,
        id: &[u8; 20]
    ) -> Result<Self> {
        let mut handshake = [0u8; 68];
        let name = format!("{peer_info}");

        let mut stream = TcpStream::connect(
            format!("{}:{}", peer_info.host, peer_info.port)
        )?;
        println!("{}: Connected", name);

        // Send the handshake
        handshake[0] = 19;
        handshake[1..20].copy_from_slice(b"BitTorrent protocol");
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(&metainfo.info_hash);
        handshake[48..68].copy_from_slice(id);
        stream.write_all(&handshake)?;
        println!("{}: Sent handshake", name);

        // Receive the handshake response and verify it is right
        stream.read_exact(&mut handshake)?;
        anyhow::ensure!(
            handshake[0] == 19,
            "expected handshake response to start with 19"
        );
        anyhow::ensure!(
            &handshake[1..20] == b"BitTorrent protocol",
            "expected handshake response to have 'BitTorrent protocol'"
        );
        anyhow::ensure!(
            &handshake[28..48] == metainfo.info_hash,
            "expected handshake response to have same info hash"
        );
        if let Some(peer_id) = peer_info.id {
            anyhow::ensure!(
                handshake[48..68] == peer_id,
                "expected handshake response to have right peer id"
            );
        }
        println!("{}: Received handshake", name);

        let num_pieces = metainfo.num_pieces;
        Ok(Self {
            stream,
            metainfo: metainfo,
            name,
            choked: true,
            interested: false,
            piece_bitfield: PieceBitfield::new(num_pieces),
            blocks: HashMap::new(),
        })
    }

    pub fn assemble_piece(&mut self, index: usize) -> Vec<u8> {
        let piece_length = self.metainfo.piece_length(index);
        let mut piece = vec![0; piece_length];
        for (key, val) in self.blocks.extract_if(|(p, _), _| *p == index) {
            let begin = BLOCK_SIZE * key.1;
            piece[begin..begin + val.len()].copy_from_slice(&val);
        }
        println!("{}: Assembled piece {}", self.name, index);
        piece
    }

    pub fn verify_piece(&mut self, index: usize) -> Result<()> {
        let piece = self.assemble_piece(index);
        let hash = Sha1::digest(&piece);
        let correct = hash == self.metainfo.pieces[index].into();
        println!("{}: Verified piece {} to be {}", self.name, index,
            if correct {"correct"} else {"incorrect"});
        if correct {
            self.write_piece(index, piece)?;
        }
        Ok(())
    }

    pub fn write_piece(&self, index: usize, piece: Vec<u8>) -> Result<()> {
        for piece_file in &self.metainfo.piece_files[index] {
            let metainfo_file = &self.metainfo.files[piece_file.file_index];
            let path = Path::new("torrents").join(&metainfo_file.path);
            fs::create_dir_all(&path.parent().unwrap())?;
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            file.set_len(metainfo_file.length as u64)?;
            file.write_all_at(
                &piece[piece_file.piece_offset..(piece_file.piece_offset + piece_file.length)],
                piece_file.file_offset as u64
            )?;
            println!("{}: Wrote piece {} to {}", self.name, index, &path.to_string_lossy());
        }
        Ok(())
    }

    pub fn request_block(&mut self, piece_index: usize, block_index: usize) -> Result<()> {
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
        self.send(Message::Request {
            index: piece_index,
            begin: begin,
            length: length,
        })
    }

    pub fn send(&mut self, message: Message) -> Result<()> {
        let mut log = String::new();
        let mut payload = Vec::new();

        match message {
            Message::Bitfield(bitfield) => {
                payload.push(5);
                payload.extend(bitfield.as_bytes());
            }
            Message::Have(have) => {
                payload.push(4);
                payload.extend((have as u32).to_be_bytes());
            }
            Message::Request{ index, begin, length } => {
                payload.push(6);
                payload.extend((index as u32).to_be_bytes());
                payload.extend((begin as u32).to_be_bytes());
                payload.extend((length as u32).to_be_bytes());
                log = format!(
                    "{}: Sent request {{index: {}, begin: {}, length: {}}}",
                    self.name, index, begin, length
                );
            }
            Message::Cancel{ index, begin, length } => {
                payload.push(8);
                payload.extend((index as u32).to_be_bytes());
                payload.extend((begin as u32).to_be_bytes());
                payload.extend((length as u32).to_be_bytes());
                log = format!(
                    "{}: Sent cancel {{index: {}, begin: {}, length: {}}}",
                    self.name, index, begin, length
                );
            }
            Message::Piece{ index, begin, piece } => {
                payload.push(7);
                payload.extend((index as u32).to_be_bytes());
                payload.extend((begin as u32).to_be_bytes());
                payload.extend(piece);
            }
            Message::Choked(choked) => {
                payload.push(if choked {0} else {1});
                log = format!("{}: Sent choke of {choked}", self.name);
            }
            Message::Interested(interested) => {
                payload.push(if interested {2} else {3});
                log = format!("{}: Sent interest of {interested}", self.name);
            }

            Message::KeepAlive => {}
        }

        let length = u32::try_from(payload.len())?;
        self.stream.write_all(&length.to_be_bytes())?;
        self.stream.write_all(&payload)?;
        eprintln!("{log}");
        Ok(())
    }

    pub fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Bitfield(bitfield) => {
                anyhow::ensure!(
                    bitfield.num_bytes() == self.piece_bitfield.num_bytes(),
                    "The received bitfield should be {} bytes long, got {}",
                    self.piece_bitfield.num_bytes(), bitfield.num_bytes()
                );
                self.piece_bitfield.set_bytes(bitfield.as_bytes());
            }
            Message::Have(index) => {
                anyhow::ensure!(
                    index < self.metainfo.num_pieces,
                    "The index of have leads outside the number of pieces, got {}",
                    index
                );
                self.piece_bitfield.set_piece(index);
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

            Message::Choked(choked) => self.choked = choked,
            Message::Interested(interested) => self.interested = interested,
            _ => panic!("unsupported message"),
        }

        Ok(())
    }

    pub fn receive(&mut self) -> Result<Message> {
        let mut int_buf = [0u8; 4];
        self.stream.read_exact(&mut int_buf)?;
        let message_length = u32::from_be_bytes(int_buf) as usize;

        if message_length == 0 {
            return Ok(Message::KeepAlive);
        }

        let mut id_buf = [0u8; 1];
        self.stream.read_exact(&mut id_buf)?;
        match id_buf[0] {
            5 => { // bitfield
                anyhow::ensure!(
                    message_length >= 2,
                    format!("The length of a bitfield payloud should be at least 2, got {message_length}")
                );
                let mut bitfield = vec![0u8; message_length - 1];
                self.stream.read_exact(&mut bitfield)?;
                println!("{}: Received bitfield {:?}", self.name, bitfield);
                Ok(Message::Bitfield(PieceBitfield::from_vec(bitfield)))
            }
            4 => { // have
                anyhow::ensure!(
                    message_length == 1 + 4,
                    format!("The length of an have payloud should be 5, got {message_length}")
                );
                self.stream.read_exact(&mut int_buf)?;
                let index = u32::from_be_bytes(int_buf) as usize;
                println!("{}: Received have {:?}", self.name, index);
                Ok(Message::Have(index))
            }
            6 => { // request
                anyhow::ensure!(
                    message_length == 1 + 4 + 4 + 4,
                    format!("The length of a request payloud should be 13, got {message_length}")
                );
                self.stream.read_exact(&mut int_buf)?;
                let index = u32::from_be_bytes(int_buf) as usize;
                self.stream.read_exact(&mut int_buf)?;
                let begin = u32::from_be_bytes(int_buf) as usize;
                self.stream.read_exact(&mut int_buf)?;
                let length = u32::from_be_bytes(int_buf) as usize;
                eprintln!(
                    "{}: Received request {{index: {}, begin: {}, length: {}}}",
                    self.name, index, begin, length
                );
                Ok(Message::Request { index, begin, length })
            }
            7 => { // piece
                anyhow::ensure!(
                    message_length > 1 + 4 + 4,
                    format!("The length of a piece payloud should be at least 10, got {message_length}")
                );
                self.stream.read_exact(&mut int_buf)?;
                let index = u32::from_be_bytes(int_buf) as usize;
                self.stream.read_exact(&mut int_buf)?;
                let begin = u32::from_be_bytes(int_buf) as usize;
                let mut piece = vec![0u8; message_length - 1 - 4 - 4];
                self.stream.read_exact(&mut piece)?;
                eprintln!(
                    "{}: Received piece {{index: {}, begin: {}, length: {}}}",
                    self.name, index, begin, piece.len()
                );
                Ok(Message::Piece { index, begin, piece })
            }
            8 => { // cancel
                anyhow::ensure!(
                    message_length == 1 + 4 + 4 + 4,
                    format!("The length of a cancel payloud should be 13, got {message_length}")
                );
                self.stream.read_exact(&mut int_buf)?;
                let index = u32::from_be_bytes(int_buf) as usize;
                self.stream.read_exact(&mut int_buf)?;
                let begin = u32::from_be_bytes(int_buf) as usize;
                self.stream.read_exact(&mut int_buf)?;
                let length = u32::from_be_bytes(int_buf) as usize;
                eprintln!(
                    "{}: Received cancel {{index: {}, begin: {}, length: {}}}",
                    self.name, index, begin, length
                );
                Ok(Message::Cancel { index, begin, length })
            }
            0 => {
                anyhow::ensure!(
                    message_length == 1,
                    format!("The length of a choke payload should be 1, got {message_length}")
                );
                println!("{}: Received choke", self.name);
                Ok(Message::Choked(true))
            }
            1 => {
                anyhow::ensure!(
                    message_length == 1,
                    format!("The length of an unchoke payload should be 1, got {message_length}")
                );
                println!("{}: Received unchoke", self.name);
                Ok(Message::Choked(false))
            }
            2 => {
                anyhow::ensure!(
                    message_length == 1,
                    format!("The length of an intereseted payload should be 1, got {message_length}")
                );
                println!("{}: Received interested", self.name);
                Ok(Message::Interested(true))
            }
            3 => {
                anyhow::ensure!(
                    message_length == 1,
                    format!("The length of an uninterested payload should be 1, got {message_length}")
                );
                println!("{}: Received uninterested", self.name);
                Ok(Message::Interested(false))
            }
            _ => panic!("Unsupported peer message"),
        }
    }
}
