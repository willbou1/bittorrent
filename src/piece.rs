use anyhow::Result;
use sha1::{Sha1, Digest};
use std::{
    collections::{HashMap},
    io::SeekFrom,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tracing::{info, warn, debug, trace};

use crate::{
    metainfo::{Metainfo, PieceFile},
    peer::PeerId,
    timer::Timer,
};

type Hash = [u8; 20];

const BLOCK_SIZE: usize = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(2);

pub struct File {
    pub path: PathBuf,
    pub piece_offset: usize,
    pub file_offset: usize,
    pub length: usize,
    pub file_length: usize,
}

#[derive(Clone)]
enum Block {
    Unobtained,
    Downloading {
        peer_id: PeerId,
        timer: Timer,
    },
    Downloaded(Vec<u8>),
    Written,
}

pub struct Piece {
    blocks: Vec<Block>,
    downloaded_blocks: usize,
    downloading_blocks: usize,
    written: bool,
    index: usize,
    hash: Hash,
    length: usize,
    files: Vec<File>,
}

impl Piece {
    pub fn new(metainfo: &Metainfo, index: usize) -> Self {
        let length = metainfo.piece_length(index);
        let num_blocks = length.div_ceil(BLOCK_SIZE);
        let files = metainfo.piece_files[index]
            .iter()
            .map(|pf| File {
                length: pf.length,
                file_offset: pf.file_offset,
                piece_offset: pf.piece_offset,
                path: metainfo.files[pf.file_index].path.clone(),
                file_length: metainfo.files[pf.file_index].length,
            })
            .collect();

        Self {
            blocks: vec![Block::Unobtained; num_blocks],
            downloaded_blocks: 0,
            downloading_blocks: 0,
            written: false,
            hash: metainfo.pieces[index],
            length,
            files,
            index,
        }
    }

    pub fn is_available(&self) -> bool {
        !self.written &&
            (self.downloading_blocks + self.downloaded_blocks) != self.blocks.len()
    }

    pub fn is_downloading(&self) -> bool {
        self.downloading_blocks != 0
    }

    pub fn is_downloaded(&self) -> bool {
        self.downloaded_blocks == self.blocks.len()
    }

    pub fn is_written(&self) -> bool {
        self.written
    }

    pub fn find_available_block(&mut self) -> Option<usize> {
        if !self.is_available() {
            return None;
        }

        for (b, block) in self.blocks.iter_mut().enumerate() {
            match block {
                Block::Unobtained => return Some(b),
                _ => (),
            }
        }
        None
    }

    pub fn download(&mut self, index: usize, peer_id: PeerId) {
        match self.blocks[index] {
            Block::Unobtained => {
                let mut timer = Timer::new();
                timer.start();
                self.blocks[index] = Block::Downloading {
                    peer_id,
                    timer,
                };
                self.downloading_blocks += 1;
            }
            _ => (),
        }
    }

    pub async fn place(&mut self, index: usize, block: Vec<u8>) -> Result<bool> {
        // TODO return a little weird 
        match self.blocks[index] {
            Block::Downloading { .. } => {
                self.blocks[index] = Block::Downloaded(block);
                self.downloading_blocks -= 1;
                self.downloaded_blocks += 1;
            }
            _ => (),
        };

        if self.is_downloaded() && self.write().await? {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn check_timeout(&mut self) -> HashMap<PeerId, usize> {
        if !self.is_downloading() {
            return HashMap::new();
        }

        let mut ret: HashMap<PeerId, usize> = HashMap::new();
        for (b, block) in self.blocks.iter_mut().enumerate() {
            match block {
                Block::Downloading { timer, peer_id, .. } => {
                    if timer.elapsed() > TIMEOUT {
                        self.downloading_blocks -= 1;
                        debug!("Block {}:{b} timed out", self.index);
                        *ret.entry(*peer_id).or_insert(0) += 1;
                        *block = Block::Unobtained;
                    }
                }
                _ => (),
            }
        }
        ret
    }

    pub fn reset(&mut self, affected_peer_id: &PeerId) {
        if !self.is_downloading() {
            return;
        }
       
        for block in self.blocks.iter_mut() {
            match block {
                Block::Downloading { peer_id, .. } => {
                    if affected_peer_id == peer_id {
                        *block = Block::Unobtained;
                    }
                }
                _ => (),
            }
        }
    }

    fn assemble(&self) -> Option<Vec<u8>> {
        if !self.is_downloaded() {
            return None;
        }
        
        let mut piece = vec![0; self.length];
        for (b, block) in self.blocks.iter().enumerate() {
            match block {
                Block::Downloaded(block) => {
                    let begin = BLOCK_SIZE * b;
                    piece[begin..begin + block.len()].copy_from_slice(&block);
                }
                _ => return None,
            }
        }
        Some(piece)
    }

    fn verify(&self, piece: &[u8]) -> bool {
        let actual_hash = Sha1::digest(piece);
        if actual_hash == self.hash.into() {
            debug!(piece = %self.index, "Verification passed");
            return true;
        } else {
            warn!(piece = %self.index, "Verification failed");
            return false;
        }
    }

    async fn write(&mut self) -> Result<bool> {
        if let Some(piece) = self.assemble() {
            let correct = self.verify(&piece);
            if !correct {
                self.blocks.fill(Block::Unobtained);
                return Ok(false);
            }
            
            for piece_file in &self.files {
                let path = Path::new("torrents").join(&piece_file.path);
                fs::create_dir_all(&path.parent().unwrap()).await?;
                let mut file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path).await?;
                file.set_len(piece_file.file_length as u64).await?;
                file.seek(SeekFrom::Start(piece_file.file_offset as u64)).await?;
                file.write_all(
                    &piece[piece_file.piece_offset..(piece_file.piece_offset + piece_file.length)],
                ).await?;
                debug!(piece = %self.index, "Written to {}", &path.to_string_lossy());
            }

            self.blocks.fill(Block::Written);
            self.written = true;
            return Ok(true);
        }
        Ok(false)
    }

}
