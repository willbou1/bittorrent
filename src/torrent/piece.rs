use anyhow::Result;
use sha1::{Sha1, Digest};
use std::{
    collections::{HashMap},
    time::{Duration},
};
use tracing::{info, warn, debug, trace};

use crate::{
    timer::Timer,
    types::*,
};

type Hash = [u8; 20];

const BLOCK_SIZE: usize = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(2);
const REJECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum BlockState {
    Unobtained,
    Downloading {
        peer_id: PeerId,
        timer: Timer,
    },
    Downloaded(Vec<u8>),
    Written,
}

#[derive(Clone)]
struct Block {
    state: BlockState,
    rejects: HashMap<PeerId, Timer>,
}

impl Block {
    fn new(state: BlockState) -> Self {
        Self {
            state,
            rejects: HashMap::new(),
        }
    }
}

impl Default for Block {
    fn default() -> Self {
        Self {
            state: BlockState::Unobtained,
            rejects: HashMap::new(),
        }
    }
}

pub struct Piece {
    blocks: Vec<Block>,
    downloaded_blocks: usize,
    downloading_blocks: usize,
    written: bool,
    hash: Hash,
    length: usize,
    index: usize,
    for_metadata: bool,
}

impl Piece {
    pub fn new(
        for_metadata: bool,
        index: usize,
        length: usize,
        hash: Hash,
        written: bool,
    ) -> Self {
        let num_blocks = length.div_ceil(BLOCK_SIZE);

        Self {
            blocks: vec![Block::default(); num_blocks],
            downloaded_blocks: 0,
            downloading_blocks: 0,
            written,
            hash,
            length,
            index,
            for_metadata,
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

    pub fn find_available_block(&mut self, peer_id: &PeerId) -> Option<usize> {
        if !self.is_available() {
            return None;
        }

        for (b, block) in self.blocks.iter_mut().enumerate() {
            match block.state {
                BlockState::Unobtained => {
                    match block.rejects.get(peer_id) {
                        Some(timer) => {
                            if timer.elapsed() > REJECTION_TIMEOUT {
                                block.rejects.remove(&peer_id);
                                return Some(b);
                            }
                            continue;
                        }
                        None => return Some(b),
                    }
                },
                _ => (),
            }
        }
        None
    }

    pub fn download(&mut self, index: usize, peer_id: PeerId) {
        match &mut self.blocks[index].state {
            state @ BlockState::Unobtained => {
                let mut timer = Timer::new();
                timer.start();
                *state = BlockState::Downloading {
                    peer_id,
                    timer,
                };
                self.downloading_blocks += 1;
            }
            _ => (),
        }
    }

    pub fn reject(&mut self, index: usize, peer_id: PeerId) {
        let block = &mut self.blocks[index];
        match &mut block.state {
            state @ BlockState::Downloading { .. } => {
                let mut timer = Timer::new();
                timer.start();
                block.rejects.insert(peer_id, timer);
                *state = BlockState::Unobtained;
                self.downloading_blocks -= 1;
            }
            _ => (),
        }
    }

    pub fn place(&mut self, index: usize, block: Vec<u8>) -> Option<Vec<u8>> {
        match &mut self.blocks[index].state {
            state @ BlockState::Downloading { .. } => {
                *state = BlockState::Downloaded(block);
                self.downloading_blocks -= 1;
                self.downloaded_blocks += 1;
            }
            _ => (),
        };

        if self.is_downloaded() {
            if let Some(piece) = self.assemble() {
                let correct = self.verify(&piece);
                if !correct {
                    self.blocks.fill(Block::default());
                    return None;
                }
                // TODO: we may wanna handle write error gracefully and signal to piece that we should write
                self.blocks.fill(Block::new(BlockState::Written));
                self.written = true;
                return Some(piece);
            }
        }

        None
    }

    pub fn check_timeout(&mut self) -> HashMap<PeerId, usize> {
        if !self.is_downloading() {
            return HashMap::new();
        }

        let mut ret: HashMap<PeerId, usize> = HashMap::new();
        for (b, block) in self.blocks.iter_mut().enumerate() {
            match &block.state {
                BlockState::Downloading { timer, peer_id, .. } => {
                    if timer.elapsed() > TIMEOUT {
                        self.downloading_blocks -= 1;
                        debug!("Block {}:{b} timed out", self.index);
                        *ret.entry(*peer_id).or_insert(0) += 1;
                        block.state = BlockState::Unobtained;
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
            match &mut block.state {
                BlockState::Downloading { peer_id, .. } => {
                    if affected_peer_id == peer_id {
                        block.state = BlockState::Unobtained;
                        self.downloading_blocks -= 1;
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
            match &block.state {
                BlockState::Downloaded(block) => {
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
            debug!(metadata = &self.for_metadata,  piece = %self.index,
                "Verification passed");
            return true;
        } else {
            warn!(metadata = &self.for_metadata,  piece = %self.index,
                "Verification failed");
            return false;
        }
    }
}
