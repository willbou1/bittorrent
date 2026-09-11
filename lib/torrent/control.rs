use crate::{
    types::*,
    bitfield::Bitfield,
};

#[derive(Clone)]
pub struct PieceProgress {
    pub index: usize,
    pub num_blocks: usize,
    pub num_obtained_blocks: usize,
    pub block_bitfield: Bitfield,
}

#[derive(Clone)]
pub struct TransferProgress {
    pub down_speed: usize,
    pub up_speed: usize,
    pub downloaded: usize,
    pub uploaded: usize,
    pub size: usize,
    pub piece_bitfield: Bitfield,
    pub active_pieces: Vec<PieceProgress>,
}

#[derive(Clone)]
pub struct Progress {
    pub num_discovery_attempts: usize,
    pub num_peers: usize,
    pub num_connected_peers: usize,
    pub display_name: String,
    pub metadata_down_speed: usize,
    pub metadata_up_speed: usize,
    pub metadata_bitfield: Option<Bitfield>,
    pub transfer: Option<TransferProgress>,
}

pub enum Command {
    Stop,
    Pause,
    Resume,
}
