use std::{
    collections::{HashMap, VecDeque},
};

pub struct PieceCache {
    pieces: HashMap<usize, Vec<u8>>,
    order: VecDeque<usize>,
    capacity: usize,
}

impl PieceCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            pieces: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.pieces.get(&index).map(Vec::as_slice)
    }

    pub fn insert(&mut self, index: usize, piece: Vec<u8>) {
        if self.pieces.contains_key(&index) {
            return;
        }

        while self.pieces.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.pieces.remove(&oldest);
            }
        }

        self.pieces.insert(index, piece);
        self.order.push_back(index);
    }
}
