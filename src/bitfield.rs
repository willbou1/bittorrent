#[derive(Debug)]
pub struct PieceBitfield {
    buffer: Vec<u8>,
    size: usize,
}

impl PieceBitfield {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; (size + 7) / 8],
            size: size,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            size: vec.len() * 8,
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

    pub fn fill(&mut self, val: bool) {
        for byte in self.buffer.iter_mut() {
            *byte = if val {0xFF} else {0};
        }
    }

    pub fn unset_piece(&mut self, index: usize) {
        self.buffer[index / 8] &= !Self::bit(index);
    }

    pub fn set_bytes(&mut self, bitfield: &[u8]) {
        self.buffer.copy_from_slice(bitfield);
    }
}
