use std::fmt;

#[derive(Debug)]
pub struct Bitfield {
    buffer: Vec<u8>,
    size: usize,
}

impl Bitfield {
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

    pub fn has(&self, index: usize) -> bool {
        self.buffer[index / 8] & Self::bit(index) != 0
    }

    pub fn set(&mut self, index: usize) {
        self.buffer[index / 8] |= Self::bit(index);
    }

    pub fn fill(&mut self, val: bool) {
        for byte in self.buffer.iter_mut() {
            *byte = if val {0xFF} else {0};
        }
    }

    pub fn unset(&mut self, index: usize) {
        self.buffer[index / 8] &= !Self::bit(index);
    }

    pub fn set_bytes(&mut self, bitfield: &[u8]) {
        self.buffer.copy_from_slice(bitfield);
    }
}

impl fmt::Display for Bitfield {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const BRAILLE_BITS: [u8; 8] = [0, 1, 2, 6, 3, 4, 5, 7];

        let num_cols = f.width().unwrap_or(0);
        write!(f, "[ ")?;

        let mut bits = 0u8;
        let chunks = num_cols * 8;
        let len = self.len();

        for c in 0..chunks {
            let start = c * len / chunks;
            let end = ((c + 1) * len + chunks - 1) / chunks;
            
            if (start..end).all(|i| self.has(i)) {
                bits |= 1 << BRAILLE_BITS[c % 8];
            }
            
            if c % 8 == 7 {
                if self.len() == 0 {
                    bits = 0;
                }
                write!(f, "{}", char::from_u32(0x2800 + bits as u32).unwrap())?;
                bits = 0;
            }
        }

        write!(f, " ]")?;
        Ok(())
    }
}

