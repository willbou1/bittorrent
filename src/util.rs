pub fn pretty_size(size: usize) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let mut unit = 0;
    let mut size = size as f64;
    while size > 1000. {
        size /= 1000.;
        unit += 1;
    }
    format!("{size:.2} {}", UNITS[unit])
}

pub fn chunk_progress(chunks: &[bool]) -> String {
    const BRAILLE_BITS: [u8; 8] = [0, 1, 2, 6, 3, 4, 5, 7];
        
    let mut progress = String::from("[");
    for col in chunks.chunks(8) {
        let mut bits = 0u8;
        for (c, chunk) in col.iter().enumerate() {
            if *chunk {
                bits |= 1 << BRAILLE_BITS[c];
            }
        }
        progress.push(char::from_u32(0x2800 + bits as u32).unwrap());
    }
    progress.push(']');
    progress
}
