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

