use anyhow::Error;
use std::time::Duration;

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

pub fn pretty_duration(duration: Duration) -> String {
    let mut secs = duration.as_secs();
    let days = secs / (60 * 60 * 24);
    secs %= 60 * 60 * 24;
    let hours = secs / (60 * 60);
    secs %= 60 * 60;
    let minutes = secs / 60;
    secs %= 60;

    format!("{}{}{}{}",
        if days > 0 {format!("{days}d ")} else {String::new()},
        if hours > 0 {format!("{hours}h ")} else {String::new()},
        if minutes > 0 {format!("{minutes}m ")} else {String::new()},
        if secs > 0 {format!("{secs}s")} else {String::new()},
    )
}

fn is_disconnection_boring(error: anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>()
        .is_some_and(|e| matches!(e.kind(),
            std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
        ))
}
