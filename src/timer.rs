use std::time::{Instant, Duration};

#[derive(Clone)]
pub struct Timer {
    elapsed: Duration,
    start_time: Option<Instant>,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            elapsed: Duration::from_millis(0),
            start_time: None,
        }
    }

    pub fn start(&mut self) {
        match self.start_time {
            Some(_) => (),
            None => self.start_time = Some(Instant::now()),
        }
    }

    pub fn stop(&mut self) {
        if let Some(start_time) = self.start_time {
            self.elapsed += start_time.elapsed();
            self.start_time = None;
        }
    }

    pub fn elapsed(&self) -> Duration {
        match self.start_time {
            Some(start_time) => self.elapsed + start_time.elapsed(),
            None => self.elapsed,
        }
    }

    pub fn restart(&mut self) {
        self.elapsed = Duration::default();
        self.start_time = self.start_time.map(|_| Instant::now());
    }
}
