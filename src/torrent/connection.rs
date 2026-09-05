use anyhow::Result;
use tokio::{
    sync::mpsc,
};
use std::{
    time::{Duration},
    fmt,
};

use crate::{
    bitfield::Bitfield,
    proto::bit_torrent::{Message},
    util::*,
};

const DEFAULT_MAX_REQUESTS: usize = 70;
const MIN_MAX_REQUESTS: usize = 5;
const MAX_MAX_REQUESTS: usize = 70;
const MAX_REQUESTS_STEP: usize = 2;

pub struct Connection {
    pub supports_fast: bool,
    pub piece_cursor: Option<usize>,
    tx: mpsc::Sender<Message>,

    sent_requests: usize,
    max_requests: usize,

    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    piece_bitfield: Bitfield,

    downloaded_this_second: usize,
    response_times_sum: Duration,
    num_response_times: usize,
    chokes_this_second: usize,
    rejects_this_second: usize,
    timeouts_this_second: usize,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<Message>, num_pieces: usize, supports_fast: bool) -> Self {
        Self {
            supports_fast,
            sent_requests: 0,
            piece_cursor: None,
            tx,
            max_requests: DEFAULT_MAX_REQUESTS,

            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            piece_bitfield: Bitfield::new(num_pieces),

            downloaded_this_second: 0,
            num_response_times: 0,
            response_times_sum: Duration::default(),
            chokes_this_second: 0,
            rejects_this_second: 0,
            timeouts_this_second: 0,
        }
    }

    pub fn can_request(&self) -> bool {
        self.sent_requests < self.max_requests && !self.peer_choking
    }

    pub fn downloaded_this_second(&self) -> usize {self.downloaded_this_second}
    pub fn timeouts_this_second(&self) -> usize {self.timeouts_this_second}
    pub fn max_requests(&self) -> usize {self.max_requests}
    pub fn sent_requests(&self) -> usize {self.sent_requests}
    pub fn am_choking(&self) -> bool {self.am_choking}
    pub fn am_interested(&self) -> bool {self.am_interested}
    pub fn peer_choking(&self) -> bool {self.peer_choking}
    pub fn peer_interested(&self) -> bool {self.peer_interested}
    pub fn piece_bitfield(&self) -> &Bitfield {&self.piece_bitfield}

    pub fn sub_sent_requests(&mut self, count: usize) {
        self.sent_requests = self.sent_requests.saturating_sub(count);
    }
    pub fn reset_sent_requests(&mut self) {
        self.sent_requests = 0;
    }

    pub async fn set_am_choking(&mut self, choking: bool) {
        self.am_choking = choking;
        self.send(Message::Choked(choking)).await;
    }

    pub async fn set_am_interested(&mut self, interested: bool) {
        self.am_interested = interested;
        self.send(Message::Interested(interested)).await;
    }

    pub fn set_peer_choking(&mut self, choking: bool) {
        self.peer_choking = choking;
        if choking {
            self.chokes_this_second += 1;
        }
    }

    pub fn set_peer_interested(&mut self, interested: bool) {
        self.peer_interested = interested;
    }

    pub fn set_bitfield(&mut self, bitfield: Bitfield) -> Result<()> {
        anyhow::ensure!(
            bitfield.num_bytes() == self.piece_bitfield.num_bytes(),
            "The received bitfield should be {} bytes long, got {}",
            self.piece_bitfield.num_bytes(), bitfield.num_bytes()
        );
        self.piece_bitfield.set_bytes(bitfield.as_bytes());
        Ok(())
    }
    pub fn has_piece(&self, index: usize) -> bool {
        self.piece_bitfield.has(index)
    }
    pub fn set_piece(&mut self, index: usize) {
        self.piece_bitfield.set(index)
    }
    pub fn set_pieces(&mut self) {
        self.piece_bitfield.fill(true);
    }
    pub fn unset_pieces(&mut self) {
        self.piece_bitfield.fill(false);
    }

    pub fn reject(&mut self) {
        self.rejects_this_second += 1;
        self.sub_sent_requests(1);
    }

    pub fn timeout(&mut self, count: usize) {
        self.timeouts_this_second += 1;
        self.sub_sent_requests(count);
    }

    pub async fn send(&self, message: Message) {
        let _ = self.tx.send(message).await;
    }

    pub async fn request(&mut self, index: usize, begin: usize, length: usize) {
        self.send(Message::Request { index, begin, length }).await;
        self.sent_requests += 1;
    }

    pub fn piece(&mut self, length: usize, response_time: Option<Duration>) {
        self.sub_sent_requests(1);
        self.downloaded_this_second += length;
        if let Some(response_time) = response_time {
            self.response_times_sum += response_time;
            self.num_response_times += 1;
        }
    }

    pub fn reset_stats(&mut self) {
        self.downloaded_this_second = 0;
        self.num_response_times = 0;
        self.response_times_sum = Duration::default();
        self.chokes_this_second = 0;
        self.rejects_this_second = 0;
        self.timeouts_this_second = 0;
    }
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:10} ⬇ {}{} {} |{} ⇢ {} {}/s ⏱ {:.2} ms {} t/s {} c/s {} r/s ⬆ {}{}",
            self.piece_bitfield(),
            if self.am_interested() {'I'} else {'-'},
            if self.peer_choking() {'C'} else {'U'},
            self.piece_cursor.map(|c| format!("■ {c}")).unwrap_or(String::new()),
            self.max_requests(),
            self.sent_requests(),
            pretty_size(self.downloaded_this_second),
            self.response_times_sum.as_secs_f64() * 1000.
                / self.num_response_times as f64,
            self.timeouts_this_second,
            self.chokes_this_second,
            self.rejects_this_second,
            if self.peer_interested() {'I'} else {'-'},
            if self.am_choking() {'C'} else {'U'},
        )
    }
}
