use anyhow::Result;
use tokio::{
    sync::mpsc,
};
use std::{
    time::{Duration},
    collections::VecDeque,
    fmt,
};
use tracing::{debug, level_filters::STATIC_MAX_LEVEL};

use crate::{
    bitfield::Bitfield,
    proto::bit_torrent::{Message},
    util::*,
    types::*,
};

const UPLOAD_LIMIT: usize = 32 * 1024; // b/s per peer

const TIMEOUTS_BOOST_ALPHA: f64 = 0.4;
const TIMEOUTS_THROTTLE_ALPHA: f64 = 1.0;

const DEFAULT_MAX_REQUESTS: usize = 2;
const MIN_MAX_REQUESTS: usize = 1;
const MAX_MAX_REQUESTS: usize = 200;
const MAX_REQUESTS_STEP: usize = 2;

pub struct Connection {
    id: PeerId,
    
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

    uploaded_this_second: usize,

    downloaded_this_second: usize,
    response_times_sum: Duration,
    num_response_times: usize,
    chokes_this_second: usize,
    rejects_this_second: usize,
    timeouts_this_second: usize,

    timeouts_ema: f64,
}

impl Connection {
    pub fn new(id: PeerId, tx: mpsc::Sender<Message>, num_pieces: usize, supports_fast: bool) -> Self {
        Self {
            id,
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

            uploaded_this_second: 0,
            downloaded_this_second: 0,
            num_response_times: 0,
            response_times_sum: Duration::default(),
            chokes_this_second: 0,
            rejects_this_second: 0,
            timeouts_this_second: 0,

            timeouts_ema: 0.,
        }
    }

    pub fn can_request(&self) -> bool {
        self.sent_requests < self.max_requests && !self.peer_choking
    }

    pub fn can_upload(&self) -> bool {
        self.uploaded_this_second < UPLOAD_LIMIT && !self.am_choking
    }

    pub fn uploaded_this_second(&self) -> usize {self.uploaded_this_second}
    pub fn downloaded_this_second(&self) -> usize {self.downloaded_this_second}
    pub fn timeouts_this_second(&self) -> usize {self.timeouts_this_second}
    pub fn max_requests(&self) -> usize {self.max_requests}
    pub fn sent_requests(&self) -> usize {self.sent_requests}
    pub fn am_choking(&self) -> bool {self.am_choking}
    pub fn am_interested(&self) -> bool {self.am_interested}
    pub fn peer_choking(&self) -> bool {self.peer_choking}
    pub fn peer_interested(&self) -> bool {self.peer_interested}
    pub fn piece_bitfield(&self) -> &Bitfield {&self.piece_bitfield}

    pub fn boost_max_requests(&mut self) {
        self.max_requests = (self.max_requests + MAX_REQUESTS_STEP)
            .min(MAX_MAX_REQUESTS);
    }
    pub fn throttle_max_requests(&mut self) {
        self.max_requests = (self.max_requests * 3) / 4;
        if self.max_requests == 0 {
            self.max_requests = MIN_MAX_REQUESTS;
        }
    }

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
            if !self.supports_fast {
                self.reset_sent_requests();
            }
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
        self.timeouts_this_second += count;
        self.sub_sent_requests(count);
    }

    pub async fn send(&self, message: Message) {
        let _ = self.tx.send(message).await;
    }

    pub async fn request(&mut self, index: usize, begin: usize, length: usize) {
        self.send(Message::Request { index, begin, length }).await;
        self.sent_requests += 1;
    }

    pub fn receive_piece(&mut self, length: usize, response_time: Option<Duration>, is_downloading: bool) {
        if is_downloading {
            self.sub_sent_requests(1);
        }
        self.downloaded_this_second += length;
        if let Some(response_time) = response_time {
            self.response_times_sum += response_time;
            self.num_response_times += 1;
        }
    }

    pub async fn send_piece(&mut self, index: usize, begin: usize, piece: Vec<u8>) {
        self.uploaded_this_second += piece.len();
        self.send(Message::Piece {
            index,
            begin,
            piece,
            response_time: None,
        }).await;
    }

    fn control_congestion(&mut self) {
        let alpha = if self.timeouts_this_second as f64 > self.timeouts_ema {
            TIMEOUTS_BOOST_ALPHA
        } else {
            TIMEOUTS_THROTTLE_ALPHA
        };
        self.timeouts_ema = alpha * self.timeouts_this_second as f64
            + (1. - alpha) * self.timeouts_ema;

        if self.timeouts_ema >= 1. {
            if !self.peer_choking {
                self.throttle_max_requests();
            }
        } else if self.downloaded_this_second != 0 {
            self.boost_max_requests();
        }
    }

    pub fn reset_stats(&mut self) {
        self.control_congestion();

        self.uploaded_this_second = 0;
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
        write!(f, "{:#10} │ {} {} {:>4} {:>3} {:>3} {:>9}/s {:>8.2} {:>4} {:>4} {:>4} │ {} {} {:>9}/s",
            self.piece_bitfield(),
            if self.am_interested() {'■'} else {'□'},
            if self.peer_choking() {'■'} else {'□'},
            self.piece_cursor.map(|c| format!("{c}")).unwrap_or(String::new()),
            self.max_requests(),
            self.sent_requests(),
            pretty_size(self.downloaded_this_second()),
            self.response_times_sum.as_secs_f64() * 1000.
                / self.num_response_times as f64,
            self.timeouts_this_second,
            self.chokes_this_second,
            self.rejects_this_second,
            if self.peer_interested() {'■'} else {'□'},
            if self.am_choking() {'■'} else {'□'},
            pretty_size(self.uploaded_this_second()),
        )
    }
}

