use tokio::{
    sync::mpsc,
};
use std::{
    time::{Duration, Instant},
};
use tracing::{warn, debug};

use crate::{
    proto::bit_torrent::Message,
    types::*,
};

const MAX_METADATA_REQUESTS: usize = 2;
const MIN_RECONNECTION_INTERVAL: Duration = Duration::from_secs(5);

pub enum PeerState {
    Disconnected {
        at: Instant,
        interval: Duration,
        reconnecting: bool,
        reason: anyhow::Error,
    },
    Connected {
        at: Instant,
        sent_metadata_requests: usize,
        tx: mpsc::Sender<Message>,
    },
}

impl PeerState {
    pub fn disconnect(&mut self, reason: anyhow::Error) {
        *self = Self::Disconnected {
            at: Instant::now(),
            interval: MIN_RECONNECTION_INTERVAL,
            reconnecting: false,
            reason,
        }
    }

    pub fn reconnect(&mut self, tx: mpsc::Sender<Message>) {
        *self = Self::Connected {
            at: Instant::now(),
            sent_metadata_requests: 0,
            tx
        }
    }

    pub fn back_off(&mut self) {
        if let Self::Disconnected { interval, reconnecting, at, .. } = self {
            *interval *= 2;
            *reconnecting = false;
            *at = Instant::now();
        }
    }
}

pub struct Peer {
    pub state: PeerState,
    pub info: PeerInfo,
    pub client: Option<String>,
    pub supports_fast: bool,
    pub supports_pex: bool,
    pub supports_metadataa: bool,
    pub supports_dht: bool,
}

impl Peer {
    pub fn new(info: PeerInfo, tx: mpsc::Sender<Message>, supports_fast: bool, supports_dht: bool) -> Self {
        Self {
            client: None,
            info,
            supports_fast,
            supports_dht,
            supports_metadataa: false,
            supports_pex: false,
            state: PeerState::Connected {
                at: Instant::now(),
                sent_metadata_requests: 0,
                tx
            },
        }
    }

    pub fn can_request_metadata(&self) -> bool {
        if let PeerState::Connected {sent_metadata_requests, ..} = &self.state {
            return *sent_metadata_requests < MAX_METADATA_REQUESTS;
        }
        false
    }

    pub fn decrement_metadata_requests(&mut self, count: usize) {
        if let PeerState::Connected {sent_metadata_requests, ..} = &mut self.state {
            *sent_metadata_requests = sent_metadata_requests.saturating_sub(count);
        }
    }

    pub async fn send(&self, msg: Message) {
        if let PeerState::Connected {tx, ..} = &self.state {
            if let Err(_) = tx.send(msg).await {
                debug!("Tried to send message to a closed channel");
            }
        }
    }
}
