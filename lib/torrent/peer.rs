use reqwest::header::CONNECTION;
use tokio::{
    sync::mpsc,
};
use std::{
    time::{Duration, Instant},
    collections::{HashMap, VecDeque},
};
use tracing::{warn, debug};

use crate::{
    proto::{bit_torrent::Message, metadata::MetadataMessage},
    types::*,
};

const INITIAL_TRANSFER_MESSAGES_CAPACITY: usize = 5;

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
        initial_transfer_messages: VecDeque<Message>,
        received_extension_handshake: bool,
    },
}

impl PeerState {
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected {..})
    }
    
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
            initial_transfer_messages: VecDeque::with_capacity(INITIAL_TRANSFER_MESSAGES_CAPACITY),
            at: Instant::now(),
            sent_metadata_requests: 0,
            received_extension_handshake: false,
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
    pub supports_metadata: bool,
    pub supports_dht: bool,

    downloaded_metadata_this_second: usize,
    uploaded_metadata_this_second: usize,
}

impl Peer {
    pub fn new(info: PeerInfo, tx: mpsc::Sender<Message>, supports_fast: bool, supports_dht: bool) -> Self {
        Self {
            client: None,
            info,
            supports_fast,
            supports_dht,
            supports_metadata: false,
            supports_pex: false,
            uploaded_metadata_this_second: 0,
            downloaded_metadata_this_second: 0,
            state: PeerState::Connected {
                initial_transfer_messages: VecDeque::with_capacity(INITIAL_TRANSFER_MESSAGES_CAPACITY),
                at: Instant::now(),
                sent_metadata_requests: 0,
                received_extension_handshake: false,
                tx
            },
        }
    }

    pub fn uploaded_metadata_this_second(&self) -> usize {self.uploaded_metadata_this_second}
    pub fn downloaded_metadata_this_second(&self) -> usize {self.downloaded_metadata_this_second}

    pub fn reset_statistics(&mut self) {
        self.uploaded_metadata_this_second = 0;
        self.downloaded_metadata_this_second = 0;
    }

    pub fn can_request_metadata(&self) -> bool {
        if let PeerState::Connected {sent_metadata_requests, received_extension_handshake, ..} = &self.state {
            return *sent_metadata_requests < MAX_METADATA_REQUESTS
                && *received_extension_handshake;
        }
        false
    }

    pub fn sub_sent_metadata_requests(&mut self, count: usize) {
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

    pub async fn request_metadata(&mut self, index: usize) {
        self.send(Message::Metadata(
            MetadataMessage::Request { index }
        )).await;
        if let PeerState::Connected {sent_metadata_requests, ..} = &mut self.state {
            *sent_metadata_requests += 1;
        }
    }
    pub async fn send_metadata(&mut self, index: usize, metadata_size: usize, piece: Vec<u8>) {
        self.uploaded_metadata_this_second += 1;
        self.send(Message::Metadata(
            MetadataMessage::Data { index, total_size: metadata_size, piece }
        )).await;
    }
    pub async fn send_metadata_reject(&mut self, index: usize) {
        self.send(Message::Metadata(
            MetadataMessage::Reject {index}
        )).await;
    }

    pub fn reject_metadata(&mut self) {
        self.sub_sent_metadata_requests(1);
    }
    pub fn timeout_metadata(&mut self, count: usize) {
        self.sub_sent_metadata_requests(count);
    }
    pub fn receive_metadata(&mut self, downloading: bool) {
        self.downloaded_metadata_this_second += 1;
        if downloading {
            self.sub_sent_metadata_requests(1);
        }
    }

    pub fn queue_transfer_message(&mut self, message: Message) {
        if let PeerState::Connected {initial_transfer_messages, ..} = &mut self.state {
            initial_transfer_messages.push_back(message);
        }
    }

    pub fn receive_extension_handshake(&mut self, client: Option<String>, extensions: HashMap<String, u8>) {
        self.client = client;
        self.supports_metadata |= extensions.contains_key("ut_metadata");
        self.supports_pex |= extensions.contains_key("ut_pex");
        if let PeerState::Connected {received_extension_handshake, ..} = &mut self.state {
            *received_extension_handshake = true;
        }
    }
}

fn is_disconnection_boring(error: anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>()
        .is_some_and(|e| matches!(e.kind(),
            std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
        ))
}
