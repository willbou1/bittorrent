use anyhow::Result;
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tracing::trace;

use crate::{
    tracker::PeerInfo,
    bitfield::PieceBitfield,
    torrent::{Event},
};

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choked(bool),
    Interested(bool),
    Bitfield(PieceBitfield),
    Have(usize),
    Reject {
        index: usize,
        begin: usize,
        length: usize,
    },
    Request {
        index: usize,
        begin: usize,
        length: usize,
    },
    Cancel {
        index: usize,
        begin: usize,
        length: usize,
    },
    Piece {
        index: usize,
        begin: usize,
        piece: Vec<u8>,
    },
    Unsupported(u8),
}

pub type PeerId = [u8; 20];

pub struct Peer {
    pub stream: TcpStream,
    pub name: String,
    pub id: PeerId,
}

impl Peer {
    pub async fn handshake(
        peer_info: &PeerInfo,
        info_hash: &[u8; 20],
        local_id: &PeerId,
    ) -> Result<Self> {
        let mut handshake = [0u8; 68];
        let name = format!("{peer_info}");

        let mut stream = TcpStream::connect(
            format!("{}:{}", peer_info.host, peer_info.port)
        ).await?;
        trace!(peer = %name, "Connected");

        // Send the handshake
        handshake[0] = 19;
        handshake[1..20].copy_from_slice(b"BitTorrent protocol");
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(info_hash);
        handshake[48..68].copy_from_slice(local_id);
        stream.write_all(&handshake).await?;
        trace!(peer = %name, "Sent handshake");

        // Receive the handshake response and verify it is right
        stream.read_exact(&mut handshake).await?;
        anyhow::ensure!(
            handshake[0] == 19,
            "expected handshake response to start with 19"
        );
        anyhow::ensure!(
            &handshake[1..20] == b"BitTorrent protocol",
            "expected handshake response to have 'BitTorrent protocol'"
        );
        anyhow::ensure!(
            &handshake[28..48] == info_hash,
            "expected handshake response to have same info hash"
        );
        if let Some(peer_id) = peer_info.id {
            anyhow::ensure!(
                handshake[48..68] == peer_id,
                "expected handshake response to have right peer id"
            );
        }
        trace!(peer = %name, "Received handshake");

        Ok(Self {
            stream,
            name,
            id: handshake[48..68].try_into()?,
        })
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<Event>,
        mut rx: mpsc::Receiver<Message>,
    ) -> Result<()> {
        let Peer {
            stream,
            name,
            id,
        } = self;
        let (mut reader, mut writer) = stream.into_split();

        loop {
            tokio::select! {
                message = rx.recv() => {
                    match message {
                        Some(message) => {
                            if let Err(e) = send(&mut writer, &name, message).await {
                                let _ = tx.send(Event::PeerDisconnection(id)).await;
                                return Err(e.into());
                            }
                        }
                        None => return Ok(()),
                    }
                }
                
                result = receive(&mut reader, &name) => {
                    match result {
                        Ok(message) => {
                            tx.send(Event::PeerMessage(id, message)).await?;
                        }
                        Err(e) => {
                            let _ = tx.send(Event::PeerDisconnection(id)).await;
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    }
}

async fn send(writer: &mut OwnedWriteHalf, name: &str, message: Message) -> Result<()> {
    let mut log = String::new();
    let mut payload = Vec::new();

    fn send_index_begin(payload: &mut Vec<u8>, index: usize, begin: usize) {
        payload.extend((index as u32).to_be_bytes());
        payload.extend((begin as u32).to_be_bytes());
    }

    match message {
        Message::Bitfield(bitfield) => {
            payload.push(5);
            payload.extend(bitfield.as_bytes());
            log = format!("Sent bitfield {:?}", bitfield.as_bytes());
        }
        Message::Have(have) => {
            payload.push(4);
            payload.extend((have as u32).to_be_bytes());
            log = format!("Sent have {have}");
        }
        Message::Request{ index, begin, length } => {
            payload.push(6);
            send_index_begin(&mut payload, index, begin);
            payload.extend((length as u32).to_be_bytes());
            log = format!(
                "Sent request {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
        }
        Message::Cancel{ index, begin, length } => {
            payload.push(8);
            send_index_begin(&mut payload, index, begin);
            payload.extend((length as u32).to_be_bytes());
            log = format!(
                "Sent cancel {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
        }
        Message::Piece{ index, begin, piece } => {
            payload.push(7);
            send_index_begin(&mut payload, index, begin);
            let piece_length = piece.len();
            payload.extend(piece);
            log = format!(
                "Sent piece {{index: {}, begin: {}, length: {}}}",
                index, begin, piece_length
            );
        }
        Message::Choked(choked) => {
            payload.push(if choked {0} else {1});
            log = format!("Sent choke of {choked}");
        }
        Message::Interested(interested) => {
            payload.push(if interested {2} else {3});
            log = format!("Sent interest of {interested}");
        }

        // extensions
        Message::Reject{ index, begin, length } => {
            payload.push(8);
            send_index_begin(&mut payload, index, begin);
            payload.extend((length as u32).to_be_bytes());
            log = format!(
                "Sent reject {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
        }

        Message::KeepAlive => {}
        Message::Unsupported(_) => {}
    }

    let length = u32::try_from(payload.len())?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    trace!(peer = %name, "{log}");
    Ok(())
}

async fn receive(reader: &mut OwnedReadHalf, name: &str) -> Result<Message> {
    let mut int_buf = [0u8; 4];
    reader.read_exact(&mut int_buf).await?;
    let message_length = u32::from_be_bytes(int_buf) as usize;

    if message_length == 0 {
        return Ok(Message::KeepAlive);
    }

    async fn receive_index_begin(reader: &mut OwnedReadHalf) -> Result<(usize, usize)> {
        let mut int_buf = [0u8; 4];
        reader.read_exact(&mut int_buf).await?;
        let index = u32::from_be_bytes(int_buf) as usize;
        reader.read_exact(&mut int_buf).await?;
        let begin = u32::from_be_bytes(int_buf) as usize;
        Ok((index, begin))
    }

    let mut id_buf = [0u8; 1];
    reader.read_exact(&mut id_buf).await?;
    match id_buf[0] {
        5 => { // bitfield
            anyhow::ensure!(
                message_length >= 2,
                "The length of a bitfield payloud should be at least 2, got {message_length}"
            );
            let mut bitfield = vec![0u8; message_length - 1];
            reader.read_exact(&mut bitfield).await?;
            trace!(peer = %name, "Received bitfield {:?}", bitfield);
            Ok(Message::Bitfield(PieceBitfield::from_vec(bitfield)))
        }
        4 => { // have
            anyhow::ensure!(
                message_length == 1 + 4,
                "The length of an have payloud should be 5, got {message_length}"
            );
            reader.read_exact(&mut int_buf).await?;
            let index = u32::from_be_bytes(int_buf) as usize;
            trace!(peer = %name, "Received have {:?}", index);
            Ok(Message::Have(index))
        }
        6 => { // request
            anyhow::ensure!(
                message_length == 1 + 4 + 4 + 4,
                "The length of a request payloud should be 13, got {message_length}"
            );
            let (index, begin) = receive_index_begin(reader).await?;
            reader.read_exact(&mut int_buf).await?;
            let length = u32::from_be_bytes(int_buf) as usize;
            trace!(peer = %name,
                "Received request {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
            Ok(Message::Request { index, begin, length })
        }
        7 => { // piece
            anyhow::ensure!(
                message_length > 1 + 4 + 4,
                "The length of a piece payloud should be at least 10, got {message_length}"
            );
            let (index, begin) = receive_index_begin(reader).await?;
            let mut piece = vec![0u8; message_length - 1 - 4 - 4];
            reader.read_exact(&mut piece).await?;
            trace!(peer = %name,
                "Received piece {{index: {}, begin: {}, length: {}}}",
                index, begin, piece.len()
            );
            Ok(Message::Piece { index, begin, piece })
        }
        8 => { // cancel
            anyhow::ensure!(
                message_length == 1 + 4 + 4 + 4,
                "The length of a cancel payloud should be 13, got {message_length}"
            );
            let (index, begin) = receive_index_begin(reader).await?;
            reader.read_exact(&mut int_buf).await?;
            let length = u32::from_be_bytes(int_buf) as usize;
            trace!(peer = %name,
                "Received cancel {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
            Ok(Message::Cancel { index, begin, length })
        }
        0 => {
            anyhow::ensure!(
                message_length == 1,
                "The length of a choke payload should be 1, got {message_length}"
            );
            trace!(peer = %name, "Received choke");
            Ok(Message::Choked(true))
        }
        1 => {
            anyhow::ensure!(
                message_length == 1,
                "The length of an unchoke payload should be 1, got {message_length}"
            );
            trace!(peer = %name, "Received unchoke");
            Ok(Message::Choked(false))
        }
        2 => {
            anyhow::ensure!(
                message_length == 1,
                "The length of an intereseted payload should be 1, got {message_length}"
            );
            trace!(peer = %name, "Received interested");
            Ok(Message::Interested(true))
        }
        3 => {
            anyhow::ensure!(
                message_length == 1,
                "The length of an uninterested payload should be 1, got {message_length}"
            );
            trace!(peer = %name, "Received uninterested");
            Ok(Message::Interested(false))
        }

        // extensions
        16 => { // reject
            anyhow::ensure!(
                message_length == 1 + 4 + 4 + 4,
                "The length of a reject payloud should be 13, got {message_length}"
            );
            let (index, begin) = receive_index_begin(reader).await?;
            reader.read_exact(&mut int_buf).await?;
            let length = u32::from_be_bytes(int_buf) as usize;
            trace!(peer = %name,
                "Received reject {{index: {}, begin: {}, length: {}}}",
                index, begin, length
            );
            Ok(Message::Reject { index, begin, length })
        }

        _ => {
            let mut payload = vec![0u8; message_length - 1];
            reader.read_exact(&mut payload).await?;
            trace!(peer = %name, "Unsopported message {}", id_buf[0]);
            Ok(Message::Unsupported(id_buf[0]))
        }
    }
}
