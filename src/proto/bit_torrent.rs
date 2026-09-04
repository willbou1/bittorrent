use anyhow::{Result, anyhow};
use tokio::{
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    io::{AsyncWriteExt, AsyncReadExt},
    sync::mpsc,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Instant, Duration},
};
use tracing::{trace, warn};

use crate::{
    bitfield::Bitfield,
    torrent::{Event},
    bencode::BencodeValue,
    types::*,
};
use super::{
    metadata::{MetadataMessage},
    pex::PEXMessage,
};

const PEX_ID: u8 = 1;
const METADATA_ID: u8 = 2;

#[derive(Debug)]
pub enum Message {
    // TODO send keep alive periodically
    KeepAlive,

    Choked(bool),
    Interested(bool),
    Bitfield(Bitfield),
    Have {
        index: usize,
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
        response_time: Option<Duration>,
    },

    // BEP 6: https://www.bittorrent.org/beps/bep_0006.html
    Reject {
        index: usize,
        begin: usize,
        length: usize,
    },
    Suggest {
        index: usize,
    },
    AllowedFast {
        index: usize,
    },
    HaveAll,
    HaveNone,

    // BEP 10: https://bittorrent.org/beps/bep_0010.html
    ExtensionHandshake {
        extensions: HashMap<String, u8>,
        client: Option<String>,
        max_requests: Option<usize>,
        metadata_size: Option<usize>,
    },
    PEX(PEXMessage),
    Metadata(MetadataMessage),

    // BEP 5: https://bittorrent.org/beps/bep_0005.html
    DHTPort {
        port: u16,
    },

    UnsupportedExtension {
        type_byte: u8,
    },
    Unsupported {
        type_byte: u8,
    },
}

impl Message {
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::ExtensionHandshake { extensions, client, max_requests, metadata_size } => {
                let mut root = HashMap::new();
                if let Some(client) = client {
                    root.insert("v".to_string(),
                        BencodeValue::ByteString(client.into_bytes())
                    );
                }
                if let Some(max_requests) = max_requests {
                    root.insert("reqq".to_string(),
                        BencodeValue::Integer(max_requests as i64)
                    );
                }
                if let Some(metadata_size) = metadata_size {
                    root.insert("metadata_size".to_string(),
                        BencodeValue::Integer(metadata_size as i64)
                    );
                }
                root.insert("m".to_string(), BencodeValue::Dictionary(
                    extensions.iter()
                        .map(|(name, id)| (name.clone(), BencodeValue::Integer(*id as i64)))
                        .collect()
                ));
                BencodeValue::Dictionary(root).to_bytes()
            }
            _ => Vec::new(),
        }
    }

    pub fn from_extension_handshake_bytes(encoded: &[u8]) -> Result<Self, String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        Ok(Self::ExtensionHandshake {
            extensions: root.required("m")?.as_dict().ok_or_else(|| "'m' must be a dictionary")?
                .iter().map(
                    |(name, id)| Ok((name.clone(), id.unsigned(name)? as u8))
                ).collect::<Result<HashMap<_, _>, String>>()?,
            client: root.optional_string("v")?,
            max_requests: root.optional_unsigned("reqq")?.map(|m| m as usize),
            metadata_size: root.optional_unsigned("metadata_size")?.map(|s| s as usize),
        })
    }
}

type RequestKey = (usize, usize, usize);
type RequestTimes = Arc<tokio::sync::Mutex<HashMap<RequestKey, Instant>>>;
type ExtensionMapping = Arc<tokio::sync::Mutex<HashMap<String, u8>>>;

pub struct BitTorrent {
    pub stream: TcpStream,
    pub name: String,
    pub id: PeerId,
    pub supports_fast: bool,
    pub supports_dht: bool,
}

impl BitTorrent {
    pub async fn handshake(
        peer_info: &PeerInfo,
        info_hash: &Hash,
        local_id: &PeerId,
    ) -> Result<Self> {
        let mut handshake = [0u8; 68];
        let name = format!("{peer_info}");

        let addrs = peer_info.resolve().await?;

        let mut stream = TcpStream::connect(
            addrs.iter().next().unwrap()
        ).await?;
        trace!(peer = %name, "Connected");

        // Send the handshake
        handshake[0] = 19;
        handshake[1..20].copy_from_slice(b"BitTorrent protocol");
        handshake[20 + 5] |= 0x10; // Advertise extensions
        handshake[20 + 7] |= 0x04; // Advertise fast
        handshake[20 + 7] |= 0x01; // Avertise DHT
        handshake[28..48].copy_from_slice(info_hash.as_bytes());
        handshake[48..68].copy_from_slice(local_id.as_bytes());
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
            &handshake[28..48] == info_hash.as_bytes(),
            "expected handshake response to have same info hash"
        );
        if let Some(peer_id) = peer_info.id {
            anyhow::ensure!(
                &handshake[48..68] == peer_id.as_bytes(),
                "expected handshake response to have right peer id"
            );
        }
        trace!(peer = %name, "Received handshake");

        Ok(Self {
            stream,
            name,
            id: PeerId::from(handshake[48..68].try_into()?),
            supports_fast: handshake[20 + 7] & 0x04 > 0,
            supports_dht: handshake[20 + 7] & 0x01 > 0,
        })
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<Event>,
        mut rx: mpsc::Receiver<Message>,
    ) {
        let BitTorrent {
            stream,
            name,
            id,
            ..
        } = self;
        
        let request_times = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let extensions = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let read_request_times = request_times.clone();
        let write_request_times = request_times.clone();
        let read_extensions = extensions.clone();
        let write_extensions = extensions.clone();

        let (mut reader, mut writer) = stream.into_split();
        
        let read_tx = tx.clone();
        let read_name = name.clone();
        
        let reader_task = async move {
            loop {
                match Self::receive(&mut reader, &read_name, read_request_times.clone(), read_extensions.clone()).await {
                    Ok(message) => {
                        if read_tx.send(Event::Message(id, message)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        };
        
        let writer_task = async move {
            // Send extension handshake
            Self::send(&mut writer, &name, Message::ExtensionHandshake {
                extensions: HashMap::from_iter(vec![
                    (String::from("ut_pex"), PEX_ID),
                    (String::from("ut_metadata"), METADATA_ID),
                ]),
                client: Some(String::from("custom")),
                max_requests: Some(70),
                metadata_size: None,
            }, write_request_times.clone(), write_extensions.clone()).await?;

            let sleep = tokio::time::sleep(Duration::from_secs(60 * 2));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    message = rx.recv() => {
                        match message {
                            Some(message) => {
                                Self::send(&mut writer, &name, message, write_request_times.clone(), write_extensions.clone()).await?;
                                sleep.as_mut().reset(
                                    tokio::time::Instant::now() + Duration::from_secs(60 * 2)
                                );
                            }
                            _ => break,
                        }
                    }
                    _ = &mut sleep => {
                        Self::send(&mut writer, &name, Message::KeepAlive, write_request_times.clone(), write_extensions.clone()).await?;
                        sleep.as_mut().reset(
                            tokio::time::Instant::now() + Duration::from_secs(60 * 2)
                        );
                    }
                }
            }
            
            Ok::<_, anyhow::Error>(())
        };
        
        tokio::select! {
            result = reader_task => {
                if let Err(e) = result {
                    let _ = tx.send(Event::Disconnection(id, e)).await;
                }
            }
            result = writer_task => {
                if let Err(e) = result {
                    let _ = tx.send(Event::Disconnection(id, e)).await;
                }
            }
        }
    }

    async fn send(
        writer: &mut OwnedWriteHalf,
        name: &str,
        message: Message,
        request_times: RequestTimes,
        extensions: ExtensionMapping,
    ) -> Result<()> {
        let mut payload = Vec::new();

        fn send_index_begin(payload: &mut Vec<u8>, index: usize, begin: usize) {
            payload.extend((index as u32).to_be_bytes());
            payload.extend((begin as u32).to_be_bytes());
        }

        trace!(peer = %name, "Sent {message:?}");
        match message {
            Message::Bitfield(bitfield) => {
                payload.push(5);
                payload.extend(bitfield.as_bytes());
            }
            Message::Have { index } => {
                payload.push(4);
                payload.extend((index as u32).to_be_bytes());
            }
            Message::Request{ index, begin, length } => {
                payload.push(6);
                send_index_begin(&mut payload, index, begin);
                payload.extend((length as u32).to_be_bytes());
                request_times.lock().await
                    .insert((index, begin, length), Instant::now());
            }
            Message::Cancel{ index, begin, length } => {
                payload.push(8);
                send_index_begin(&mut payload, index, begin);
                payload.extend((length as u32).to_be_bytes());
            }
            Message::Piece{ index, begin, piece, .. } => {
                payload.push(7);
                send_index_begin(&mut payload, index, begin);
                let piece_length = piece.len();
                payload.extend(piece);
            }
            Message::Choked(choked) => {
                payload.push(if choked {0} else {1});
            }
            Message::Interested(interested) => {
                payload.push(if interested {2} else {3});
            }

            // extensions
            Message::Reject{ index, begin, length } => {
                payload.push(16);
                send_index_begin(&mut payload, index, begin);
                payload.extend((length as u32).to_be_bytes());
            }
            Message::HaveAll => {
                payload.push(0x0E);
            }
            Message::HaveNone => {
                payload.push(0x0F);
            }
            Message::Suggest { index } => {
                payload.push(0x0D);
                payload.extend((index as u32).to_be_bytes());
            }
            Message::AllowedFast { index } => {
                payload.push(0x11);
                payload.extend((index as u32).to_be_bytes());
            }

            Message::DHTPort { port } => {
                payload.push(0x09);
                payload.extend(port.to_be_bytes());
            }

            Message::KeepAlive => {}
            Message::Unsupported { .. } => {}
            Message::UnsupportedExtension { .. } => {}
            msg @ Message::ExtensionHandshake { .. } => {
                payload.push(20);
                payload.push(0);
                payload.extend(msg.into_bytes());
            }
            Message::Metadata(msg) => {
                payload.push(20);
                payload.push(*extensions.lock().await.get("ut_metadata").unwrap());
                payload.extend(msg.into_bytes());
            }
            Message::PEX { .. } => {}
        }

        let length = u32::try_from(payload.len())?;
        writer.write_all(&length.to_be_bytes()).await?;
        writer.write_all(&payload).await?;
        Ok(())
    }

    fn check_length(expected: usize, actual: usize, at_least: bool, op: &str) -> Result<()> {
        anyhow::ensure!(
            if at_least {actual >= expected} else {actual == expected},
            "The length of a {op} payloud should be{} {expected}, got {actual}",
            if at_least {" at least "} else {""}
        );
        Ok(())
    }


    async fn receive(
        reader: &mut OwnedReadHalf,
        name: &str,
        request_times: RequestTimes,
        extensions: ExtensionMapping,
    ) -> Result<Message> {
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
        let msg = match id_buf[0] {
            5 => { // bitfield
                Self::check_length(2, message_length, true, "Bitfield")?;
                let mut bitfield = vec![0u8; message_length - 1];
                reader.read_exact(&mut bitfield).await?;
                Message::Bitfield(Bitfield::from_vec(bitfield))
            }
            4 => { // have
                Self::check_length(1 + 4, message_length, false, "Have")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Message::Have { index }
            }
            6 => { // request
                Self::check_length(1 + 4 * 3, message_length, false, "Request")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Message::Request { index, begin, length }
            }
            7 => { // piece
                Self::check_length(1 + 4 * 2, message_length, true, "Piece")?;
                let (index, begin) = receive_index_begin(reader).await?;
                let mut piece = vec![0u8; message_length - 1 - 4 - 4];
                reader.read_exact(&mut piece).await?;

                let key = (index, begin, piece.len());
                let sent_at = request_times.lock().await
                    .remove(&key);
                let response_time = sent_at.map(|t| t.elapsed());

                Message::Piece { index, begin, piece, response_time }
            }
            8 => { // cancel
                Self::check_length(1 + 4 * 3, message_length, false, "Cancel")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Message::Cancel { index, begin, length }
            }
            0 => {
                Self::check_length(1, message_length, false, "Choke")?;
                Message::Choked(true)
            }
            1 => {
                Self::check_length(1, message_length, false, "Unchoke")?;
                Message::Choked(false)
            }
            2 => {
                Self::check_length(1, message_length, false, "Interested")?;
                Message::Interested(true)
            }
            3 => {
                Self::check_length(1, message_length, false, "Uninterested")?;
                Message::Interested(false)
            }

            16 => { // reject
                Self::check_length(1 + 4 * 3, message_length, false, "Reject")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Message::Reject { index, begin, length }
            }
            0x0E => { // HaveAll
                Self::check_length(1, message_length, false, "HaveAll")?;
                Message::HaveAll
            }
            0x0F => { // HaveNone
                Self::check_length(1, message_length, false, "HaveNone")?;
                Message::HaveNone
            }
            0x0D => { // suggest
                Self::check_length(1 + 4, message_length, false, "suggest")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Message::Suggest { index }
            }
            0x11 => { // allow fast
                Self::check_length(1 + 4, message_length, false, "AllowFast")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Message::AllowedFast { index }
            }

            0x09 => { // DHTPort
                Self::check_length(1 + 2, message_length, false, "DHTPort")?;
                reader.read_exact(&mut int_buf[0..2]).await?;
                let port = u16::from_be_bytes(int_buf[0..2].try_into().unwrap());
                Message::DHTPort { port }
            }

            // extensions
            20 => {
                let mut payload = vec![0u8; message_length - 1];
                reader.read_exact(&mut payload).await?;
                trace!(peer = %name,
                    "Received extension {{id: {}}}",
                    payload[0],
                );
                match payload[0] {
                    0 => {
                        let msg = Message::from_extension_handshake_bytes(&payload[1..])
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        if let Message::ExtensionHandshake { extensions: new, .. } = &msg {
                            extensions.lock().await.extend(new.clone());
                        }
                        msg
                    }
                    PEX_ID => Message::PEX(
                        PEXMessage::from_bytes(&payload[1..])
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                    ),
                    METADATA_ID => Message::Metadata(
                        MetadataMessage::from_bytes(&payload[1..])
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                    ),
                    _ => {
                        let mut payload = vec![0u8; message_length - 1];
                        reader.read_exact(&mut payload).await?;
                        Message::UnsupportedExtension { type_byte: payload[0] }
                    }
                }
            }

            _ => {
                let mut payload = vec![0u8; message_length - 1];
                reader.read_exact(&mut payload).await?;
                Message::Unsupported { type_byte: id_buf[0] }
            }
        };
        trace!(peer = %name, "Received {msg:?}");
        Ok(msg)
    }
}
