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
    bitfield::PieceBitfield,
    torrent::{Event},
    bencode::BencodeValue,
    types::*,
};

const PEX_ID: u8 = 1;
const METADATA_ID: u8 = 2;

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choked(bool),
    Interested(bool),
    Bitfield(PieceBitfield),
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

    // BEP 11: https://www.bittorrent.org/beps/bep_0011.html#bep-40
    Pex {
        added: Vec<PeerInfo>,
        dropped: Vec<PeerEndpoint>,
    },

    // BEP 9: https://www.bittorrent.org/beps/bep_0009.html 
    MetadataRequest {
        index: usize,
    },
    MetadataReject {
        index: usize,
    },
    Metadata {
        index: usize,
        total_size: usize,
        piece: Vec<u8>,
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
            Self::MetadataRequest { index } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(0));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
                BencodeValue::Dictionary(root).to_bytes()
            }
            Self::Metadata { index, total_size, piece } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(0));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
                root.insert("total_size".to_string(), BencodeValue::Integer(total_size as i64));
                let mut ret = BencodeValue::Dictionary(root).to_bytes();
                ret.extend(piece);
                ret
            }
            Self::MetadataReject { index } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(2));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
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

    pub fn from_pex_bytes(encoded: &[u8]) -> Result<Self, String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        let mut added = Vec::new();
        let mut dropped = Vec::new();

        let flags4 = root.optional_bytes("added.f")?;
        let flags6 = root.optional_bytes("added6.f")?;

        added.extend(
            PeerEndpoint::from_compact_4(
                &root.required_bytes("added")?
            ).into_iter().enumerate().map(|(e, endpoint)| PeerInfo {
                endpoints: HashSet::from_iter(vec![endpoint]),
                id: None,
                flags: flags4.as_ref().map(|f| f[e]),
            })
        );

        added.extend(
            PeerEndpoint::from_compact_6(
                &root.required_bytes("added6")?
            ).into_iter().enumerate().map(|(e, endpoint)| PeerInfo {
                endpoints: HashSet::from_iter(vec![endpoint]),
                id: None,
                flags: flags6.as_ref().map(|f| f[e]),
            })
        );

        dropped.extend(
            PeerEndpoint::from_compact_4(
                &root.required_bytes("dropped")?
            )
        );

        dropped.extend(
            PeerEndpoint::from_compact_6(
                &root.required_bytes("dropped6")?
            )
        );

        Ok(Self::Pex {
            added,
            dropped,
        })
    }

    pub fn from_metadata_bytes(encoded: &[u8]) -> Result<Self, String> {
        let decoded = BencodeValue::from_bytes(encoded)?;
        let root = decoded.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        Ok(match root.required_unsigned("msg_type")? {
            0 => Self::MetadataRequest {
                index: root.required_unsigned("piece")? as usize,
            },
            1 => Self::Metadata {
                index: root.required_unsigned("piece")? as usize,
                total_size: root.required_unsigned("total_size")? as usize,
                piece: decoded.1.to_vec(),
            },
            2 => Self::MetadataRequest {
                index: root.required_unsigned("piece")? as usize,
            },
            _ => Self::Unsupported { type_byte: 20 + METADATA_ID },
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
        info_hash: &InfoHash,
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
        handshake[20 + 5] |= 0x10; // Enable extensions
        handshake[20 + 7] |= 0x04; // Enable fast
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

            while let Some(message) = rx.recv().await {
                Self::send(&mut writer, &name, message, write_request_times.clone(), write_extensions.clone()).await?;
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
        let mut log = String::new();
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

            Message::KeepAlive => {}
            Message::Unsupported { .. } => {}
            msg @ Message::ExtensionHandshake { .. } => {
                payload.push(20);
                payload.push(0);
                payload.extend(msg.into_bytes());
            }
            msg @ Message::MetadataRequest { .. } |
            msg @ Message::MetadataReject { .. } |
            msg @ Message::Metadata { .. } => {
                payload.push(20);
                payload.push(*extensions.lock().await.get("ut_metadata").unwrap());
                payload.extend(msg.into_bytes());
            }
            Message::Pex { .. } => {}
        }

        let length = u32::try_from(payload.len())?;
        writer.write_all(&length.to_be_bytes()).await?;
        writer.write_all(&payload).await?;
        Ok(())
    }

    fn check_length(expected: usize, actual: usize, at_least: bool, op: &str) -> Result<()> {
        anyhow::ensure!(
            if at_least {actual >= expected} else {actual == expected},
            "The length of a bitfield payloud should be{} {expected}, got {actual}",
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
                Self::check_length(2, message_length, true, "bitfield")?;
                let mut bitfield = vec![0u8; message_length - 1];
                reader.read_exact(&mut bitfield).await?;
                Ok(Message::Bitfield(PieceBitfield::from_vec(bitfield)))
            }
            4 => { // have
                Self::check_length(1 + 4, message_length, false, "have")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::Have { index })
            }
            6 => { // request
                Self::check_length(1 + 4 * 3, message_length, false, "request")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::Request { index, begin, length })
            }
            7 => { // piece
                Self::check_length(1 + 4 * 2, message_length, true, "piece")?;
                let (index, begin) = receive_index_begin(reader).await?;
                let mut piece = vec![0u8; message_length - 1 - 4 - 4];
                reader.read_exact(&mut piece).await?;

                let key = (index, begin, piece.len());
                let sent_at = request_times.lock().await
                    .remove(&key);
                let response_time = sent_at.map(|t| t.elapsed());

                Ok(Message::Piece { index, begin, piece, response_time })
            }
            8 => { // cancel
                Self::check_length(1 + 4 * 3, message_length, false, "cancel")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::Cancel { index, begin, length })
            }
            0 => {
                Self::check_length(1, message_length, false, "choke")?;
                Ok(Message::Choked(true))
            }
            1 => {
                Self::check_length(1, message_length, false, "unchoke")?;
                Ok(Message::Choked(false))
            }
            2 => {
                Self::check_length(1, message_length, false, "interested")?;
                Ok(Message::Interested(true))
            }
            3 => {
                Self::check_length(1, message_length, false, "uninterested")?;
                Ok(Message::Interested(false))
            }

            16 => { // reject
                Self::check_length(1 + 4 * 3, message_length, false, "reject")?;
                let (index, begin) = receive_index_begin(reader).await?;
                reader.read_exact(&mut int_buf).await?;
                let length = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::Reject { index, begin, length })
            }
            0x0E => { // HaveAll
                Self::check_length(1, message_length, false, "have all")?;
                Ok(Message::HaveAll)
            }
            0x0F => { // HaveNone
                Self::check_length(1, message_length, false, "have none")?;
                Ok(Message::HaveNone)
            }
            0x0D => { // suggest
                Self::check_length(1 + 4, message_length, false, "suggest")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::Suggest { index })
            }
            0x11 => { // allow fast
                Self::check_length(1 + 4, message_length, false, "allow fast")?;
                reader.read_exact(&mut int_buf).await?;
                let index = u32::from_be_bytes(int_buf) as usize;
                Ok(Message::AllowedFast { index })
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
                        Ok(msg)
                    }
                    PEX_ID => Message::from_pex_bytes(&payload[1..])
                        .map_err(|e| anyhow::anyhow!("{e}")),
                    METADATA_ID => Message::from_metadata_bytes(&payload[1..])
                        .map_err(|e| anyhow::anyhow!("{e}")),
                    _ => Ok(Message::Unsupported { type_byte: payload[0] + 20 }),
                }
            }

            _ => {
                let mut payload = vec![0u8; message_length - 1];
                reader.read_exact(&mut payload).await?;
                Ok(Message::Unsupported { type_byte: id_buf[0] })
            }
        };
        trace!(peer = %name, "Received {msg:?}");
        msg
    }
}
