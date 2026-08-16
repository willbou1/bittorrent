use reqwest::Url;
use anyhow::Result;
use std::io::Read;
use std::fmt;

use crate::bencode::BencodeValue;

pub enum Event {
    started,
    completed,
    stopped,
}

impl Event {
    fn to_string(&self) -> &str {
        match self {
            Self::started => "started",
            Self::stopped => "stopped",
            Self::completed => "completed",
        }
    }
}

pub struct PeerInfo {
    pub id: Option<[u8; 20]>,
    pub host: String,
    pub port: u16,
}

pub struct Response {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub complete: Option<u64>,
    pub incomplete: Option<u64>,
    pub peers: Vec<PeerInfo>,
}

impl PeerInfo {
    fn from_bencode(root: &BencodeValue) -> Result<Vec<Self>, String> {
        match root {
            BencodeValue::List(peers) => {
                peers.iter().map(|peer| Ok(Self {
                    id: Some(peer.required_bytes("peer id")?.try_into().map_err(
                        |_| "'id' needs to be 20 bytes long"
                    )?),
                    host: peer.required_string("ip")?,
                    port: peer.required_unsigned("port")?.try_into().map_err(
                        |_| ""
                    )?,
                }))
                    .collect::<Result<_, _>>()
            },
            BencodeValue::ByteString(peers) => {
                Ok(peers.chunks(6)
                    .map(|peer| Self {
                        id: None,
                        host: peer[0..4].iter().map(u8::to_string)
                            .collect::<Vec<_>>().join("."),
                        port: ((peer[4] as u16) << 8) | peer[5] as u16,
                    })
                    .collect())
            },
            _ => Err(format!("'peers' must be either a list or a byte string")),
        }
    }
}

impl Response {
    fn from_bytes(encoded: &[u8]) -> Result<Self, String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        Ok(Self {
            interval: root.required_unsigned("interval")?,
            min_interval: root.optional_unsigned("interval")?,
            complete: root.optional_unsigned("complete")?,
            incomplete: root.optional_unsigned("incomplete")?,
            peers: PeerInfo::from_bencode(root.required("peers")?)?,
        })
    }
}

pub fn tracker_request(
    url: &str,
    info_hash: &[u8; 20],
    uploaded: usize,
    downloaded: usize,
    left: usize,
    event: Event,
) -> Result<Response> {
    let mut url = Url::parse(url)?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("peer_id", "aaaaaaaaaaaaaaaaaaaa");
        query.append_pair("port", "6881");
        query.append_pair("uploaded", &uploaded.to_string());
        query.append_pair("downloaded", &downloaded.to_string());
        query.append_pair("left", &left.to_string());
        query.append_pair("event", &event.to_string());
    }

    let info_hash = info_hash
        .iter() .map(|b| format!("%{b:02X}"))
        .collect::<String>();

    let url = format!("{url}&info_hash={info_hash}");

    let mut res = reqwest::blocking::get(url)?;
    let mut body = vec![];
    res.read_to_end(&mut body)?;
    let response = Response::from_bytes(&body)
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;

    Ok(response)
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)?;
        if let Some(id) = self.id {
            write!(
                f,
                " ({:02x}{:02x}{:02x}{:02x}...{:02x}{:02x}{:02x}{:02x})",
                id[0], id[1], id[2], id[3], id[16], id[17], id[18], id[19],
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Interval: {}", self.interval)?;
        if let Some(min_interval) = self.min_interval {
            writeln!(f, "Minimum interval: {}", min_interval)?;
        }
        if let Some(complete) = self.complete {
            writeln!(f, "Complete: {}", complete)?;
        }
        if let Some(incomplete) = self.incomplete {
            writeln!(f, "Incomplete: {}", incomplete)?;
        }
        writeln!(f, "Peers:")?;
        for peer in &self.peers {
            writeln!(f, "    {}", peer)?;
        }
        Ok(())
    }
}
