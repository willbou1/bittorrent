use anyhow::Result;
use rand::Rng;
use std::{
    collections::HashSet, fmt::{self, write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv6Addr}
};

use crate::{
    bencode::BencodeValue,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn from_xt(xt: &str) -> Result<Self> {
        let bytes: [u8; 20] = hex::decode(
            xt.strip_prefix("urn:btih:").ok_or(
                anyhow::anyhow!("Only the btih format is supported for xt")
            )?
        )?.try_into().map_err(|_| anyhow::anyhow!("Btih format requires 20 bytes"))?;
        Ok(Self(bytes))
    }

    pub fn encode(&self) -> String {
        self.0
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>()
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.map(|b| format!("{b:02x}")).join(""))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(pub [u8; 20]);

impl PeerId {
    pub fn from(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn random() -> Self {
        let mut bytes = [0; 20];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn to_compact(&self) -> String {
        let id = self.0;
        format!(
            "{:02x}{:02x}{:02x}..{:02x}{:02x}{:02x}",
            id[0], id[1], id[2], id[17], id[18], id[19],
        )
    }

    pub fn encode(&self) -> String {
        self.0
            .iter() .map(|b| format!("%{b:02X}"))
            .collect::<String>()
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.map(|b| format!("{b:02x}")).join(""))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PeerEndpoint {
    Ip(SocketAddr),
    Hostname {
        host: String,
        port: u16,
    },
}

impl PeerEndpoint {
    pub fn from_str(s: &str, port: u16) -> Self {
        if let Ok(ip) = s.parse::<Ipv4Addr>() {
            Self::Ip(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        } else {
            Self::Hostname {
                host: s.to_string(),
                port,
            }
        }
    }
    
    pub fn from_compact_4(compact: &[u8]) -> Vec<Self> {
        compact
            .chunks_exact(6)
            .map(|peer| {
                Self::Ip(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(peer[0], peer[1], peer[2], peer[3]),
                    u16::from_be_bytes([peer[4], peer[5]]),
                )))
            })
            .collect()
    }

    pub fn from_compact_6(compact: &[u8]) -> Vec<Self> {
        compact
            .chunks_exact(18)
            .map(|peer| {
                let ip = Ipv6Addr::from([
                    peer[0], peer[1], peer[2], peer[3],
                    peer[4], peer[5], peer[6], peer[7],
                    peer[8], peer[9], peer[10], peer[11],
                    peer[12], peer[13], peer[14], peer[15],
                ]);
                
                let port = u16::from_be_bytes([peer[16], peer[17]]);
                
                Self::Ip(SocketAddr::V6(SocketAddrV6::new(
                    ip, port, 0, 0,
                )))
            })
            .collect()
    }
}

impl fmt::Display for PeerEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hostname { host, port } => write!(f, "{host}:{port}"),
            Self::Ip(ip) => write!(f, "{ip}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: Option<PeerId>,
    pub endpoints: HashSet<PeerEndpoint>,
    pub flags: Option<u8>,
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(endpoint) = self.endpoints.iter().next() {
            write!(f, "{}", endpoint)?;
        }
        if let Some(id) = self.id {
            write!(f, " ({})", id.to_compact())?;
        }
        Ok(())
    }
}

impl PeerInfo {
    pub fn is_same_peer(&self, other: &Self) -> bool {
        match (self.id, other.id) {
            (Some(a), Some(b)) => a == b,
            _ => self.endpoints.intersection(&other.endpoints).next().is_some(),
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.id = self.id.or(other.id);
        self.flags = self.flags.map(
            |f| f | other.flags.unwrap_or(0xFF)
        );
        self.endpoints.extend(other.endpoints);
    }

    pub fn prefers_encryption(&self) -> Option<bool> {
        self.flags.map(|f| (f & 0x01) >= 1)
    }
    pub fn upload_only(&self) -> Option<bool> {
        self.flags.map(|f| (f & 0x02) >= 1)
    }
    pub fn supports_utp(&self) -> Option<bool> {
        self.flags.map(|f| (f & 0x04) >= 1)
    }
    pub fn holepunch_support(&self) -> Option<bool> {
        self.flags.map(|f| (f & 0x08) >= 1)
    }
    pub fn reachable(&self) -> Option<bool> {
        self.flags.map(|f| (f & 0x10) >= 1)
    }

    pub fn new(endpoint: PeerEndpoint) -> Self {
        Self {
            id: None,
            flags: None,
            endpoints: HashSet::from_iter(vec![endpoint]),
        }
    }

    pub fn from_tracker_bencode(root: &BencodeValue) -> Result<Vec<Self>, String> {
        match root {
            BencodeValue::List(peers) => {
                peers.iter().map(|peer| Ok(Self {
                    id: Some(PeerId::from(peer.required_bytes("peer id")?.try_into().map_err(
                        |_| "'id' needs to be 20 bytes long"
                    )?)),
                    flags: None,
                    endpoints: HashSet::from_iter(vec![
                        PeerEndpoint::from_str(
                            &peer.required_string("ip")?,
                            peer.required_unsigned("port")?.try_into().map_err(
                                |_| ""
                            )?,
                        )
                    ]),
                }))
                    .collect::<Result<_, _>>()
            },
            BencodeValue::ByteString(peers) => {
                Ok(Self::from_compact(peers))
            },
            _ => Err(format!("'peers' must be either a list or a byte string")),
        }
    }

    pub fn from_compact(compact: &[u8]) -> Vec<Self> {
        PeerEndpoint::from_compact_4(compact).into_iter()
            .map(Self::new).collect()
    }

    pub async fn resolve(&self) -> Result<Vec<SocketAddr>> {
        let mut addrs = Vec::new();

        for endpoint in &self.endpoints {
            match endpoint {
                PeerEndpoint::Ip(addr) => {
                    addrs.push(*addr);
                }
                PeerEndpoint::Hostname { host, port } => {
                    addrs.extend(
                        tokio::net::lookup_host((host.as_str(), *port)).await?
                    );
                }
            }
        }

        Ok(addrs)
    }
}
