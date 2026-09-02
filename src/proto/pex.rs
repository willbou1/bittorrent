use std::{
    collections::{HashSet},
};

use crate::{
    bencode::BencodeValue,
    types::*,
};

// BEP 11: https://www.bittorrent.org/beps/bep_0011.html#bep-40

#[derive(Debug)]
pub struct PEXMessage {
    pub added: Vec<PeerInfo>,
    pub dropped: Vec<PeerEndpoint>,
}

impl PEXMessage {
    pub fn from_bytes(encoded: &[u8]) -> Result<Self, String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        let mut added = Vec::new();
        let mut dropped = Vec::new();

        let flags4 = root.optional_bytes("added.f")?;
        let flags6 = root.optional_bytes("added6.f")?;

        if let Some(added_bytes) = &root.optional_bytes("added")? {
            added.extend(
                PeerEndpoint::from_compact_4(added_bytes)
                    .into_iter().enumerate().map(
                        |(e, endpoint)| PeerInfo {
                            endpoints: HashSet::from_iter(vec![endpoint]),
                            id: None,
                            flags: flags4.as_ref().map(|f| f[e]),
                        })
            );
        }
        if let Some(added6_bytes) = &root.optional_bytes("added6")? {
            added.extend(
                PeerEndpoint::from_compact_6(added6_bytes)
                    .into_iter().enumerate().map(
                        |(e, endpoint)| PeerInfo {
                            endpoints: HashSet::from_iter(vec![endpoint]),
                            id: None,
                            flags: flags6.as_ref().map(|f| f[e]),
                        })
            );
        }

        if let Some(dropped_bytes) = &root.optional_bytes("dropped")? {
            dropped.extend(PeerEndpoint::from_compact_4(dropped_bytes));
        }
        if let Some(dropped6_bytes) = &root.optional_bytes("dropped6")? {
            dropped.extend(PeerEndpoint::from_compact_4(dropped6_bytes));
        }

        Ok(Self {
            added,
            dropped,
        })
    }
}
