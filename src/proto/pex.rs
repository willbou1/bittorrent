use std::{
    collections::{HashSet},
};

use crate::{
    bencode::BencodeValue,
    types::*,
};
use super::bit_torrent::Message;

pub fn from_pex_bytes(encoded: &[u8]) -> Result<Message, String> {
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

    Ok(Message::PEX {
        added,
        dropped,
    })
}

