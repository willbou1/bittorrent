use std::{
    collections::HashMap,
};

use crate::{
    bencode::BencodeValue,
};

// BEP 9: https://www.bittorrent.org/beps/bep_0009.html 

#[derive(Debug)]
pub enum MetadataMessage {
    Request {
        index: usize,
    },
    Reject {
        index: usize,
    },
    Data {
        index: usize,
        total_size: usize,
        piece: Vec<u8>,
    },
    Unsupported {
        type_byte: u64,
    },
}

impl MetadataMessage {
    pub fn from_bytes(encoded: &[u8]) -> Result<Self, String> {
        let decoded = BencodeValue::from_bytes(encoded)?;
        let root = decoded.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;

        Ok(match root.required_unsigned("msg_type")? {
            0 => Self::Request {
                index: root.required_unsigned("piece")? as usize,
            },
            1 => Self::Data {
                index: root.required_unsigned("piece")? as usize,
                total_size: root.required_unsigned("total_size")? as usize,
                piece: decoded.1.to_vec(),
            },
            2 => Self::Reject {
                index: root.required_unsigned("piece")? as usize,
            },
            type_byte => Self::Unsupported { type_byte },
        })
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Request { index } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(0));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
                BencodeValue::Dictionary(root).to_bytes()
            }
            Self::Data { index, total_size, piece } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(0));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
                root.insert("total_size".to_string(), BencodeValue::Integer(total_size as i64));
                let mut ret = BencodeValue::Dictionary(root).to_bytes();
                ret.extend(piece);
                ret
            }
            Self::Reject { index } => {
                let mut root = HashMap::new();
                root.insert("msg_type".to_string(), BencodeValue::Integer(2));
                root.insert("piece".to_string(), BencodeValue::Integer(index as i64));
                BencodeValue::Dictionary(root).to_bytes()
            }
            _ => Vec::new(),
        }
    }
}
