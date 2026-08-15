use sha1::{Sha1, Digest};
use std::fmt;

use crate::bencode::BencodeValue;

pub struct MetainfoFile {
    pub path: Vec<String>,
    pub length: usize,
}

pub enum MetainfoMode {
    SingleFile(MetainfoFile),
    MultiFile(Vec<MetainfoFile>),
}

pub struct Metainfo {
    pub announces: Vec<Vec<String>>,
    pub created_by: Option<String>,
    pub name: String,
    pub piece_length: usize,
    pub pieces: Vec<[u8; 20]>,
    pub mode: MetainfoMode,
    pub info_hash: [u8; 20],
}

impl MetainfoMode {
    fn from_bencode(bencode: &BencodeValue) -> Result<Self, String> {
        match (bencode.get("length"), bencode.get("files")) {
            (Some(_), Some(_)) => return Err(format!("only one of 'length' or 'files' must be set")),
            (Some(length), _) => Ok(Self::SingleFile(
                MetainfoFile {
                    path: Vec::new(),
                    length: length.unsigned("length")? as usize,
                }
            )),
            (_, Some(files)) => Ok(Self::MultiFile(
                files
                    .as_list()
                    .ok_or_else(|| "'files' must be a list")?
                    .iter()
                    .map(|file| {
                        Ok(MetainfoFile {
                            path: file.required_string_list("path")?,
                            length: file.required_unsigned("length")? as usize,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?
            )),
            _ => return Err(format!("either 'length' or 'files' must be set")),
        }
    }
}

impl Metainfo {
    pub fn from_bytes(encoded: &[u8]) -> Result<Self, String> {

        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;
        let info = root.required("info")?;

        Ok(Metainfo {
            info_hash: Sha1::digest(info.to_bytes()).into(),
            announces: match root.get("announce-list") {
                Some(announce_list) => announce_list
                    .as_list()
                    .ok_or_else(|| "'announce-list' must be a list")?
                    .iter()
                    .map(|tier| tier.string_list("announce-list[]"))
                    .collect::<Result<_, _>>()?,
                None => vec![vec![root.required_string("announce")?]],
            },
            created_by: root.optional_string("created by")?,
            name: info.required_string("name")?,
            piece_length: info.required_unsigned("piece length")? as usize,
            pieces: info.required_bytes("pieces")?
                .chunks(20).map(|chunk| chunk.try_into().unwrap())
                .collect(),
            mode: MetainfoMode::from_bencode(&info)?,
        })
    }

    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    pub fn length(&self) -> usize {
        match &self.mode {
            MetainfoMode::SingleFile(file) => {
                file.length
            }
            MetainfoMode::MultiFile(files) => {
                files.iter().fold(0, |a, e| e.length + a)
            }
        }
    }

    pub fn last_piece_length(&self) -> usize {
        self.length() - self.piece_length * (self.num_pieces() - 1)
    }
}

impl fmt::Display for Metainfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Name: {}", self.name)?;
        if let Some(created_by) = &self.created_by {
            writeln!(f, "Created by: {}", created_by)?;
        }
        writeln!(f, "{} pieces of {} kB", self.pieces.len(), self.piece_length / 1024)?;
        writeln!(f, "Info hash: {}", self.info_hash.map(|b| format!("{b:02x}")).join(""))?;
        writeln!(f, "----------------------------------------------")?;
        writeln!(f, "{} tracker tiers:", self.announces.len())?;
        for (i, announce) in self.announces.iter().enumerate() {
            writeln!(f, "{}. {:?}", i + 1, announce)?;
        }
        writeln!(f, "----------------------------------------------")?;
        match &self.mode {
            MetainfoMode::SingleFile(file) => {
                writeln!(f, "Length: {}", file.length)?;
            }
            MetainfoMode::MultiFile(files) => {
                writeln!(f, "{} files:", files.len())?;
                for file in files {
                    let pretty_path = file.path.join("/");
                    writeln!(f, "/{} ({})", pretty_path, file.length)?;
                }
            }
        }
        Ok(())
    }
}
