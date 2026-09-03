use sha1::{Sha1, Digest};
use std::fmt;
use std::path::PathBuf;

use crate::{
    bencode::BencodeValue,
    util::pretty_size,
    types::*,
};

const LABEL_WIDTH: usize = 20;
const INDENT: &str = "    ";

pub struct MetainfoFile {
    pub path: PathBuf,
    pub length: usize,
}

impl MetainfoFile {
    fn from_bencode(root: &BencodeValue, name: &str) -> Result<Vec<Self>, String> {
        match (root.get("length"), root.get("files")) {
            (Some(_), Some(_)) => return Err(format!("only one of 'length' or 'files' must be set")),
            (Some(length), _) => Ok(vec![
                MetainfoFile {
                    path: PathBuf::from(name),
                    length: length.unsigned("length")? as usize,
                }
            ]),
            (_, Some(files)) => Ok(
                files
                    .as_list()
                    .ok_or_else(|| "'files' must be a list")?
                    .iter()
                    .map(|file| {
                        Ok(Self {
                            path: PathBuf::from(name).join(
                                file.required_string_list("path")?
                                    .join("/")
                            ),
                            length: file.required_unsigned("length")? as usize,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?
            ),
            _ => return Err(format!("either 'length' or 'files' must be set")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PieceFile {
    pub file_index: usize,
    pub piece_offset: usize,
    pub file_offset: usize,
    pub length: usize,
}

pub struct Metadata {
    pub name: String,
    pub piece_length: usize,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<MetainfoFile>,

    // derived from metadata
    pub num_pieces: usize,
    pub length: usize,
    pub last_piece_length: usize,
    pub piece_files: Vec<Vec<PieceFile>>,
}

impl Metadata {
    pub fn from_bytes(encoded: &[u8]) -> Result<Self, String> {
        let info = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;
        let name = info.required_string("name")?;
        let files = MetainfoFile::from_bencode(&info, &name)?;
        let piece_length = info.required_unsigned("piece length")? as usize;
        let pieces: Vec<_> = info.required_bytes("pieces")?
            .chunks(20).map(|chunk| chunk.try_into().unwrap())
            .collect();

        let num_pieces = pieces.len();
        let length = files.iter().fold(0, |a, e| e.length + a);
        let last_piece_length = length - piece_length * (num_pieces - 1);

        let mut piece_files = Vec::new();
        for p in 0..num_pieces {
            let mut file_offset = 0;
            let piece_start = p * piece_length;
            let piece_length = if p == num_pieces - 1 {last_piece_length} else {piece_length};
            let piece_end = piece_start + piece_length;
            let mut files_for_piece = Vec::new();
            for (f, file) in files.iter().enumerate() {
                let file_start = file_offset;
                let file_end = file_start + file.length;
                let overlap_start = piece_start.max(file_start);
                let overlap_end = piece_end.min(file_end);
                if overlap_start < overlap_end {
                    files_for_piece.push(PieceFile {
                        file_index: f,
                        piece_offset: overlap_start - piece_start,
                        file_offset: overlap_start - file_start,
                        length: overlap_end - overlap_start,
                    });
                }
                file_offset = file_end;
            }
            piece_files.push(files_for_piece);
        }

        Ok(Self {
            name,
            piece_length,
            pieces,
            files,

            length,
            last_piece_length,
            num_pieces,
            piece_files,
        })
    }

    pub fn piece_length(&self, index: usize) -> usize {
        if index == self.num_pieces - 1 {
            self.last_piece_length
        } else {
            self.piece_length
        }
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{}", "Name", self.name)?;
        writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{} ({})", "Pieces",
            self.pieces.len(), pretty_size(self.piece_length))?;
        writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{}", "Length",
            pretty_size(self.length))?;
        write!(f, "{INDENT}{:<LABEL_WIDTH$}", "Files")?;
        for (i, file) in self.files.iter().enumerate() {
            if i != 0 {
                write!(f, "{INDENT}{:<LABEL_WIDTH$}", "")?;
            }
            writeln!(f, "{} ({})", file.path.to_string_lossy(), pretty_size(file.length))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Metainfo {
    pub announces: Vec<Vec<String>>,

    pub created_by: Option<String>,
    pub comment: Option<String>,
    pub info_hash: InfoHash,
}

impl Metainfo {
    pub fn from_magnet(info_hash: InfoHash, trackers: Vec<String>) -> Self {
        Self {
            announces: vec![trackers],
            info_hash,
            created_by: None,
            comment: None,
        }
    }
    
    pub fn from_bytes(encoded: &[u8]) -> Result<(Self, Vec<u8>), String> {
        let root = BencodeValue::from_bytes(encoded)?.0.ok_or_else(
            || "Unable to find root dictionary"
        )?;
        let info = root.required("info")?;
        let info_bytes = info.to_bytes();

        Ok((Metainfo {
            info_hash: InfoHash::from(Sha1::digest(&info_bytes).into()),
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
            comment: root.optional_string("comment")?,
        }, info_bytes))
    }
}


impl fmt::Display for Metainfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(created_by) = &self.created_by {
            writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{}", "Created by", created_by)?;
        }
        writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{}", "Info hash",
            self.info_hash)?;
        write!(f, "{INDENT}{:<LABEL_WIDTH$}", "Tracker tiers")?;
        for (i, announce) in self.announces.iter().enumerate() {
            if i != 0 {
                write!(f, "{INDENT}{:<LABEL_WIDTH$}", "")?;
            }
            writeln!(f, "{}. {:?}", i + 1, announce)?;
        }
        if let Some(comment) = &self.comment {
            writeln!(f, "{INDENT}{:<LABEL_WIDTH$}{}", "Comment", comment)?;
        }
        Ok(())
    }
}
