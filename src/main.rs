mod bencode;
mod metainfo;

use bencode::BencodeValue;
use metainfo::Metainfo;

use std::{env,fs};


fn main() {
    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    if command == "decode" {
        // eprintln!("Logs from your program will appear here!");

        let encoded_value = &args[2];
        let decoded_value = BencodeValue::from_bytes(encoded_value.as_bytes()).unwrap_or_else(
            |e| panic!("{e}")
        );
        println!("{:?}", decoded_value.0.unwrap());
    } else if command == "info" {
        let torrent_path = &args[2];
        let torrent = fs::read(torrent_path).unwrap();
        let metainfo = Metainfo::from_bytes(&torrent);
        println!("{}", metainfo.unwrap());
    } else {
        println!("unknown command: {}", args[1])
    }
}
