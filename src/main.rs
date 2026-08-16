mod bencode;
mod metainfo;
mod tracker;
mod peer;

use bencode::BencodeValue;
use metainfo::Metainfo;
use peer::{Peer, Message};

use std::sync::Arc;
use std::{env,fs, range};

fn main() {
    let args: Vec<String> = env::args().collect();
    let command = &args[1];
    let second = &args[2];

    match command.as_str() {
        "init" | "info" => {
            let torrent = fs::read(second).unwrap();
            let metainfo = Arc::new(Metainfo::from_bytes(&torrent).unwrap());
            let tracker_response = tracker::tracker_request(
                &metainfo.announces[0][0],
                &metainfo.info_hash, 0, 0, 100,
                tracker::Event::started
            ).unwrap();
            println!("{}", metainfo);
            println!("{}", tracker_response);

            if command == "init" {
                let peer_info = &tracker_response.peers[0];
                let mut peer = Peer::from_info(
                    peer_info,
                    metainfo.clone(),
                    b"00000000000000000000"
                ).unwrap();
                let rec = peer.receive().unwrap();
                peer.handle_message(rec).unwrap();
                peer.send(Message::Interested(true)).unwrap();
                let rec = peer.receive().unwrap();
                peer.handle_message(rec).unwrap();
                for p in 0..metainfo.num_pieces {
                    let num_blocks = metainfo.piece_length(p).div_ceil(1024 * 16);
                    for b in 0..num_blocks {
                        peer.request_block(p, b).unwrap();
                        let rec = peer.receive().unwrap();
                        peer.handle_message(rec).unwrap();
                    }
                    peer.verify_piece(p).unwrap();
                }
            }
        }
        "decode" => {
            let decoded_value = BencodeValue::from_bytes(second.as_bytes()).unwrap_or_else(
                |e| panic!("{e}")
            );
            println!("{:?}", decoded_value.0.unwrap());
        }
        _ => println!("unknown command: {}", args[1]),
    }
}
