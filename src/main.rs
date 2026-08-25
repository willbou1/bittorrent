mod bencode;
mod metainfo;
mod tracker;
mod peer;
mod bitfield;
mod torrent;
mod timer;

use std::{
    path::PathBuf,
    env,
};

use torrent::Torrent;
use peer::PeerId;
use bencode::BencodeValue;
use tracing_subscriber::{EnvFilter, fmt};
use rand::Rng;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                .add_directive("hyper=warn".parse().unwrap())
        )
        .without_time()
        .init();

    let args: Vec<String> = env::args().collect();
    let command = &args[1];
    let second = &args[2];

    match command.as_str() {
        "info" | "download" => {
            let mut client_id: PeerId = [0; 20];
            rand::rng().fill_bytes(&mut client_id);
            println!("Client id: {}", client_id.map(|b| format!("{b:02x}")).join(""));
            let mut torrent = Torrent::from_torrent_file(&PathBuf::from(second), &client_id).await.unwrap();
            if command == "download" {
                torrent.download().await.unwrap();
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
