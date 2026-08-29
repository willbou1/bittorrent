mod bencode;
mod metainfo;
mod tracker;
mod peer;
mod bitfield;
mod torrent;
mod timer;
mod piece;
mod util;

use std::{
    path::PathBuf,
    env,
};

use torrent::Torrent;
use peer::PeerId;
use bencode::BencodeValue;
use tracing_subscriber::{EnvFilter, fmt};
use rand::Rng;
use tokio::{
    signal,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("reqwest=warn".parse().unwrap())
        )
        .with_target(false)
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
            if command == "download" {
                let token = CancellationToken::new();
                let token_clone = token.clone();
                let second = args[2].clone();
                let task = tokio::spawn( async move {
                    let mut torrent = Torrent::from_torrent_file(&PathBuf::from(second), &client_id).await.unwrap();
                    torrent.run(token_clone).await;
                });
                let _ = signal::ctrl_c().await;
                token.cancel();
                let _ = tokio::join!(task);
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
