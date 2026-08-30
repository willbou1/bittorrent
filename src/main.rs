mod bencode;
mod metainfo;
mod tracker;
mod peer;
mod bitfield;
mod torrent;
mod timer;
mod piece;
mod util;
mod types;

use std::{
    path::PathBuf,
    env,
};

use torrent::Torrent;
use bencode::BencodeValue;
use types::{InfoHash, PeerId};

use tracing_subscriber::{EnvFilter, fmt};
use tokio::{
    signal,
};
use tokio_util::sync::CancellationToken;
use url::Url;

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
        "download" => {
            let mut client_id = PeerId::random();
            println!("Client id: {client_id}");

            let token = CancellationToken::new();
            let token_clone = token.clone();
            let uri = args[2].clone();

            if uri.starts_with("magnet:?") {
                let url = Url::parse(&uri).unwrap();
                let mut pairs = url.query_pairs();
                let xt = pairs.find(|(n, _)| n == "xt").unwrap();
                let dn = pairs.find(|(n, _)| n == "dn").unwrap();
                let display_name = dn.1;

                let info_hash = InfoHash::from_xt(&xt.1).unwrap();
                let trackers: Vec<_> = pairs.filter(|(n, _)| n == "tr")
                    .map(|(_, v)| v).collect();

                println!("{info_hash} {display_name} {trackers:?}");
                return;
            }

            let task = tokio::spawn( async move {
                let mut torrent = Torrent::from_torrent_file(&PathBuf::from(uri), &client_id).await.unwrap();
                torrent.run(token_clone).await;
            });
            let _ = signal::ctrl_c().await;
            token.cancel();
            let _ = tokio::join!(task);
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
