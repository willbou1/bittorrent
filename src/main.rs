mod bencode;
mod metainfo;
mod tracker;
mod proto;
mod bitfield;
mod torrent;
mod timer;
mod util;
mod types;

use std::{
    path::PathBuf,
    env,
};

use torrent::Torrent;
use bencode::BencodeValue;
use types::*;

use console_subscriber;
use tracing_subscriber::{
    EnvFilter, Layer, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt
};
use tokio::{
    signal,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let console_layer = console_subscriber::spawn()
        .with_filter(LevelFilter::TRACE);

    let file_appender = tracing_appender::rolling::never(".", "debug.json");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let json_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_filter(
            EnvFilter::new("debug")
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("reqwest=warn".parse().unwrap())
        );

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time()
        .with_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("reqwest=warn".parse().unwrap())
        );

    tracing_subscriber::registry()
        .with(console_layer)
        .with(json_layer)
        .with(fmt_layer)
        .init();

    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    match command.as_str() {
        "run" => {
            let client_id = PeerId::random();
            println!("Client id: {client_id}");

            let mut torrents = Vec::new();
            let token = CancellationToken::new();
            for arg in &args[2..] {
                let token_clone = token.clone();
                let uri = arg.clone();

                torrents.push(
                    tokio::task::Builder::new()
                        .name("torrent")
                        .spawn( async move {
                            if uri.starts_with("magnet:?") {
                                let mut torrent = Torrent::from_magnet(&uri, client_id).await.unwrap();
                                torrent.run(token_clone).await.unwrap();
                            } else {
                                let mut torrent = Torrent::from_torrent_file(&PathBuf::from(uri), client_id).await.unwrap();
                                torrent.run(token_clone).await.unwrap();
                            }
                        }).unwrap()
                );
            }

            let _ = signal::ctrl_c().await;
            token.cancel();
            for torrent in torrents {
                torrent.await.unwrap();
            }
        }
        "decode" => {
            let second = &args[2];
            let decoded_value = BencodeValue::from_bytes(second.as_bytes()).unwrap_or_else(
                |e| panic!("{e}")
            );
            println!("{:?}", decoded_value.0.unwrap());
        }
        _ => println!("unknown command: {}", args[1]),
    }
}
