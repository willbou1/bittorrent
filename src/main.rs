use anyhow::Result;
use std::{
    path::PathBuf,
    time::Duration,
    env,
};
use console_subscriber;
use tracing_subscriber::{
    EnvFilter, Layer, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt
};
use tokio::{
    signal,
    sync::{mpsc, watch},
    task::JoinHandle,
};

use libvictoria::{
    torrent::{Torrent, control::*},
    bencode::BencodeValue,
    util::*,
    types::*,
};

struct TorrentTask {
    task: JoinHandle<()>,
    tx: mpsc::Sender<Command>,
    rx: watch::Receiver<Progress>,
}

async fn run_torrents(torrent_uris: &[String]) -> Result<()> {
    let client_id = PeerId::random();
    println!("Client id: {client_id}");

    let mut torrent_tasks = Vec::new();
    for arg in torrent_uris {
        let uri = arg.clone();

        let mut torrent = if uri.starts_with("magnet:?") {
            Torrent::from_magnet(&uri, client_id).await.unwrap()
        } else {
            Torrent::from_torrent_file(&PathBuf::from(uri), client_id).await.unwrap()           
        };

        torrent_tasks.push(TorrentTask {
            rx: torrent.progress_rx.clone(),
            tx: torrent.command_tx.clone(),
            task: tokio::task::Builder::new()
                .name("torrent")
                .spawn( async move {
                    torrent.run().await.unwrap();
                }).unwrap()
        });
    }

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let ctrl_c = signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                print!("\x1B[2J\x1B[H");

                for torrent_task in &torrent_tasks {
                    let progress = torrent_task.rx.borrow();

                    println!(
                        "{} {:>3} {:>3} {:>4} {:>9}/s {:>9}/s {:>5.2}% {}\n{} ETA {}",
                        if let Some(bitfield) = &progress.metadata_bitfield
                            && bitfield.len() != 0 && bitfield.len() == bitfield.num_set()
                            {"⇆"} else {"ℹ"},
                        progress.num_peers,
                        progress.num_connected_peers,
                        progress.num_discovery_attempts,
                        progress.transfer.as_ref()
                            .map(|t| pretty_size(t.down_speed))
                            .unwrap_or(String::new()),
                        progress.transfer.as_ref()
                            .map(|t| pretty_size(t.up_speed))
                            .unwrap_or(String::new()),
                        progress.transfer.as_ref()
                            .map(|t| t.downloaded as f64 * 100. / t.size as f64)
                            .unwrap_or(0.),
                        progress.display_name,
                        progress.transfer.as_ref()
                            .map(|t| format!("{:80}", t.piece_bitfield))
                            .unwrap_or(String::new()),
                        progress.transfer.as_ref()
                        .map(|t| pretty_duration(
                            Duration::from_secs(
                                if t.down_speed == 0 {
                                    0
                                } else {
                                    ((t.size - t.downloaded) / t.down_speed) as u64
                                }
                            )
                        ))
                        .unwrap_or(String::new()),
                    );

                    if let Some(transfer) = &progress.transfer {
                        let mut i = 0;
                        for piece in &transfer.active_pieces {
                            print!("{:<4} {:30} {:>5.2}% {:>11}",
                                piece.index,
                                piece.block_bitfield,
                                piece.num_obtained_blocks as f64 * 100. / piece.num_blocks as f64,
                                format!("({}/{})", piece.num_obtained_blocks, piece.num_blocks),
                            );
                            i += 1;
                            print!("{}",
                                if i % 2 == 0 || i == transfer.active_pieces.len() {"\n"}
                                else {"   │   "}
                            );
                        }
                    }
                }
            }

            _ = &mut ctrl_c => {
                break;
            }
        }
    }
    
    for torrent_task in torrent_tasks {
        torrent_task.tx.send(Command::Stop).await?;
        torrent_task.task.await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time()
        .with_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("error"))
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("reqwest=warn".parse().unwrap())
        );

    #[cfg(debug_assertions)] {
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


        tracing_subscriber::registry()
            .with(console_layer)
            .with(json_layer)
            .with(fmt_layer)
            .init();
    }

    #[cfg(not(debug_assertions))] {
        tracing_subscriber::registry()
            .with(fmt_layer)
            .init();
    }

    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    match command.as_str() {
        "run" => {
            run_torrents(&args[2..]).await.unwrap_or_else(|e| eprintln!("{e}"));
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
