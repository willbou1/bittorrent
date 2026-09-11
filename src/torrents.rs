use anyhow::Result;
use std::{
    path::PathBuf,
    time::Duration,
};
use tokio::{
    signal,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use crossterm::{
    execute,
    cursor::MoveTo,
    terminal::{Clear, ClearType, self},
};

use libvictoria::{
    torrent::{Torrent, control::*},
    util::*,
    types::*,
};
use super::table::*;

impl Row for Progress {
    fn columns() -> &'static [Column<Self>] {
        &[
            Column {
                header: "",
                alignment: Alignment::Left,
                max_width: None,
                flex: None,
                total: None,
            },
            Column {
                header: "Con",
                alignment: Alignment::Right,
                max_width: None,
                flex: None,
                total: Some(|rows| {
                    let total: usize = rows.iter()
                        .map(|r| r.num_connected_peers).sum();
                    format!("{total}")
                }),
            },
            Column {
                header: "Dis",
                alignment: Alignment::Right,
                max_width: None,
                flex: None,
                total: Some(|rows| {
                    let total: usize = rows.iter()
                        .map(|r| r.num_discovery_attempts).sum();
                    format!("{total}")
                }),
            },
            Column {
                header: "Down",
                alignment: Alignment::Right,
                max_width: None,
                flex: None,
                total: Some(|rows| {
                    let total: usize = rows.iter()
                        .map(|r| r.transfer.as_ref().map_or(0, |t| t.down_speed)).sum();
                    format!("{}", pretty_size(total))
                }),
            },
            Column {
                header: "Up",
                alignment: Alignment::Right,
                max_width: None,
                flex: None,
                total: Some(|rows| {
                    let total: usize = rows.iter()
                        .map(|r| r.transfer.as_ref().map_or(0, |t| t.up_speed)).sum();
                    format!("{}", pretty_size(total))
                }),
            },
            Column {
                header: "Name",
                alignment: Alignment::Left,
                max_width: None,
                flex: Some(2),
                total: None,
            },
            Column {
                header: "Piece bitfield",
                alignment: Alignment::Left,
                max_width: None,
                flex: Some(1),
                total: None,
            },
            Column {
                header: "%",
                alignment: Alignment::Right,
                max_width: None,
                flex: None,
                total: None,
            },
        ]
    }

    fn display_column(&self, index: usize, width: Option<usize>) -> String {
        match index {
            0 => {
                if let Some(bitfield) = &self.metadata_bitfield
                    && bitfield.len() != 0
                    && bitfield.len() == bitfield.num_set()
                {
                    "⇆".into()
                } else {
                    "ℹ".into()
                }
            }
            1 => self.num_connected_peers.to_string(),
            2 => self.num_discovery_attempts.to_string(),
            3 => self.transfer
                .as_ref()
                .map(|t| pretty_size(t.down_speed))
                .unwrap_or_default().to_string(),
            4 => self.transfer
                .as_ref()
                .map(|t| pretty_size(t.up_speed))
                .unwrap_or_default().to_string(),
            5 => {
                if let Some(width) = width {
                    self.display_name.chars().take(width - 1).chain(['…']).collect()
                } else {
                    self.display_name.to_string()
                }
            },
            6 => self.transfer.as_ref().map(
                |t| format!("{:WIDTH$}", t.piece_bitfield, WIDTH = width.unwrap())
            ).unwrap_or(String::new()),
            7 => format!(
                "{:.2}",
                self.transfer
                    .as_ref()
                    .map(|t| t.downloaded as f64 * 100. / t.size as f64)
                    .unwrap_or(0.)
            ),
            _ => unreachable!(),
        }
    }
}

struct TorrentTask {
    task: JoinHandle<()>,
    tx: mpsc::Sender<Command>,
    rx: watch::Receiver<Progress>,
}

pub async fn run_torrents(torrent_uris: &[String]) -> Result<()> {
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
    let table = Table::<Progress>::new(2);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let rows: Vec<Progress> = torrent_tasks
                .iter()
                .map(|tt| tt.rx.borrow().clone())
                .collect();
                let frame = table.render(&rows, terminal::size()?.0.into());


                //for torrent_task in &torrent_tasks {
                //    let progress = torrent_task.rx.borrow();

                //    frame.push_str(&format!(
                //        "{}\n",
                //        progress.display_column(0, None),
                //    ));

                //    if let Some(transfer) = &progress.transfer {
                //        let mut i = 0;
                //        for piece in &transfer.active_pieces {
                //            frame.push_str(&format!("{:<4} {:30} {:>5.2}% {:>11}",
                //                piece.index,
                //                piece.block_bitfield,
                //                piece.num_obtained_blocks as f64 * 100. / piece.num_blocks as f64,
                //                format!("({}/{})", piece.num_obtained_blocks, piece.num_blocks),
                //            ));
                //            i += 1;
                //            frame.push_str(&format!("{}",
                //                if i % 2 == 0 || i == transfer.active_pieces.len() {"\n"}
                //                else {"   │   "}
                //            ));
                //        }
                //    }
                //}
                execute!(
                    std::io::stdout(),
                    Clear(ClearType::All),
                    MoveTo(0, 0),
                )?;
                print!("{frame}");
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
