use super::request::Request;
use super::{Event, UDP_MAX_PAYLOAD};
use std::{net::SocketAddr, sync::Arc};
use store::tracing::{debug, error};
use tokio::sync::watch;
use tokio::{net::UdpSocket, sync::mpsc};

/*
  Quidnunc: an inquisitive and gossipy person, from Latin quid nunc? 'what now?'.
  Spawns the gossip process in charge of discovering peers and detecting failures.
*/
pub async fn spawn_quidnunc(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    mut gossip_rx: mpsc::Receiver<(SocketAddr, Request)>,
    main_tx: mpsc::Sender<Event>,
) {
    let _socket = Arc::new(match UdpSocket::bind(bind_addr).await {
        Ok(socket) => socket,
        Err(e) => {
            error!("Failed to bind UDP socket on '{}': {}", bind_addr, e);
            std::process::exit(1);
        }
    });

    let socket = _socket.clone();
    tokio::spawn(async move {
        while let Some((target_addr, response)) = gossip_rx.recv().await {
            //debug!("Sending packet to {}: {:?}", target_addr, response);
            if let Err(e) = socket.send_to(&response.to_bytes(), &target_addr).await {
                error!("Failed to send UDP packet to {}: {}", target_addr, e);
            }
        }
    });

    let socket = _socket;
    tokio::spawn(async move {
        let mut buf = vec![0; UDP_MAX_PAYLOAD];

        loop {
            //TODO encrypt packets
            tokio::select! {
                packet = socket.recv_from(&mut buf) => {
                    match packet {
                        Ok((size, addr)) => {
                            if let Some(request) = Request::from_bytes(&buf[..size]) {
                                //debug!("Received packet from {}", addr);
                                if let Err(e) = main_tx.send(Event::Gossip { addr, request }).await {
                                    error!("Gossip process error, tx.send() failed: {}", e);
                                }
                            } else {
                                debug!("Received invalid gossip message from {}", addr);
                            }
                        }
                        Err(e) => {
                            error!("Gossip process ended, socket.recv_from() failed: {}", e);
                        }
                    }
                },
                _ = shutdown_rx.changed() => {
                    debug!("Gossip listener shutting down.");
                    break;
                }
            };
        }
    });
}
