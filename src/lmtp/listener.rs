use std::{net::SocketAddr, sync::Arc, time::Duration};

use actix_web::web;
use store::{
    config::env_settings::EnvSettings,
    tracing::{debug, error, info, warn},
    Store,
};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::watch};
use tokio_rustls::TlsAcceptor;

use crate::{cluster::rpc::tls::load_tls_server_config, lmtp::session::Session, JMAPServer};

const TIMEOUT: Duration = Duration::from_secs(5 * 60); // 5 minutes
const DEFAULT_LMTP_PORT: u16 = 11200;

pub fn init_lmtp() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel::<bool>(true)
}

pub fn spawn_lmtp<T>(
    core: web::Data<JMAPServer<T>>,
    settings: &EnvSettings,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    T: for<'x> Store<'x> + 'static,
{
    // Parse bind address
    let bind_addr = SocketAddr::from((
        settings.parse_ipaddr("lmtp-bind-addr", "127.0.0.1"),
        settings.parse("lmtp-port").unwrap_or(DEFAULT_LMTP_PORT),
    ));
    info!("Starting LMTP service at {}...", bind_addr);

    // Build TLS acceptor
    let tls_acceptor = if let (Some(cert_path), Some(key_path)) = (
        settings.get("lmtp-cert-path"),
        settings.get("lmtp-key-path"),
    ) {
        Arc::new(TlsAcceptor::from(Arc::new(load_tls_server_config(
            &cert_path, &key_path,
        ))))
        .into()
    } else {
        None
    };
    let mut tls_only = settings.parse("lmtp-tls-only").unwrap_or(false);
    if tls_only && tls_acceptor.is_none() {
        warn!("LMTP server is configured to only accept TLS connections, but no TLS certificate was provided.");
        tls_only = false;
    }

    tokio::spawn(async move {
        // Start listening for LMTP connections.
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!("Failed to bind LMTP service to {}: {}", bind_addr, err);
                return;
            }
        };

        let hostname = Arc::new(
            gethostname::gethostname()
                .to_str()
                .unwrap_or("localhost")
                .to_string(),
        );
        let greeting = Arc::new(
            format!(
                concat!(
                    "220 {} Stalwart LMTP v",
                    env!("CARGO_PKG_VERSION"),
                    " at your service.\r\n"
                ),
                &hostname
            )
            .into_bytes(),
        );

        loop {
            tokio::select! {
                stream = listener.accept() => {
                    match stream {
                        Ok((mut stream, _)) => {
                            let shutdown_rx = shutdown_rx.clone();
                            let core = core.clone();
                            let greeting = greeting.clone();
                            let tls_acceptor = tls_acceptor.clone();
                            let hostname = hostname.clone();

                            tokio::spawn(async move {
                                let peer_addr = stream.peer_addr().unwrap();

                                if tls_only {
                                    let mut stream = match tls_acceptor.as_ref().unwrap().accept(stream).await {
                                        Ok(stream) => stream,
                                        Err(e) => {
                                            debug!("Failed to accept TLS connection: {}", e);
                                            return;
                                        }
                                    };

                                    // Send greeting
                                    if let Err(err) = stream.write_all(&greeting).await {
                                        debug!("Failed to send greeting to {}: {}", peer_addr, err);
                                        return;
                                    }

                                    handle_conn(
                                        Session::new(core, peer_addr, stream.into(), None, hostname),
                                        shutdown_rx
                                    ).await;
                                } else {
                                    // Send greeting
                                    if let Err(err) = stream.write_all(&greeting).await {
                                        debug!("Failed to send greeting to {}: {}", peer_addr, err);
                                        return;
                                    }

                                    handle_conn(
                                        Session::new(core, peer_addr, stream.into(), tls_acceptor, hostname),
                                        shutdown_rx
                                    ).await;
                                }
                            });
                        }
                        Err(err) => {
                            error!("Failed to accept TCP connection: {}", err);
                        }
                    }
                },
                _ = shutdown_rx.changed() => {
                    debug!("LMTP listener shutting down.");
                    break;
                }
            };
        }
    });
}

pub async fn handle_conn<T>(mut session: Session<T>, mut shutdown_rx: watch::Receiver<bool>)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut buf = vec![0; 4096];

    loop {
        tokio::select! {
            result = tokio::time::timeout(
                TIMEOUT,
                session.read_bytes(&mut buf)) => {
                match result {
                    Ok(Ok(bytes_read)) => {
                        if bytes_read > 0 {
                            if session.ingest(&buf[..bytes_read]).await.is_err() {
                                debug!("Disconnecting client.");
                                return;
                            }
                        } else {
                            debug!("LMTP connection closed by {}", session.peer_addr);
                            break;
                        }
                    },
                    Ok(Err(_)) => {
                        break;
                    },
                    Err(_) => {
                        session.write_bytes(b"221 2.0.0 Disconnecting inactive client.\r\n").await.ok();
                        debug!("LMTP connection timed out with {}.", session.peer_addr);
                        break;
                    }
                }
            },
            _ = shutdown_rx.changed() => {
                session.write_bytes(b"421 4.3.0 Server shutting down.\r\n").await.ok();
                debug!("LMTP connection with peer {} shutting down.", session.peer_addr);
                return;
            }
        };
    }
}
