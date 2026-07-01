use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Bytes;
use futures::{future, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::io::{CopyToBytes, SinkWriter, StreamReader};
use tokio_util::sync::CancellationToken;
use tokio_yamux::{Config, Session, StreamHandle};
use tracing::info;

use crate::bistream::BiStream;
use crate::protocol::{self, StreamKind, validate_label};
use crate::Error;

#[derive(Clone)]
pub struct AgentClient {
    control: tokio_yamux::Control,
    shutdown: CancellationToken,
    bind_labels: Arc<RwLock<HashMap<String, mpsc::Sender<StreamHandle>>>>,
}

impl AgentClient {
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let url = if addr.starts_with("ws://") || addr.starts_with("wss://") {
            addr.to_string()
        } else {
            format!("ws://{addr}")
        };

        let (ws, _) = connect_async(&url)
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;
        let (sink, stream) = ws.split();

        let reader = StreamReader::new(stream.filter_map(|item| async move {
            match item {
                Ok(m) if !matches!(m, tokio_tungstenite::tungstenite::Message::Close(_)) => {
                    Some(Ok::<_, std::io::Error>(m.into_data()))
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => None,
                Err(e) => Some(Err(std::io::Error::other(e.to_string()))),
                _ => Some(Err(std::io::Error::other("unexpected websocket frame"))),
            }
        }).boxed());
        let writer = SinkWriter::new(CopyToBytes::new(
            sink.sink_map_err(|e: tokio_tungstenite::tungstenite::Error| {
                std::io::Error::other(e.to_string())
            })
            .with(|data: Bytes| {
                future::ready(Ok::<_, std::io::Error>(
                    tokio_tungstenite::tungstenite::Message::Binary(data),
                ))
            }),
        ));

        let transport = BiStream { reader, writer };
        let mut session = Session::new_client(transport, Config::default());
        let control = session.control();
        let shutdown = CancellationToken::new();
        let bind_labels: Arc<RwLock<HashMap<String, mpsc::Sender<StreamHandle>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let bl = bind_labels.clone();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            loop {
                match session.next().await {
                    Some(Ok(mut ys)) => {
                        let bl = bl.clone();
                        let sd = sd.clone();
                        tokio::spawn(async move {
                            let kind = match protocol::read_frame(&mut ys).await {
                                Ok(k) => k,
                                Err(e) => {
                                    tracing::warn!(error = %e, "read frame");
                                    return;
                                }
                            };
                            if sd.is_cancelled() {
                                return;
                            }
                            if let StreamKind::Data { label } = kind {
                                let tx = bl.read().unwrap().get(&label).cloned();
                                if let Some(tx) = tx {
                                    let _ = tx.send(ys).await;
                                }
                            }
                        });
                    }
                    Some(Err(_)) => continue,
                    None => break,
                }
            }
            sd.cancel();
        });

        Ok(Self {
            control,
            shutdown,
            bind_labels,
        })
    }

    pub async fn bind(&self, local_port: u16, label: &str) -> Result<(), Error> {
        validate_label(label)?;
        let (tx, mut rx) = mpsc::channel::<StreamHandle>(64);
        let mut control = self.control.clone();

        {
            let mut map = self.bind_labels.write().unwrap();
            match map.entry(label.to_string()) {
                Entry::Occupied(_) => {
                    return Err(Error::Protocol(format!("label '{label}' already bound")));
                }
                Entry::Vacant(e) => {
                    e.insert(tx);
                }
            }
        }

        let mut reg = control
            .open_stream()
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;
        if let Err(e) = protocol::write_frame(
            &mut reg,
            &StreamKind::Register {
                label: label.to_string(),
            },
        )
        .await
        {
            self.bind_labels.write().unwrap().remove(label);
            return Err(e);
        }

        info!(%local_port, %label, "binding");

        loop {
            let mut stream = tokio::select! {
                stream = rx.recv() => match stream {
                    Some(s) => s,
                    None => break,
                },
                _ = self.shutdown.cancelled() => break,
            };
            let mut local = match TcpStream::connect(("127.0.0.1", local_port)).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(%local_port, error = %e, "connect failed");
                    continue;
                }
            };
            tokio::spawn(async move {
                if let Err(e) = tokio::io::copy_bidirectional(&mut stream, &mut local).await {
                    tracing::warn!(error = %e, "bind bridge");
                }
            });
        }

        self.bind_labels.write().unwrap().remove(label);
        Ok(())
    }

    pub async fn forward(&self, listen_port: u16, label: &str) -> Result<(), Error> {
        validate_label(label)?;
        let control = self.control.clone();
        let label = label.to_string();
        let listener = TcpListener::bind(("127.0.0.1", listen_port)).await?;
        info!(%listen_port, %label, "forwarding");

        loop {
            let (mut incoming, peer) = tokio::select! {
                res = listener.accept() => res?,
                _ = self.shutdown.cancelled() => return Ok(()),
            };
            info!(%peer, %label, "accepted");
            let mut ctrl = control.clone();
            let lab = label.clone();
            tokio::spawn(async move {
                let mut stream = match ctrl.open_stream().await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "open stream");
                        return;
                    }
                };
                if let Err(e) =
                    protocol::write_frame(&mut stream, &StreamKind::Data { label: lab }).await
                {
                    tracing::warn!(error = %e, "write frame");
                    return;
                }
                if let Err(e) = tokio::io::copy_bidirectional(&mut incoming, &mut stream).await {
                    tracing::warn!(error = %e, "forward bridge");
                }
            });
        }
    }
}
