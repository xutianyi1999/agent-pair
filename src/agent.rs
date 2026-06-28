use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Bytes;
use futures::{future, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::io::{CopyToBytes, SinkWriter, StreamReader};
use tokio_yamux::{Config, Session, StreamHandle};
use tracing::info;

use crate::bistream::BiStream;
use crate::protocol::{self, StreamKind};
use crate::Error;

#[derive(Clone)]
pub struct AgentClient {
    control: tokio_yamux::Control,
    bind_labels: Arc<RwLock<HashMap<String, mpsc::Sender<StreamHandle>>>>,
}

impl AgentClient {
    /// Connect to an agent-pair broker via WebSocket.
    ///
    /// The WebSocket stream is immediately mapped to `Bytes` and wrapped
    /// in a yamux session.
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
                Ok(m) if !matches!(m, tokio_tungstenite::tungstenite::Message::Close(_)) => Some(Ok::<_, std::io::Error>(m.into_data())),
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => None,
                Err(e) => Some(Err(std::io::Error::other(e.to_string()))),
                _ => unreachable!(),
            }
        }).boxed());
        let writer = SinkWriter::new(CopyToBytes::new(
            sink.sink_map_err(|e: tokio_tungstenite::tungstenite::Error| std::io::Error::other(e.to_string()))
                .with(|data: Bytes| future::ready(Ok::<_, std::io::Error>(tokio_tungstenite::tungstenite::Message::Binary(data)))),
        ));

        let transport = BiStream { reader, writer };
        let mut session = Session::new_client(transport, Config::default());
        let control = session.control();
        let bind_labels: Arc<RwLock<HashMap<String, mpsc::Sender<StreamHandle>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let bl = bind_labels.clone();
        tokio::spawn(async move {
            while let Some(Ok(mut ys)) = session.next().await {
                let bl = bl.clone();
                tokio::spawn(async move {
                    let kind = match protocol::read_frame(&mut ys).await {
                        Ok(k) => k,
                        Err(e) => {
                            tracing::warn!(error = %e, "read frame");
                            return;
                        }
                    };
                    if let StreamKind::Data { label } = kind {
                        let tx = bl.read().unwrap().get(&label).cloned();
                        if let Some(tx) = tx {
                            let _ = tx.send(ys).await;
                        }
                    }
                });
            }
        });

        Ok(Self { control, bind_labels })
    }

    pub async fn bind(&self, local_port: u16, label: &str) -> Result<(), Error> {
        let (tx, mut rx) = mpsc::channel::<StreamHandle>(64);
        let mut control = self.control.clone();

        {
            let mut map = self.bind_labels.write().unwrap();
            if map.contains_key(label) {
                return Err(Error::Protocol(format!("label '{label}' already bound")));
            }
            map.insert(label.to_string(), tx);
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

        while let Some(mut stream) = rx.recv().await {
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
        Ok(())
    }

    pub async fn forward(&self, listen_port: u16, label: &str) -> Result<(), Error> {
        let control = self.control.clone();
        let label = label.to_string();
        let listener = TcpListener::bind(("127.0.0.1", listen_port)).await?;
        info!(%listen_port, %label, "forwarding");

        loop {
            let (mut incoming, peer) = listener.accept().await?;
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
