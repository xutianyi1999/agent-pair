use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{future, SinkExt, StreamExt};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::io::{CopyToBytes, SinkWriter, StreamReader};
use tokio_yamux::{Config, Control as YamuxCtrl, Session, StreamHandle};
use tracing::{info, warn};

use crate::protocol::StreamKind;
use crate::bistream::BiStream;
use crate::Error;

type LabelTable = Arc<Mutex<HashMap<String, (YamuxCtrl, u64)>>>;

static SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Broker {
    table: LabelTable,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Accept agent connections on a TCP port, upgrade to WebSocket,
    /// map messages to `Bytes`, then run the yamux session.
    pub async fn listen(&self, addr: impl tokio::net::ToSocketAddrs) -> Result<(), Error> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, peer) = listener.accept().await?;
            let sock = SockRef::from(&stream);
            let ka = TcpKeepalive::new()
                .with_time(Duration::from_secs(30))
                .with_interval(Duration::from_secs(10));
            if let Err(e) = sock.set_tcp_keepalive(&ka) {
                warn!(%peer, error = %e, "set keepalive failed");
            }
            info!(%peer, "connected");

            let ws = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    warn!(%peer, error = %e, "ws handshake failed");
                    continue;
                }
            };

            let (sink, stream) = ws.split();
            let reader = StreamReader::new(stream.filter_map(|msg| async move {
                match msg {
                    Ok(m) => {
                        if m.is_close() {
                            return None;
                        }
                        Some(Ok(m.into_data()))
                    }
                    Err(e) => Some(Err(std::io::Error::other(e.to_string()))),
                }
            }).boxed());
            let writer = SinkWriter::new(CopyToBytes::new(
                sink.sink_map_err(|e| std::io::Error::other(e.to_string()))
                    .with(|data: Bytes| future::ready(Ok::<_, std::io::Error>(Message::Binary(data)))),
            ));

            let table = self.table.clone();
            tokio::spawn(async move {
                agent_session(BiStream { reader, writer }, table).await;
            });
        }
    }

    /// Run a yamux agent session over a byte-stream transport.
    pub async fn run(&self, transport: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) {
        agent_session(transport, self.table.clone()).await;
    }

    /// Close all yamux sessions by clearing the label table.
    /// In-flight agent_session loops see no more controls and exit.
    pub fn shutdown(&self) {
        self.table.lock().unwrap().clear();
    }

    /// Open a yamux stream to a registered bind label.
    pub async fn open_stream(&self, label: &str) -> Result<StreamHandle, Error> {
        let entry = self.table.lock().unwrap().get(label).cloned();
        match entry {
            Some((mut ctrl, _)) => {
                let mut stream = ctrl.open_stream().await
                    .map_err(|e| Error::Protocol(e.to_string()))?;
                crate::protocol::write_frame(&mut stream, &StreamKind::Data { label: label.to_string() }).await?;
                Ok(stream)
            }
            None => Err(Error::Protocol(format!("label '{label}' not registered"))),
        }
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

struct SessionCleanup {
    table: LabelTable,
    session_id: u64,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
        self.table
            .lock()
            .unwrap()
            .retain(|_, (_, sid)| *sid != self.session_id);
    }
}

async fn agent_session(transport: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, table: LabelTable) {
    let mut session = Session::new_server(transport, Config::default());
    let my_control = session.control();
    let session_id = SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let _cleanup = SessionCleanup {
        table: table.clone(),
        session_id,
    };

    let mut err_count = 0u32;
    loop {
        match session.next().await {
            Some(Ok(mut ys)) => {
                err_count = 0;
        let table = table.clone();
        let my_control = my_control.clone();
        tokio::spawn(async move {
            let kind = match crate::protocol::read_frame(&mut ys).await {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "read frame");
                    return;
                }
            };

            match kind {
                StreamKind::Register { label } => {
                    info!(%label, "registered");
                    let lab = label.clone();
                    if let Some(old) = table.lock().unwrap().insert(label, (my_control, session_id)) {
                        warn!("label '{lab}' overwritten (session {})", old.1);
                    }
                }
                StreamKind::Data { label } => {
                    let entry = table.lock().unwrap().get(&label).cloned();
                    match entry {
                        Some((mut ctrl, _)) => {
                            let mut bs = match ctrl.open_stream().await {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(error = %e, "open bind stream");
                                    return;
                                }
                            };
                            if let Err(e) = crate::protocol::write_frame(&mut bs, &StreamKind::Data { label }).await {
                                warn!(error = %e, "write frame to bind agent");
                                return;
                            }
                            if let Err(e) =
                                tokio::io::copy_bidirectional(&mut ys, &mut bs).await
                            {
                                warn!(error = %e, "bridge");
                            }
                        }
                        None => {
                            warn!(%label, "no bind");
                        }
                    }
                }
            }
        });
            }
            Some(Err(_)) => {
                err_count += 1;
                if err_count >= 10 {
                    break;
                }
                continue;
            }
            None => break,
        }
    }
}
