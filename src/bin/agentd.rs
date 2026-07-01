use std::time::Duration;

use clap::Parser;
use tracing::info;

fn parse_pair(s: &str) -> Result<(u16, String), String> {
    let (port, label) = s.split_once(':').ok_or_else(|| {
        format!("expected PORT:LABEL, got '{s}'")
    })?;
    let port: u16 = port.parse().map_err(|e| format!("invalid port '{port}': {e}"))?;
    if label.is_empty() {
        return Err("label cannot be empty".into());
    }
    Ok((port, label.to_string()))
}

#[derive(Parser)]
#[command(version, about = "Label-based TCP tunnel agent")]
struct Cli {
    /// Broker WebSocket address (ws://host:port or wss://host:port).
    #[arg(short, long)]
    server: String,

    /// Register a label and forward incoming yamux streams to a local port.
    /// Can be specified multiple times. Format: PORT:LABEL.
    #[arg(long, value_parser = parse_pair, action = clap::ArgAction::Append)]
    bind: Vec<(u16, String)>,

    /// Listen on a local port and open yamux streams to a registered label.
    /// Can be specified multiple times. Format: PORT:LABEL.
    #[arg(long, value_parser = parse_pair, action = clap::ArgAction::Append)]
    forward: Vec<(u16, String)>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    if cli.bind.is_empty() && cli.forward.is_empty() {
        eprintln!("error: at least one of --bind or --forward is required");
        std::process::exit(1);
    }

    let addr = cli.server;

    loop {
        let agent = loop {
            match agent_pair::AgentClient::connect(&addr).await {
                Ok(a) => break a,
                Err(e) => {
                    tracing::warn!(error = %e, "connect failed, retry in 3s");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        };
        info!("connected");

        let mut hs = vec![];

        for &(port, ref label) in &cli.bind {
            let a = agent.clone();
            let lab = label.clone();
            hs.push(tokio::spawn(async move {
                info!(%port, %lab, "binding");
                a.bind(port, &lab).await
            }));
        }

        for &(port, ref label) in &cli.forward {
            let a = agent.clone();
            let lab = label.clone();
            hs.push(tokio::spawn(async move {
                info!(%port, %lab, "forwarding");
                a.forward(port, &lab).await
            }));
        }

        for h in hs {
            match h.await {
                Ok(Err(e)) => tracing::warn!(error = %e, "task ended"),
                Ok(Ok(())) => {}
                Err(e) => tracing::warn!(error = %e, "join error"),
            }
        }

        tracing::warn!("disconnected, reconnecting in 3s");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
