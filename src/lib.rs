//! `agent-pair` — label-based TCP tunnel over yamux.
//!
//! A [`Broker`] accepts WebSocket connections from agents. **Bind** agents
//! register a label (e.g. a project ID), and **forward** agents open data
//! streams with that label. The broker bridges the two streams.
//!
//! # Broker (standalone port)
//!
//! ```rust,no_run
//! use agent_pair::Broker;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = Broker::new();
//! broker.listen("0.0.0.0:7799").await?;
//! # Ok(()) }
//! ```
//!
//! # Broker (in-process, with byte-stream transport)
//!
//! Use [`Broker::handle_ws`] when you already have a
//! `Stream<Item = Result<Bytes, E>> + Sink<Bytes, Error = E>` from any
//! source (e.g., an Axum WebSocket handler that maps messages to `Bytes`).
//!
//! # Agent — bind a local port (e.g. PC side)
//!
//! ```rust,no_run
//! use agent_pair::AgentClient;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! AgentClient::connect("relay:7799")
//!     .await?
//!     .bind(5037, "project_abc")
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! # Agent — forward a local port (e.g. Docker side)
//!
//! ```rust,no_run
//! use agent_pair::AgentClient;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! AgentClient::connect("relay:7799")
//!     .await?
//!     .forward(15037, "project_abc")
//!     .await?;
//! # Ok(()) }
//! ```

pub mod bistream;
pub mod protocol;

mod broker;
mod agent;

pub use broker::Broker;
pub use agent::AgentClient;
pub use tokio_yamux::StreamHandle;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn error_debug() {
        assert!(format!("{:?}", Error::Protocol("x".into())).contains("Protocol"));
    }
    #[test]
    fn error_display_io() {
        assert_eq!(Error::Io(std::io::Error::new(std::io::ErrorKind::Other, "b")).to_string(), "I/O error: b");
    }
    #[test]
    fn error_display_protocol() {
        assert_eq!(Error::Protocol("x".into()).to_string(), "protocol error: x");
    }
    #[test]
    fn error_from_io() {
        assert!(matches!(Error::from(std::io::Error::new(std::io::ErrorKind::Other, "x")), Error::Io(_)));
    }
}
