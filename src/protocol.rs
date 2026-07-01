pub use validate::validate_label;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_yamux::StreamHandle;

use crate::Error;

/// Maximum allowed postcard frame payload in bytes.
/// StreamKind (enum + String) is well under this.
pub const MAX_FRAME_SIZE: usize = 4096;

/// Maximum label length in bytes.
///
/// Must leave room for postcard overhead (1 B variant tag + up to 2 B length varint)
/// so the encoded frame stays within [`MAX_FRAME_SIZE`].
pub const MAX_LABEL_LENGTH: usize = 4000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StreamKind {
    Register { label: String },
    Data { label: String },
}

/// Read one postcard-encoded frame from a yamux stream.
///
/// Frames are length-prefixed: `[2B BE len][postcard StreamKind]`.
pub async fn read_frame(ys: &mut StreamHandle) -> Result<StreamKind, Error> {
    let mut hdr = [0u8; 2];
    ys.read_exact(&mut hdr).await?;
    let flen = u16::from_be_bytes(hdr) as usize;
    if flen > MAX_FRAME_SIZE {
        return Err(Error::Protocol(format!("frame too large: {flen}")));
    }
    let mut buf = vec![0u8; flen];
    ys.read_exact(&mut buf).await?;
    postcard::from_bytes(&buf).map_err(|e| Error::Protocol(e.to_string()))
}

/// Write a postcard-encoded frame to a yamux stream.
pub async fn write_frame(ys: &mut StreamHandle, kind: &StreamKind) -> Result<(), Error> {
    let enc = postcard::to_stdvec(kind).map_err(|e| Error::Protocol(e.to_string()))?;
    let flen = (enc.len() as u16).to_be_bytes();
    ys.write_all(&flen).await?;
    ys.write_all(&enc).await?;
    Ok(())
}

#[cfg(test)]
mod frame_tests {
    use crate::protocol::{write_frame, StreamKind};
    use futures::StreamExt;
    use tokio_yamux::{Config, Session};

    /// Set up two connected yamux sessions and open a stream.
    /// Both sessions run in background tokio tasks.
    async fn yamux_pair() -> tokio_yamux::StreamHandle {
        let (a, b) = tokio::io::duplex(65536);
        tokio::spawn(async move {
            let mut server = Session::new_server(b, Config::default());
            while server.next().await.is_some() {}
        });
        let mut client = Session::new_client(a, Config::default());
        let stream = client.open_stream().unwrap();
        tokio::spawn(async move {
            while client.next().await.is_some() {}
        });
        stream
    }

    #[tokio::test]
    async fn frame_roundtrip_various_labels() {
        for label in &["a", "ab", "项目", "label.with.dots", "x"] {
            let mut ys = yamux_pair().await;
            let kind = StreamKind::Data { label: label.to_string() };
            write_frame(&mut ys, &kind).await.unwrap();
        }
    }

    #[tokio::test]
    async fn register_roundtrip_various_labels() {
        for label in &["srv", "web", "db", "很长标签"] {
            let mut ys = yamux_pair().await;
            let kind = StreamKind::Register { label: label.to_string() };
            write_frame(&mut ys, &kind).await.unwrap();
        }
    }
}

mod validate {
    use crate::protocol::MAX_LABEL_LENGTH;
    use crate::Error;

    pub fn validate_label(label: &str) -> Result<(), Error> {
        if label.is_empty() {
            return Err(Error::Protocol("label cannot be empty".into()));
        }
        if label.len() > MAX_LABEL_LENGTH {
            return Err(Error::Protocol(format!(
                "label too long (max {MAX_LABEL_LENGTH} bytes)"
            )));
        }
        if label.contains('\n') || label.contains('\r') {
            return Err(Error::Protocol("label contains invalid characters".into()));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use crate::protocol::validate::validate_label;

        #[test]
        fn rejects_empty() {
            let r = validate_label("");
            assert!(r.is_err());
            assert!(r.unwrap_err().to_string().contains("empty"));
        }

        #[test]
        fn rejects_newline() {
            let r = validate_label("abc\n123");
            assert!(r.is_err());
        }

        #[test]
        fn rejects_carriage_return() {
            let r = validate_label("abc\r123");
            assert!(r.is_err());
        }

        #[test]
        fn accepts_normal() {
            assert!(validate_label("project_abc").is_ok());
        }

        #[test]
        fn accepts_unicode() {
            assert!(validate_label("项目/プロジェクト").is_ok());
        }

        #[test]
        fn accepts_max_length() {
            let label = "a".repeat(super::super::MAX_LABEL_LENGTH);
            assert!(validate_label(&label).is_ok());
        }

        #[test]
        fn rejects_too_long() {
            let label = "a".repeat(super::super::MAX_LABEL_LENGTH + 1);
            let r = validate_label(&label);
            assert!(r.is_err());
            assert!(r.unwrap_err().to_string().contains("too long"));
        }


    }
}
