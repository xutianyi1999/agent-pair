pub use validate::validate_label;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_yamux::StreamHandle;

use crate::Error;

/// Maximum allowed postcard frame payload in bytes.
/// StreamKind (enum + String) is well under this.
pub const MAX_FRAME_SIZE: usize = 4096;

/// Maximum label length in bytes.
pub const MAX_LABEL_LENGTH: usize = 4096;

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
}
