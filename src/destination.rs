//! Transfer destinations (where to write the file to)

use crate::protocol::file::{FileProtocol, WriteMode};
use crate::protocol::{DestinationProtocol, DestinationWriter, TransferError};
use crate::transfer::TransferConfig;
use url::Url;

/// A transfer destination.
pub enum Destination {
    /// A file accessible via a local filesystem mount.
    ///
    /// Scheme: `file://`.
    File(FileProtocol),
}

/// Resolve a destination URL to a [`Destination`] protocol handler.
///
/// # Errors
///
/// Returns [`TransferError::Permanent`] if the URL scheme is not supported.
/// Refer to the [`Destination`] enum for supported schemes.
pub fn resolve_destination(
    url: &Url,
    config: &TransferConfig,
) -> Result<Destination, TransferError> {
    let write_mode = if config.overwrite {
        WriteMode::Overwrite
    } else {
        WriteMode::CreateNew
    };

    match url.scheme() {
        "file" => Ok(Destination::File(FileProtocol::new(write_mode))),
        scheme => Err(TransferError::Permanent {
            reason: format!("unsupported destination protocol: {scheme}"),
        }),
    }
}

impl Destination {
    /// Creates a writer which can be used to persist bytes to the destination.
    ///
    /// # Errors
    ///
    /// Propagates errors from the underlying protocol
    /// (e.g. I/O failures or permission issues).
    pub async fn get_writer(&self, url: Url) -> Result<impl DestinationWriter, TransferError> {
        match self {
            Destination::File(proto) => {
                let writer = proto.get_writer(url).await?;
                Ok(writer)
            }
        }
    }
}
