use crate::protocol::file::{FileProtocol, WriteMode};
use crate::protocol::{DestinationProtocol, DestinationWriter, TransferError};
use url::Url;

/// A transfer destination.
pub enum Destination {
    File(FileProtocol),
}

// FIXME: This doesn't expose any sort of tuning / user preference.
// We should have a standard way of communicating configuration/prefs to every protocol.
// It's not super clear if this is even the right pattern. You effectively need to pass
// the URL multiple times anyways. Maybe we should just construct the variants explicitly.
pub fn resolve_destination(url: &Url) -> Result<Destination, TransferError> {
    match url.scheme() {
        "file" => Ok(Destination::File(FileProtocol::new(WriteMode::CreateNew))),
        scheme => Err(TransferError::Permanent {
            reason: format!("unsupported destination protocol: {scheme}"),
        }),
    }
}

impl Destination {
    /// Creates a writer which can be used to persist bytes to the destination.
    pub async fn get_writer(&self, url: Url) -> Result<impl DestinationWriter, TransferError> {
        match self {
            Destination::File(proto) => {
                let writer = proto.get_writer(url).await?;
                Ok(writer)
            }
        }
    }
}
