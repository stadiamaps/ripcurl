use url::Url;

use crate::protocol::{
    ReadOffset, SourceProtocol, SourceReader, TransferError, http::HttpSourceProtocol,
};

// FIXME: This doesn't expose any sort of tuning / user preference.
// We should have a standard way of communicating configuration/prefs to every protocol.
// It's not super clear if this is even the right pattern? You effectively need to pass
// the URL multiple times anyways. Maybe we should just construct the variants explicitly.
pub fn resolve_source(url: &Url) -> Result<Source, TransferError> {
    match url.scheme() {
        "http" | "https" => Ok(Source::Http(HttpSourceProtocol::new()?)),
        scheme => Err(TransferError::Permanent {
            reason: format!("unsupported source protocol: {scheme}"),
        }),
    }
}

/// A transfer source.
pub enum Source {
    Http(HttpSourceProtocol),
}

impl Source {
    /// Gets a reader from the source, starting at a given offset.
    pub async fn get_reader(
        &mut self,
        url: Url,
        start_byte_offset: u64,
    ) -> Result<(impl SourceReader, ReadOffset), TransferError> {
        match self {
            Source::Http(proto) => {
                let (reader, outcome) = proto.get_reader(url, start_byte_offset).await?;
                Ok((reader, outcome))
            }
        }
    }
}
