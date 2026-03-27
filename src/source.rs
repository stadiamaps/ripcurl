use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::protocol::{
    ReadOffset, SourceProtocol, SourceReader, TransferError, http::HttpSourceProtocol,
};
use crate::transfer::TransferConfig;

pub fn resolve_source(url: &Url, config: &TransferConfig) -> Result<Source, TransferError> {
    match url.scheme() {
        "http" | "https" => {
            let headers = build_header_map(&config.custom_http_headers)?;
            Ok(Source::Http(HttpSourceProtocol::new(headers)?))
        }
        scheme => Err(TransferError::Permanent {
            reason: format!("unsupported source protocol: {scheme}"),
        }),
    }
}

/// Convert parsed header pairs into a [`HeaderMap`].
fn build_header_map(headers: &[(String, String)]) -> Result<HeaderMap, TransferError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let header_name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| TransferError::Permanent {
                reason: format!("invalid header name \"{name}\": {e}"),
            })?;
        let header_value = HeaderValue::from_str(value).map_err(|e| TransferError::Permanent {
            reason: format!("invalid header value for \"{name}\": {e}"),
        })?;
        map.append(header_name, header_value);
    }
    Ok(map)
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
