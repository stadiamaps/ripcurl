//! # HTTP(S) source protocol
//!
//! Implementation of the source protocol using reqwest.
//! Supports byte range requests and conditional headers for resuming interrupted transfers.

use super::{ReadOffset, SourceProtocol, SourceReader, TransferError};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::{StatusCode, redirect::Policy};
use std::time::Duration;
use url::Url;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(5);
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_secs(60);

const MAX_REDIRECTS: usize = 10;

pub struct HttpSourceProtocol {
    client: reqwest::Client,
    /// Custom headers to include in every request.
    custom_headers: reqwest::header::HeaderMap,
    /// Cached metadata from previous responses. `None` before any request completes.
    server_meta: Option<ServerMeta>,
}

/// What we know about the remote server after at least one successful response.
#[derive(Debug, Clone)]
struct ServerMeta {
    etag: Option<String>,
    last_modified: Option<String>,
    supports_ranges: bool,
}

impl ServerMeta {
    /// Extract metadata from a fresh-start response. Consumes any previous state.
    ///
    /// Used for offset-0 requests where resource identity continuity is not required.
    /// `previous` is consumed to prevent accidental reuse; only `supports_ranges`
    /// is preserved (servers don't always repeat the `Accept-Ranges` header).
    fn fresh(response: &reqwest::Response, previous: Option<ServerMeta>) -> Self {
        Self::extract(response, previous.as_ref())
    }

    /// Extract metadata from a resume (206) response, validating continuity with
    /// the previous state. Consumes `previous` so stale state cannot be referenced
    /// after this call.
    ///
    /// Returns `Err(CachedStateConflict)` if the resource appears to have changed.
    fn resume(
        response: &reqwest::Response,
        previous: Option<ServerMeta>,
    ) -> Result<Self, CachedStateConflict> {
        let new = Self::extract(response, previous.as_ref());
        if let Some(old) = previous {
            old.check_continuity(&new)?;
        }
        Ok(new)
    }

    /// Extract header values from a response (shared implementation).
    fn extract(response: &reqwest::Response, previous: Option<&ServerMeta>) -> Self {
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let last_modified = response
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let supports_ranges = response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v != "none")
            .or_else(|| previous.map(|m| m.supports_ranges))
            .unwrap_or(false);

        Self {
            etag,
            last_modified,
            supports_ranges,
        }
    }

    /// Check that `new` is consistent with `self` (resource hasn't changed).
    /// Detects value changes and disappearing headers.
    fn check_continuity(self, new: &ServerMeta) -> Result<(), CachedStateConflict> {
        let mut conflicts = Vec::new();

        if let (Some(old), Some(new)) = (&self.etag, &new.etag)
            && old != new
        {
            conflicts.push(format!("ETag changed ({old} → {new})"));
        }
        if let (Some(old), Some(new)) = (&self.last_modified, &new.last_modified)
            && old != new
        {
            conflicts.push(format!("Last-Modified changed ({old} → {new})"));
        }
        if self.etag.is_some() && new.etag.is_none() {
            conflicts.push("ETag header disappeared from response".to_string());
        }
        if self.last_modified.is_some() && new.last_modified.is_none() {
            conflicts.push("Last-Modified header disappeared from response".to_string());
        }

        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(CachedStateConflict {
                reason: conflicts.join("; "),
            })
        }
    }
}

/// The response headers indicate the resource has changed since the last request.
#[derive(Debug)]
struct CachedStateConflict {
    reason: String,
}

impl std::fmt::Display for CachedStateConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl HttpSourceProtocol {
    pub fn new(custom_headers: reqwest::header::HeaderMap) -> Result<Self, TransferError> {
        let client = reqwest::Client::builder()
            .user_agent(format!("ripcurl/{}", env!("CARGO_PKG_VERSION")))
            // Connect timeout after which we retry
            .connect_timeout(CONNECT_TIMEOUT)
            // Catch stalled connections
            .read_timeout(READ_TIMEOUT)
            // Explicit redirect policy
            .redirect(Policy::limited(MAX_REDIRECTS))
            .build()
            .map_err(|e| TransferError::Permanent {
                reason: format!("failed to build HTTP client: {e}"),
            })?;

        Ok(Self {
            client,
            custom_headers,
            server_meta: None,
        })
    }

    /// Creates a GET request builder with custom headers pre-applied.
    fn build_get(&self, url: Url) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url);
        for (name, value) in &self.custom_headers {
            req = req.header(name, value);
        }
        req
    }

    /// Sends a plain GET request (no Range or conditional headers).
    /// Used for initial requests and as a fallback after 412 or range failure.
    async fn get_from_start(
        &mut self,
        url: Url,
    ) -> Result<(HttpSourceReader, ReadOffset), TransferError> {
        let response = self
            .build_get(url)
            .send()
            .await
            .map_err(|e| map_reqwest_error(e, 0))?;

        let status = response.status();
        if !status.is_success() {
            return Err(map_response_error(&response, 0));
        }

        self.server_meta = Some(ServerMeta::fresh(&response, self.server_meta.take()));
        let total_size = response.content_length();

        Ok((
            HttpSourceReader { response },
            ReadOffset {
                offset: 0,
                total_size,
            },
        ))
    }

    /// Interpret the response to a range request.
    ///
    /// Handles both 206 Partial Content (successful resume) and 200 OK (server
    /// ignored the Range header, doesn't support ranges).
    async fn handle_range_response(
        &mut self,
        url: Url,
        response: reqwest::Response,
        requested_offset: u64,
    ) -> Result<(HttpSourceReader, ReadOffset), TransferError> {
        if response.status() == StatusCode::PARTIAL_CONTENT {
            match ServerMeta::resume(&response, self.server_meta.take()) {
                Ok(meta) => self.server_meta = Some(meta),
                Err(conflict) => {
                    tracing::info!("{conflict} on resumed response. Restarting.");
                    return self.get_from_start(url).await;
                }
            }

            let total_size = parse_content_range_from_response(&response)
                .or_else(|| response.content_length().map(|cl| requested_offset + cl));

            Ok((
                HttpSourceReader { response },
                ReadOffset {
                    offset: requested_offset,
                    total_size,
                },
            ))
        } else {
            // 200 OK: server ignored Range header, doesn't support ranges.
            tracing::info!("Server returned 200 instead of 206. Range requests not supported.");
            let mut meta = ServerMeta::fresh(&response, self.server_meta.take());
            meta.supports_ranges = false;
            self.server_meta = Some(meta);

            let total_size = response.content_length();

            Ok((
                HttpSourceReader { response },
                ReadOffset {
                    offset: 0,
                    total_size,
                },
            ))
        }
    }
}

impl SourceProtocol for HttpSourceProtocol {
    type Reader = HttpSourceReader;

    async fn get_reader(
        &mut self,
        url: Url,
        start_byte_offset: u64,
    ) -> Result<(Self::Reader, ReadOffset), TransferError> {
        match url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(TransferError::Permanent {
                    reason: format!(
                        "unsupported scheme \"{scheme}\" for HTTP protocol (expected http or https)"
                    ),
                });
            }
        }

        // Fresh start — no resume needed.
        if start_byte_offset == 0 {
            return self.get_from_start(url).await;
        }

        // Resume request (bytes_consumed > 0).

        // If we already know the server doesn't support ranges, don't bother trying.
        if self
            .server_meta
            .as_ref()
            .is_some_and(|m| !m.supports_ranges)
        {
            tracing::info!("Server does not support range requests. Restarting from byte 0.");
            return self.get_from_start(url).await;
        }

        // Build request with Range + conditional headers.
        let mut request = self.build_get(url.clone()).header(
            reqwest::header::RANGE,
            format!("bytes={start_byte_offset}-"),
        );

        if let Some(meta) = &self.server_meta {
            if let Some(etag) = &meta.etag {
                request = request.header(reqwest::header::IF_MATCH, etag.as_str());
            }
            if let Some(last_modified) = &meta.last_modified {
                request =
                    request.header(reqwest::header::IF_UNMODIFIED_SINCE, last_modified.as_str());
            }
        }

        let response = request.send().await.map_err(|e| map_reqwest_error(e, 0))?;
        let status = response.status();

        // 412 Precondition Failed means the resource changed since our last request.
        if status == StatusCode::PRECONDITION_FAILED {
            tracing::info!("HTTP 412: resource changed since last request. Restarting.");
            return self.get_from_start(url).await;
        }

        if !status.is_success() {
            return Err(map_response_error(&response, 0));
        }

        self.handle_range_response(url, response, start_byte_offset)
            .await
    }
}

#[derive(Debug)]
pub struct HttpSourceReader {
    response: reqwest::Response,
}

impl SourceReader for HttpSourceReader {
    fn stream_bytes(self) -> impl futures_core::Stream<Item = Result<Bytes, TransferError>> {
        let mut consumed_byte_count: u64 = 0;

        self.response
            .bytes_stream()
            .map(move |result| match result {
                Ok(bytes) => {
                    consumed_byte_count += bytes.len() as u64;
                    Ok(bytes)
                }
                Err(e) => Err(map_reqwest_error(e, consumed_byte_count)),
            })
    }
}

/// Maps a reqwest error to a [`TransferError`].
///
/// `consumed_byte_count` is the number of bytes successfully consumed by the
/// current reader before the error occurred.
fn map_reqwest_error(e: reqwest::Error, consumed_byte_count: u64) -> TransferError {
    // Note: `reqwest::Error` does not expose a matchable enum; the `is_*()` methods
    // are the intended API for classification.
    if e.is_timeout() {
        TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: DEFAULT_RETRY_DELAY,
            reason: format!("request timed out: {e}"),
        }
    } else if e.is_connect() {
        TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: DEFAULT_RETRY_DELAY,
            reason: format!("connection failed: {e}"),
        }
    } else if e.is_body() {
        TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: Duration::from_secs(1),
            reason: format!("error reading response body: {e}"),
        }
    } else if e.is_redirect() {
        TransferError::Permanent {
            reason: format!("too many redirects: {e}"),
        }
    } else if e.is_request() {
        TransferError::Permanent {
            reason: format!("request error: {e}"),
        }
    } else if e.is_decode() {
        TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: Duration::from_secs(1),
            reason: format!("response decode error: {e}"),
        }
    } else {
        tracing::warn!("Unexpected reqwest error: {e}. Please report this.");
        TransferError::Permanent {
            reason: format!("unexpected HTTP error: {e}"),
        }
    }
}

/// Maps an HTTP error response to a [`TransferError`], classifying by status code.
///
/// `consumed_byte_count` is the number of bytes consumed before this error.
fn map_response_error(response: &reqwest::Response, consumed_byte_count: u64) -> TransferError {
    let status = response.status();
    let retry_after = parse_retry_after(response);
    classify_error_status_code(status, retry_after, consumed_byte_count)
}

/// Classify an HTTP error status code into a [`TransferError`].
///
/// Extracted from [`map_response_error`] so it can be unit tested
/// without constructing a `reqwest::Response`.
fn classify_error_status_code(
    status: StatusCode,
    retry_after: Option<Duration>,
    consumed_byte_count: u64,
) -> TransferError {
    if status.as_u16() < 400 {
        tracing::warn!(
            "classify_error_status_code called with unexpected status {status}. Please report this as a bug."
        );
        return TransferError::Permanent {
            reason: format!("unexpected HTTP status {status} in error path"),
        };
    }

    let reason = match http_status_hint(status) {
        Some(hint) => format!("HTTP {status}: {hint}"),
        None => format!("HTTP {status}"),
    };

    match status {
        // Transient client errors
        StatusCode::REQUEST_TIMEOUT => TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: retry_after.unwrap_or(DEFAULT_RETRY_DELAY),
            reason,
        },
        StatusCode::TOO_MANY_REQUESTS => TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: retry_after.unwrap_or(RATE_LIMIT_RETRY_DELAY),
            reason,
        },

        // Transient server errors
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay: retry_after.unwrap_or(DEFAULT_RETRY_DELAY),
            reason,
        },

        // Permanent errors
        _ => TransferError::Permanent { reason },
    }
}

/// Returns a human-friendly description for common HTTP error status codes.
fn http_status_hint(status: StatusCode) -> Option<&'static str> {
    match status {
        StatusCode::BAD_REQUEST => Some("The server rejected the request."),
        StatusCode::UNAUTHORIZED => Some("Authentication is required to access this resource."),
        StatusCode::FORBIDDEN => Some("Access to this resource is denied."),
        StatusCode::NOT_FOUND => {
            Some("The requested resource was not found. Check that the URL is correct.")
        }
        StatusCode::METHOD_NOT_ALLOWED => Some("The HTTP method is not allowed for this resource."),
        StatusCode::GONE => Some("The resource is no longer available at this URL."),
        StatusCode::REQUEST_TIMEOUT => Some("The server timed out waiting for the request."),
        StatusCode::TOO_MANY_REQUESTS => Some("Rate limited by the server."),
        StatusCode::INTERNAL_SERVER_ERROR => Some("The server encountered an internal error."),
        StatusCode::BAD_GATEWAY => Some("The server received an invalid response from upstream."),
        StatusCode::SERVICE_UNAVAILABLE => {
            Some("The server is temporarily unavailable. Try again later.")
        }
        StatusCode::GATEWAY_TIMEOUT => {
            Some("The server timed out waiting for an upstream response.")
        }
        _ => None,
    }
}

/// Parses the `Retry-After` header as either an integer number of seconds
/// or an HTTP-date (RFC 9110 section 10.2.3).
fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    parse_retry_after_value(value)
}

/// Parse a Retry-After header value (integer seconds or HTTP-date).
///
/// Extracted from [`parse_retry_after`] so it can be unit tested
/// without constructing a `reqwest::Response`.
fn parse_retry_after_value(value: &str) -> Option<Duration> {
    // Try integer seconds first (most common).
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    // Try HTTP-date format.
    if let Ok(date) = httpdate::parse_http_date(value) {
        let now = std::time::SystemTime::now();
        return Some(date.duration_since(now).unwrap_or(Duration::ZERO));
    }

    None
}

/// Parse the total resource size from a `Content-Range` header.
///
/// Expected format: `bytes START-END/TOTAL` or `bytes START-END/*`.
/// Returns `None` if the header is absent, malformed, or uses `*` for the total.
fn parse_content_range_from_response(response: &reqwest::Response) -> Option<u64> {
    let value = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)?
        .to_str()
        .ok()?;
    parse_content_range(value)
}

/// Parse the total size from a Content-Range header value string.
fn parse_content_range(value: &str) -> Option<u64> {
    let after_slash = value.rsplit_once('/')?.1.trim();
    if after_slash == "*" {
        return None;
    }
    after_slash.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_status_408_transient() {
        match classify_error_status_code(StatusCode::REQUEST_TIMEOUT, None, 0) {
            TransferError::Transient {
                minimum_retry_delay: retry_delay,
                ..
            } => {
                assert_eq!(retry_delay, DEFAULT_RETRY_DELAY);
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn classify_status_429_transient_with_default_delay() {
        match classify_error_status_code(StatusCode::TOO_MANY_REQUESTS, None, 0) {
            TransferError::Transient {
                minimum_retry_delay: retry_delay,
                ..
            } => {
                assert_eq!(retry_delay, RATE_LIMIT_RETRY_DELAY);
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn classify_status_429_with_retry_after() {
        let custom_delay = Duration::from_secs(10);
        match classify_error_status_code(StatusCode::TOO_MANY_REQUESTS, Some(custom_delay), 0) {
            TransferError::Transient {
                minimum_retry_delay: retry_delay,
                ..
            } => {
                assert_eq!(retry_delay, custom_delay);
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn classify_status_500_transient() {
        assert!(matches!(
            classify_error_status_code(StatusCode::INTERNAL_SERVER_ERROR, None, 0),
            TransferError::Transient { .. }
        ));
    }

    #[test]
    fn classify_status_502_transient() {
        assert!(matches!(
            classify_error_status_code(StatusCode::BAD_GATEWAY, None, 0),
            TransferError::Transient { .. }
        ));
    }

    #[test]
    fn classify_status_504_transient() {
        assert!(matches!(
            classify_error_status_code(StatusCode::GATEWAY_TIMEOUT, None, 0),
            TransferError::Transient { .. }
        ));
    }

    #[test]
    fn classify_status_404_permanent() {
        assert!(matches!(
            classify_error_status_code(StatusCode::NOT_FOUND, None, 0),
            TransferError::Permanent { .. }
        ));
    }

    #[test]
    fn classify_status_403_permanent() {
        assert!(matches!(
            classify_error_status_code(StatusCode::FORBIDDEN, None, 0),
            TransferError::Permanent { .. }
        ));
    }

    #[test]
    fn classify_status_preserves_consumed_bytes() {
        match classify_error_status_code(StatusCode::SERVICE_UNAVAILABLE, None, 12345) {
            TransferError::Transient {
                consumed_byte_count,
                ..
            } => {
                assert_eq!(consumed_byte_count, 12345);
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn parse_retry_after_integer() {
        assert_eq!(parse_retry_after_value("5"), Some(Duration::from_secs(5)));
    }

    #[test]
    fn parse_retry_after_zero() {
        assert_eq!(parse_retry_after_value("0"), Some(Duration::from_secs(0)));
    }

    #[test]
    fn parse_retry_after_garbage() {
        assert_eq!(parse_retry_after_value("not-a-number-or-date"), None);
    }

    #[test]
    fn parse_retry_after_empty() {
        assert_eq!(parse_retry_after_value(""), None);
    }

    #[test]
    fn parse_retry_after_http_date() {
        // A date far in the future should parse to Some(positive duration)
        let result = parse_retry_after_value("Sun, 01 Jan 2090 00:00:00 GMT");
        assert!(result.is_some());
        assert!(result.unwrap() > Duration::ZERO);
    }

    #[test]
    fn parse_retry_after_past_http_date() {
        // A date in the past should parse to Duration::ZERO
        // Sat, 01 Jan 2000 is the correct day-of-week
        let result = parse_retry_after_value("Sat, 01 Jan 2000 00:00:00 GMT");
        assert_eq!(result, Some(Duration::ZERO));
    }

    #[test]
    fn test_classify_404_description() {
        match classify_error_status_code(StatusCode::NOT_FOUND, None, 0) {
            TransferError::Permanent { reason } => {
                assert!(
                    reason.contains("not found"),
                    "expected 'not found', got: {reason}"
                );
                // Should have more than just the bare "HTTP 404 Not Found"
                assert!(
                    reason.len() > "HTTP 404 Not Found".len(),
                    "expected description beyond status, got: {reason}"
                );
            }
            other => panic!("expected Permanent, got: {other:?}"),
        }
    }

    #[test]
    fn test_classify_403_description() {
        match classify_error_status_code(StatusCode::FORBIDDEN, None, 0) {
            TransferError::Permanent { reason } => {
                assert!(
                    reason.to_lowercase().contains("denied"),
                    "expected 'denied', got: {reason}"
                );
            }
            other => panic!("expected Permanent, got: {other:?}"),
        }
    }

    #[test]
    fn test_classify_401_description() {
        match classify_error_status_code(StatusCode::UNAUTHORIZED, None, 0) {
            TransferError::Permanent { reason } => {
                assert!(
                    reason.to_lowercase().contains("auth"),
                    "expected 'auth', got: {reason}"
                );
            }
            other => panic!("expected Permanent, got: {other:?}"),
        }
    }

    #[test]
    fn test_classify_503_description() {
        match classify_error_status_code(StatusCode::SERVICE_UNAVAILABLE, None, 0) {
            TransferError::Transient { reason, .. } => {
                assert!(
                    reason.to_lowercase().contains("unavailable"),
                    "expected 'unavailable', got: {reason}"
                );
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn test_classify_500_description() {
        match classify_error_status_code(StatusCode::INTERNAL_SERVER_ERROR, None, 0) {
            TransferError::Transient { reason, .. } => {
                assert!(
                    reason.to_lowercase().contains("internal error"),
                    "expected 'internal error', got: {reason}"
                );
            }
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn classify_status_below_400_does_not_panic() {
        let result = classify_error_status_code(StatusCode::from_u16(301).unwrap(), None, 0);
        assert!(matches!(result, TransferError::Permanent { .. }));
    }

    #[test]
    fn parse_content_range_total_normal() {
        assert_eq!(parse_content_range("bytes 500-999/5000"), Some(5000));
    }

    #[test]
    fn parse_content_range_total_unknown() {
        assert_eq!(parse_content_range("bytes 0-499/*"), None);
    }

    #[test]
    fn parse_content_range_total_malformed() {
        assert_eq!(parse_content_range("garbage"), None);
    }

    #[test]
    fn parse_content_range_total_zero_offset() {
        assert_eq!(parse_content_range("bytes 0-4999/5000"), Some(5000));
    }
}
