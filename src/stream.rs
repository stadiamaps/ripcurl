//! Streaming public API for consuming source bytes with built-in retry and resume.
//!
//! Unlike [`crate::transfer::execute_transfer`],
//! which writes bytes to a [`crate::destination::Destination`],
//! [`stream_source`] returns a `Stream` that yields byte chunks directly.

use crate::protocol::{SourceProtocol, SourceReader, TransferError};
use crate::retry_transient;
use crate::source::resolve_source;
use crate::transfer::TransferConfig;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use url::Url;

/// Metadata about the stream, discovered during the initial request.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Total size of the resource in bytes, if known.
    pub total_size: Option<u64>,
}

/// Stream bytes from a source URL with automatic persistent retry behavior.
///
/// # Errors
///
/// Returns [`TransferError::Permanent`] if:
/// - The source URL scheme is unsupported
/// - The source does not support random access (e.g. an HTTP server that does not accept byte range requests)
/// - The connection fails with a permanent error
pub async fn stream_from_url(
    source_url: Url,
    config: &TransferConfig,
) -> Result<(impl Stream<Item = Result<Bytes, TransferError>> + Send, StreamInfo), TransferError> {
    match resolve_source(&source_url, config)? {
        crate::source::Source::Http(src) => {
            stream_from_source(src, source_url, config.max_retries).await
        }
    }
}

/// Generic streaming function usable with any [`SourceProtocol`].
///
/// Takes ownership of the source (the stream must own it for retry).
/// Makes the initial request, verifies resume support,
/// then returns a stream backed by [`futures_util::stream::try_unfold`].
pub async fn stream_from_source<S: SourceProtocol + Send + 'static>(
    mut source: S,
    url: Url,
    max_retries: u32,
) -> Result<(impl Stream<Item = Result<Bytes, TransferError>> + Send, StreamInfo), TransferError>
where
    S::Reader: Send,
{
    // Initial request (with retry for transient get_reader failures).
    let (reader, read_offset) =
        retry_transient!(max_retries, source.get_reader(url.clone(), 0))?;

    if !read_offset.supports_random_access {
        return Err(TransferError::Permanent {
            reason: "Source does not support random access \
                     (e.g. no Accept-Ranges header in HTTP response). \
                     Streaming requires random access to recover from transient errors \
                     without data loss."
                .into(),
        });
    }

    let info = StreamInfo {
        total_size: read_offset.total_size,
    };

    let initial_stream: Pin<Box<dyn Stream<Item = Result<Bytes, TransferError>> + Send>> =
        Box::pin(reader.stream_bytes());

    let state = UnfoldState {
        source,
        reader_stream: initial_stream,
        url,
        max_retries,
        retry_count: 0,
        total_bytes_yielded: 0,
    };

    let stream = futures_util::stream::try_unfold(state, unfold_step);

    Ok((stream, info))
}

/// State carried between `try_unfold` steps.
struct UnfoldState<S: SourceProtocol> {
    source: S,
    reader_stream: Pin<Box<dyn Stream<Item = Result<Bytes, TransferError>> + Send>>,
    url: Url,
    max_retries: u32,
    retry_count: u32,
    total_bytes_yielded: u64,
}

/// A single step of the streaming state machine.
///
/// Polls the current reader for the next chunk.
/// On transient errors, performs backoff and obtains a new reader (resume).
/// On permanent errors or stream exhaustion, terminates.
async fn unfold_step<S: SourceProtocol + Send + 'static>(
    mut state: UnfoldState<S>,
) -> Result<Option<(Bytes, UnfoldState<S>)>, TransferError>
where
    S::Reader: Send,
{
    loop {
        match state.reader_stream.next().await {
            Some(Ok(bytes)) => {
                state.total_bytes_yielded += bytes.len() as u64;
                return Ok(Some((bytes, state)));
            }
            Some(Err(TransferError::Transient {
                minimum_retry_delay: server_hint,
                reason,
                ..
            })) => {
                state.retry_count += 1;
                if state.retry_count > state.max_retries {
                    return Err(TransferError::Permanent {
                        reason: format!(
                            "exhausted {} retries (last error: {reason})",
                            state.max_retries
                        ),
                    });
                }

                let delay =
                    crate::transfer::backoff_delay(state.retry_count - 1, server_hint);
                tracing::warn!(
                    "Transient error during streaming on attempt {}/{}: {reason}. \
                     Retrying after {delay:?}.",
                    state.retry_count,
                    state.max_retries
                );
                tokio::time::sleep(delay).await;

                // Get a new reader at the current offset.
                let (reader, read_offset) = retry_transient!(
                    state.max_retries,
                    state.source.get_reader(state.url.clone(), state.total_bytes_yielded)
                )?;

                // In streaming mode, offset mismatch is fatal:
                // we can't replay already-yielded bytes.
                if read_offset.offset != state.total_bytes_yielded {
                    return Err(TransferError::Permanent {
                        reason: format!(
                            "Resume failed: source streaming from offset {} \
                             but {} bytes already yielded to consumer. \
                             Cannot replay already-consumed bytes.",
                            read_offset.offset, state.total_bytes_yielded,
                        ),
                    });
                }

                state.reader_stream = Box::pin(reader.stream_bytes());
                // Loop to read from the new reader.
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        }
    }
}

