use bytes::Bytes;
use futures_core::Stream;
use std::future::Future;
use std::time::Duration;
use thiserror::Error;
use url::Url;

pub mod file;
pub mod http;

/// Tagged errors based on severity.
#[derive(Debug, Error)]
pub enum TransferError {
    /// An error which is not fatal to the transfer operation.
    ///
    /// The meaning of transient and permanent is context-dependent,
    /// but for example, a timeout or broken pipe in HTTP
    /// are examples of transient errors.
    #[error(
        "Transient error (retryable after {retry_delay:?}) after consuming {consumed_byte_count} bytes: {reason}."
    )]
    Transient {
        /// The number of bytes streamed from the source before the error occurred.
        consumed_byte_count: u64,
        /// The minimum amount of time to wait before retrying the operation.
        ///
        /// The orchestration layer *may* wait longer than this; this is simply a minimum.
        retry_delay: Duration,
        /// The reason for the failure.
        reason: String,
    },
    /// An error which is final; the transfer task cannot be completed or retried.
    ///
    /// For example, an HTTP 403 or 404 are permanent errors.
    /// These errors are not likely to succeed after a simple retry.
    #[error("Permanent error: {reason}")]
    Permanent { reason: String },
}

/// Describes where a reader returned by [`SourceProtocol::get_reader`] will stream from.
///
/// If `offset` differs from the requested `start_byte_offset`, the orchestration
/// layer must adjust the destination accordingly (e.g. truncate and restart).
#[derive(Debug)]
pub struct ReadOffset {
    /// The byte offset from which the reader will stream.
    pub offset: u64,
    /// Total size of the resource, if known.
    pub total_size: Option<u64>,
}

/// `SourceProtocol`s can be used as a source for a file transfer.
pub trait SourceProtocol {
    type Reader: SourceReader;

    /// Gets a reader for the given request.
    ///
    /// `start_byte_offset` is the offset from which to start reading.
    /// This allows for resuming a partial transfer.
    ///
    /// The protocol manages its own integrity-checking state (ETags, mtimes, etc.)
    /// internally via `&mut self`.
    ///
    /// # Implementation notes
    ///
    /// Implementations should generally follow any redirects as needed (protocol-dependent).
    fn get_reader(
        &mut self,
        url: Url,
        start_byte_offset: u64,
    ) -> impl Future<Output = Result<(Self::Reader, ReadOffset), TransferError>>;
}

pub trait SourceReader {
    /// Streams bytes until an error occurs or the source is exhausted.
    ///
    /// Consumes the reader into a stream that yields bytes as they become available.
    /// The orchestration layer handles retries transparently for all implementations,
    /// requesting a new reader (with an updated byte range) from the protocol.
    fn stream_bytes(self) -> impl Stream<Item = Result<Bytes, TransferError>>;
}

/// `SourceProtocol`s can be used as a source for a file transfer.
pub trait DestinationProtocol {
    type Writer: DestinationWriter;

    fn get_writer(&self, url: Url) -> impl Future<Output = Result<Self::Writer, TransferError>>;
}

/// An asynchronous writer.
///
/// Bytes must be sequentially from start to end.
pub trait DestinationWriter {
    /// Writes some bytes to the destination.
    ///
    /// # Implementation notes
    ///
    /// This method does not need to guarantee that bytes have been "flushed"
    /// or otherwise persisted to the destination.
    /// This method may perform internal buffering.
    ///
    /// Implementations should use asynchronous (non-blocking) I/O.
    fn write(&mut self, bytes: &[u8]) -> impl Future<Output = Result<(), TransferError>>;

    /// Finalize the write operation.
    ///
    /// # Implementation notes
    ///
    /// When this method returns, the implementation should guarantee that the write has completed,
    /// at least to the best of its knowledge (e.g. a file must be flushed and closed,
    /// the API call must have completed successfully, etc.).
    fn finalize(self) -> impl Future<Output = Result<(), TransferError>>;

    /// Truncate the destination and reset the write position to the beginning.
    ///
    /// Used by the orchestration layer when a resume attempt fails (e.g. the
    /// server doesn't support range requests) and the transfer must restart.
    fn truncate_and_reset(&mut self) -> impl Future<Output = Result<(), TransferError>>;
}
