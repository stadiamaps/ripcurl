use std::time::Duration;
use bytes::Bytes;
use futures_core::Stream;
use thiserror::Error;
use url::Url;

pub mod http;
pub mod file;

/// Tagged errors based on severity.
#[derive(Debug, Error)]
pub enum TransferError {
    /// An error which is not fatal to the transfer operation.
    ///
    /// The meaning of transient and permanent is context-dependent,
    /// but for example, a timeout or broken pipe in HTTP
    /// are examples of transient errors.
    #[error("Transient error (retryable after {retry_delay:?}) after consuming {consumed_byte_count} bytes: {reason}.")]
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
    Permanent {
        reason: String,
    },
}

pub struct ByteRange {
    start: u64,
    end: Option<u64>,
}

/// `SourceProtocol`s can be used as a source for a file transfer.
pub trait SourceProtocol {
    type Reader: SourceReader;

    /// Gets a reader for the given request.
    ///
    /// # Implementation notes
    ///
    /// Implementations should generally follow any redirects as needed (protocol-dependent).
    fn get_reader(&self, url: Url, byte_range: Option<ByteRange>) -> Result<Self::Reader, TransferError>;
}

pub trait SourceReader {
    // TODO: Should retry be in here or the source protocol???
    // TODO: Cache ETags etc!!!!
    // TODO: Something with streaming and a retry policy

    /// Streams bytes until an error occurs.
    ///
    /// # Implementation notes
    ///
    /// Implementations can surface errors without implementing retry logic internally.
    fn stream_bytes(&self) -> impl Stream<Item = Result<Bytes, TransferError>>;
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
}