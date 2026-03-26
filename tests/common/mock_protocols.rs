//! Mock implementations of the ripcurl protocol traits for testing
//! the transfer orchestration layer sans I/O.

use bytes::Bytes;
use futures_core::Stream;
use ripcurl::protocol::{
    DestinationWriter, ReadOffset, SourceProtocol, SourceReader, TransferError,
};
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;
use url::Url;

/// A transfer source that returns pre-configured readers or errors.
pub struct MockSource {
    results: VecDeque<MockReaderResult>,
    /// Records the `(url, start_byte_offset)` for each `get_reader` call.
    pub get_reader_calls: Vec<(Url, u64)>,
}

pub enum MockReaderResult {
    /// Return a reader that streams the given chunks.
    Ok {
        /// The offset the reader claims to start from.
        offset: u64,
        /// Total resource size (if known).
        total_size: Option<u64>,
        /// Chunks the reader will yield.
        chunks: Vec<Result<Bytes, TransferError>>,
    },
    /// Return an error from the `get_reader` invocation.
    Err(TransferError),
}

impl MockSource {
    pub fn new(results: Vec<MockReaderResult>) -> Self {
        Self {
            results: VecDeque::from(results),
            get_reader_calls: Vec::new(),
        }
    }
}

impl SourceProtocol for MockSource {
    type Reader = MockReader;

    fn get_reader(
        &mut self,
        url: Url,
        start_byte_offset: u64,
    ) -> impl Future<Output = Result<(Self::Reader, ReadOffset), TransferError>> {
        self.get_reader_calls.push((url, start_byte_offset));

        let result = self
            .results
            .pop_front()
            .expect("MockSource: no more scripted results for get_reader");

        async move {
            match result {
                MockReaderResult::Ok {
                    offset,
                    total_size,
                    chunks,
                } => Ok((MockReader { chunks }, ReadOffset { offset, total_size })),
                MockReaderResult::Err(e) => Err(e),
            }
        }
    }
}

/// A reader which yields chunks as results to enable fault injection.
pub struct MockReader {
    chunks: Vec<Result<Bytes, TransferError>>,
}

impl SourceReader for MockReader {
    fn stream_bytes(self) -> impl Stream<Item = Result<Bytes, TransferError>> {
        futures_util::stream::iter(self.chunks)
    }
}

/// The kind of error a MockWriter should produce (without consumed_byte_count,
/// which is auto-computed from the writer's actual state at error time).
pub enum MockErrorKind {
    Transient { reason: String },
    Permanent { reason: String },
}

/// A writer that records all bytes to inspectable in-memory storage, and can inject errors.
pub struct MockWriter {
    /// All bytes successfully written (reset on truncate).
    pub written: Vec<u8>,
    /// If set, the writer will return this error ONLY ONCE,
    /// when written length reaches the threshold.
    /// `consumed_byte_count` is auto-computed from `self.written.len()` at error time.
    error_at: Option<(usize, MockErrorKind)>,
    /// Whether `finalize` was called.
    pub finalized: bool,
    /// Number of times `truncate_and_reset` was called.
    pub truncate_count: u32,
}

impl MockWriter {
    pub fn new() -> Self {
        Self {
            written: Vec::new(),
            error_at: None,
            finalized: false,
            truncate_count: 0,
        }
    }

    /// Configure the writer to fail with a transient error when total bytes written reaches `threshold`.
    /// `consumed_byte_count` is automatically set to the actual bytes written at error time.
    pub fn fail_transiently_at(mut self, threshold: usize, reason: &str) -> Self {
        self.error_at = Some((
            threshold,
            MockErrorKind::Transient {
                reason: reason.into(),
            },
        ));
        self
    }

    /// Configure the writer to fail with a permanent error when total bytes written reaches `threshold`.
    pub fn fail_permanently_at(mut self, threshold: usize, reason: &str) -> Self {
        self.error_at = Some((
            threshold,
            MockErrorKind::Permanent {
                reason: reason.into(),
            },
        ));
        self
    }
}

impl DestinationWriter for MockWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), TransferError> {
        if let Some((threshold, _)) = &self.error_at {
            let threshold = *threshold;
            let bytes_to_write = threshold.saturating_sub(self.written.len());

            if bytes_to_write < bytes.len() {
                self.written.extend_from_slice(&bytes[..bytes_to_write]);
                // SOUNDNESS: There is no await point, so this must happen atomically.
                let (_, kind) = self.error_at.take().unwrap();
                let consumed = self.written.len() as u64;
                return Err(match kind {
                    MockErrorKind::Transient { reason } => TransferError::Transient {
                        consumed_byte_count: consumed,
                        retry_delay: Duration::from_millis(1),
                        reason,
                    },
                    MockErrorKind::Permanent { reason } => TransferError::Permanent { reason },
                });
            }
        }

        self.written.extend_from_slice(bytes);
        Ok(())
    }

    async fn finalize(mut self) -> Result<(), TransferError> {
        self.finalized = true;
        Ok(())
    }

    async fn truncate_and_reset(&mut self) -> Result<(), TransferError> {
        self.written.clear();
        self.truncate_count += 1;
        Ok(())
    }
}
