//! Event-based DSL for transfer orchestration tests.
//!
//! Tests describe **what happens** during a transfer (bytes transferred, errors encountered)
//! and the framework generates all mock wiring (source attempts, offsets, writer configuration)
//! and validates consistency automatically.
//!
//! Data is auto-generated as a deterministic ascending-byte pattern and verified for integrity
//! on successful transfers.

use bytes::Bytes;
use ripcurl::protocol::{DestinationWriter, TransferError};
use ripcurl::transfer::TransferConfig;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;

use super::mock_protocols::{MockReaderResult, MockSource, MockWriter};

/// Generate a deterministic byte pattern: `[0, 1, 2, ..., 255, 0, 1, ...]`.
fn generate_pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 256) as u8).collect()
}

// Public vocabulary

/// A single event in the transfer scenario.
#[derive(Debug, Clone)]
pub enum Step {
    /// N bytes are successfully persisted to the writer.
    Read(usize),
    /// Source stream fails with a transient (retryable) error.
    /// If no `Read` precedes this step, 0 bytes are transferred before the failure.
    SourceTransient(&'static str),
    /// Source stream fails with a permanent error.
    SourcePermanent(&'static str),
    /// Writer fails with a transient error at the current byte position.
    WriterTransient(&'static str),
    /// Writer fails with a permanent error at the current byte position.
    WriterPermanent(&'static str),
    /// `get_reader` call itself fails with a transient error (retried internally by `retry_transient!`).
    GetReaderTransient(&'static str),
    /// `get_reader` call itself fails with a permanent error.
    GetReaderPermanent(&'static str),
    /// Source cannot resume: destination is truncated, transfer restarts from byte 0.
    /// Resets the running byte total. The next `Read` re-transfers from the beginning.
    Restart,
}

/// Expected outcome of the transfer.
#[derive(Debug, Clone)]
pub enum Expect {
    /// Transfer completes successfully. Total bytes == length.
    Succeeds,
    /// Transfer fails with a permanent error.
    PermanentError,
    /// Transfer fails with a permanent error whose message contains the given substring.
    PermanentErrorContaining(&'static str),
}

// Builder

pub struct TransferTest {
    length: usize,
    steps: Vec<Step>,
    max_retries: u32,
    source_chunk_size: Option<usize>,
    expect: Expect,
}

impl TransferTest {
    pub fn new(length: usize) -> Self {
        Self {
            length,
            steps: Vec::new(),
            max_retries: 3,
            source_chunk_size: None,
            expect: Expect::Succeeds,
        }
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn source_chunk_size(mut self, size: usize) -> Self {
        self.source_chunk_size = Some(size);
        self
    }

    pub fn step(mut self, s: Step) -> Self {
        self.steps.push(s);
        self
    }

    pub fn expect(mut self, e: Expect) -> Self {
        self.expect = e;
        self
    }

    pub async fn run(self) {
        tokio::time::pause();

        let data = generate_pattern(self.length);

        let Compiled {
            results,
            writer_config,
            expected_offsets,
            final_data_cursor,
        } = compile(&data, &self.steps, self.source_chunk_size);

        // Validate step consistency before running
        if matches!(self.expect, Expect::Succeeds) && final_data_cursor != self.length {
            panic!(
                "TransferTest: steps read {final_data_cursor} bytes total but length is {}. \
                 Check your Read() values.",
                self.length
            );
        }

        let mut source = MockSource::new(results);

        let writer = match writer_config {
            Some(WriterConfig::Transient { threshold, reason }) => {
                MockWriter::new().fail_transiently_at(threshold, &reason)
            }
            Some(WriterConfig::Permanent { threshold, reason }) => {
                MockWriter::new().fail_permanently_at(threshold, &reason)
            }
            None => MockWriter::new(),
        };

        // Wrap writer to capture bytes for integrity verification
        let captured = Arc::new(Mutex::new(Vec::new()));
        let verifying_writer = VerifyingWriter {
            inner: writer,
            captured: captured.clone(),
        };

        let config = TransferConfig {
            max_retries: self.max_retries,
            overwrite: false,
        };

        let url = Url::parse("http://test.invalid/file.bin").unwrap();
        let result =
            ripcurl::transfer::run_transfer(&mut source, verifying_writer, url, &config).await;

        // Assert outcome
        match &self.expect {
            Expect::Succeeds => {
                let bytes = result
                    .unwrap_or_else(|e| panic!("expected transfer to succeed, got error: {e:?}"));
                assert_eq!(
                    bytes, self.length as u64,
                    "transfer succeeded but byte count {bytes} != length {}",
                    self.length
                );

                // Data integrity: verify the writer received the exact expected pattern
                let written = captured.lock().unwrap();
                assert_eq!(
                    *written, data,
                    "data integrity check failed: written bytes don't match expected pattern"
                );
            }
            Expect::PermanentError => {
                assert!(
                    matches!(result, Err(TransferError::Permanent { .. })),
                    "expected PermanentError, got: {result:?}"
                );
            }
            Expect::PermanentErrorContaining(substring) => match result {
                Err(TransferError::Permanent { reason }) => {
                    assert!(
                        reason.contains(substring),
                        "expected permanent error containing \"{substring}\", got: \"{reason}\""
                    );
                }
                other => {
                    panic!("expected PermanentError containing \"{substring}\", got: {other:?}")
                }
            },
        }

        // Assert reader call offsets
        let actual_offsets: Vec<u64> = source
            .get_reader_calls
            .iter()
            .map(|(_, offset)| *offset)
            .collect();
        assert_eq!(
            actual_offsets, expected_offsets,
            "get_reader call offsets mismatch"
        );
    }
}

/// Wraps a `MockWriter` to capture written bytes for post-transfer integrity checks.
/// Needed because `run_transfer` consumes the writer via `finalize(self)`.
struct VerifyingWriter {
    inner: MockWriter,
    captured: Arc<Mutex<Vec<u8>>>,
}

impl DestinationWriter for VerifyingWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), TransferError> {
        self.inner.write(bytes).await
    }

    async fn finalize(self) -> Result<(), TransferError> {
        *self.captured.lock().unwrap() = self.inner.written.clone();
        self.inner.finalize().await
    }

    async fn truncate_and_reset(&mut self) -> Result<(), TransferError> {
        self.inner.truncate_and_reset().await
    }
}

// Compilation

#[derive(Debug, Clone)]
enum WriterConfig {
    Transient { threshold: usize, reason: String },
    Permanent { threshold: usize, reason: String },
}

struct Compiled {
    results: Vec<MockReaderResult>,
    writer_config: Option<WriterConfig>,
    expected_offsets: Vec<u64>,
    /// Final data cursor position after processing all steps.
    final_data_cursor: usize,
}

/// An attempt being built. Each attempt becomes one `MockReaderResult`.
struct AttemptBuilder {
    offset: u64,
    total_size: u64,
    chunks: Vec<Result<Bytes, TransferError>>,
}

impl AttemptBuilder {
    fn new(offset: u64, total_size: u64) -> Self {
        Self {
            offset,
            total_size,
            chunks: Vec::new(),
        }
    }

    fn add_data(&mut self, data: &[u8], range: std::ops::Range<usize>, chunk_size: Option<usize>) {
        let slice = &data[range];
        match chunk_size {
            Some(size) if size > 0 => {
                for chunk in slice.chunks(size) {
                    self.chunks.push(Ok(Bytes::copy_from_slice(chunk)));
                }
            }
            _ => {
                if !slice.is_empty() {
                    self.chunks.push(Ok(Bytes::copy_from_slice(slice)));
                }
            }
        }
    }

    fn add_transient_error(&mut self, consumed: u64, reason: &str) {
        self.chunks.push(Err(TransferError::Transient {
            consumed_byte_count: consumed,
            retry_delay: Duration::from_millis(1),
            reason: reason.into(),
        }));
    }

    fn add_permanent_error(&mut self, reason: &str) {
        self.chunks.push(Err(TransferError::Permanent {
            reason: reason.into(),
        }));
    }

    fn build(self) -> MockReaderResult {
        MockReaderResult::Ok {
            offset: self.offset,
            total_size: Some(self.total_size),
            chunks: self.chunks,
        }
    }
}

/// Compile a list of steps into mock configuration.
///
/// Tracks two cursors:
/// - `data_cursor`: position in the data array (for slicing). Resets on `Restart`.
/// - `orch_offset`: what the orchestrator's `total_bytes_written` will be when it calls
///   `get_reader`. Normally equals `data_cursor`, but diverges after `Restart` because
///   the orchestrator doesn't know about the restart until it sees the offset mismatch.
fn compile(data: &[u8], steps: &[Step], chunk_size: Option<usize>) -> Compiled {
    let total_size = data.len() as u64;
    let mut results: Vec<MockReaderResult> = Vec::new();
    let mut expected_offsets: Vec<u64> = Vec::new();
    let mut writer_config: Option<WriterConfig> = None;

    // Where to slice data for the next chunk
    let mut data_cursor: usize = 0;
    // What the orchestrator will pass as start_byte_offset to get_reader
    let mut orch_offset: usize = 0;

    let mut current_attempt: Option<AttemptBuilder> = None;
    let mut restart_pending = false;

    /// Start a new attempt if one isn't already in progress.
    /// Returns mutable reference to the attempt builder.
    fn ensure_attempt<'a>(
        current_attempt: &'a mut Option<AttemptBuilder>,
        expected_offsets: &mut Vec<u64>,
        orch_offset: &mut usize,
        data_cursor: usize,
        restart_pending: &mut bool,
        total_size: u64,
    ) -> &'a mut AttemptBuilder {
        if current_attempt.is_none() {
            expected_offsets.push(*orch_offset as u64);
            let mock_offset = if *restart_pending {
                *restart_pending = false;
                // The orchestrator requests orch_offset, but mock returns 0.
                // After the orchestrator detects the mismatch, it truncates → orch_offset = 0.
                *orch_offset = 0;
                0u64
            } else {
                data_cursor as u64
            };
            *current_attempt = Some(AttemptBuilder::new(mock_offset, total_size));
        }
        current_attempt.as_mut().unwrap()
    }

    for step in steps {
        match step {
            Step::Read(n) => {
                assert!(
                    data_cursor + n <= data.len(),
                    "TransferTest: Read({n}) at position {data_cursor} exceeds data length {}",
                    data.len()
                );
                let attempt = ensure_attempt(
                    &mut current_attempt,
                    &mut expected_offsets,
                    &mut orch_offset,
                    data_cursor,
                    &mut restart_pending,
                    total_size,
                );
                attempt.add_data(data, data_cursor..data_cursor + n, chunk_size);
                data_cursor += n;
                orch_offset += n;
            }

            Step::SourceTransient(reason) => {
                let attempt = ensure_attempt(
                    &mut current_attempt,
                    &mut expected_offsets,
                    &mut orch_offset,
                    data_cursor,
                    &mut restart_pending,
                    total_size,
                );
                attempt.add_transient_error(orch_offset as u64, reason);
                results.push(current_attempt.take().unwrap().build());
            }

            Step::SourcePermanent(reason) => {
                let attempt = ensure_attempt(
                    &mut current_attempt,
                    &mut expected_offsets,
                    &mut orch_offset,
                    data_cursor,
                    &mut restart_pending,
                    total_size,
                );
                attempt.add_permanent_error(reason);
                results.push(current_attempt.take().unwrap().build());
            }

            Step::WriterTransient(reason) => {
                assert!(
                    writer_config.is_none(),
                    "TransferTest: multiple writer errors not supported"
                );
                writer_config = Some(WriterConfig::Transient {
                    threshold: orch_offset,
                    reason: reason.to_string(),
                });

                // Source sends ALL remaining data; the writer fails partway.
                let attempt = ensure_attempt(
                    &mut current_attempt,
                    &mut expected_offsets,
                    &mut orch_offset,
                    data_cursor,
                    &mut restart_pending,
                    total_size,
                );
                if data_cursor < data.len() {
                    attempt.add_data(data, data_cursor..data.len(), chunk_size);
                }
                results.push(current_attempt.take().unwrap().build());
                // data_cursor and orch_offset stay at the writer threshold
            }

            Step::WriterPermanent(reason) => {
                assert!(
                    writer_config.is_none(),
                    "TransferTest: multiple writer errors not supported"
                );
                writer_config = Some(WriterConfig::Permanent {
                    threshold: orch_offset,
                    reason: reason.to_string(),
                });

                let attempt = ensure_attempt(
                    &mut current_attempt,
                    &mut expected_offsets,
                    &mut orch_offset,
                    data_cursor,
                    &mut restart_pending,
                    total_size,
                );
                if data_cursor < data.len() {
                    attempt.add_data(data, data_cursor..data.len(), chunk_size);
                }
                results.push(current_attempt.take().unwrap().build());
            }

            Step::GetReaderTransient(reason) => {
                assert!(
                    current_attempt.is_none(),
                    "TransferTest: GetReaderTransient cannot appear mid-attempt (after Read)"
                );
                expected_offsets.push(orch_offset as u64);
                results.push(MockReaderResult::Err(TransferError::Transient {
                    consumed_byte_count: 0,
                    retry_delay: Duration::from_millis(1),
                    reason: reason.to_string(),
                }));
            }

            Step::GetReaderPermanent(reason) => {
                assert!(
                    current_attempt.is_none(),
                    "TransferTest: GetReaderPermanent cannot appear mid-attempt (after Read)"
                );
                expected_offsets.push(orch_offset as u64);
                results.push(MockReaderResult::Err(TransferError::Permanent {
                    reason: reason.to_string(),
                }));
            }

            Step::Restart => {
                assert!(
                    current_attempt.is_none(),
                    "TransferTest: Restart must follow an error step, not appear mid-attempt"
                );
                // Data cursor resets (we'll re-transfer from byte 0).
                // orch_offset stays: the orchestrator doesn't know about the restart yet.
                // It will be reset to 0 in ensure_attempt when restart_pending is true.
                data_cursor = 0;
                restart_pending = true;
            }
        }
    }

    // Finalize any pending attempt (the last successful attempt)
    if let Some(attempt) = current_attempt {
        results.push(attempt.build());
    }

    Compiled {
        results,
        writer_config,
        expected_offsets,
        final_data_cursor: data_cursor,
    }
}

// Macro

#[macro_export]
macro_rules! transfer_test {
    (
        length: $length:expr,
        $( max_retries: $max_retries:expr, )?
        $( source_chunk_size: $chunk_size:expr, )?
        steps: [$($step:expr),* $(,)?],
        expect: $expect:expr $(,)?
    ) => {{
        use $crate::common::transfer_test::{TransferTest, Step::*, Expect::*};
        #[allow(unused_imports)]
        use $crate::common::transfer_test::Step;
        #[allow(unused_imports)]
        use $crate::common::transfer_test::Expect;
        #[allow(unused_mut)]
        let mut test = TransferTest::new($length);
        $( test = test.max_retries($max_retries); )?
        $( test = test.source_chunk_size($chunk_size); )?
        $(test = test.step($step);)*
        test.expect($expect).run().await
    }};
}
