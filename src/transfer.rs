//! Protocol-agnostic transfer orchestration.

use crate::destination::resolve_destination;
use crate::protocol::{
    DestinationProtocol, DestinationWriter, SourceProtocol, SourceReader, TransferError,
};
use crate::source::resolve_source;
use futures_util::StreamExt;
use indicatif::{HumanBytes, HumanDuration, ProgressStyle};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

/// Base delay for exponential backoff (before jitter).
const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Maximum delay cap for computed exponential backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Calculate the retry delay using exponential backoff with jitter.
///
/// The exponential component (`base * 2^attempt ± 25% jitter`) is capped at [`BACKOFF_MAX`].
/// The server-provided `retry_delay` (default zero) is then applied as a hard floor.
/// We never wait *less* than what the server asked for,
/// even if it exceeds our cap (per RFC 9110, `Retry-After` should be honored).
pub(crate) fn backoff_delay(attempt: u32, server_hint: Duration) -> Duration {
    // Exponential component: base * 2^attempt, saturating to avoid overflow.
    let exp = BACKOFF_BASE.saturating_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX));

    // Apply ±25% random jitter to the exponential component only.
    let jitter_frac = rand::random_range(0.75..=1.25);
    let jittered_exp = Duration::from_secs_f64(exp.as_secs_f64() * jitter_frac);

    // Cap our own exponential delay, but not the server hint.
    let capped_exp = jittered_exp.min(BACKOFF_MAX);

    // Server hint is a hard floor — never wait less than the server asked.
    capped_exp.max(server_hint)
}

/// Configuration for a transfer operation.
pub struct TransferConfig {
    /// Maximum number of retry attempts for transient errors.
    pub max_retries: u32,
    /// If true, overwrite existing files at the destination.
    pub overwrite: bool,
    /// Custom HTTP headers to include in source requests (name, value pairs).
    pub custom_http_headers: Vec<(String, String)>,
}

/// Shared progress state for log-based progress reporting.
pub struct ProgressState {
    /// Total number of bytes written so far.
    bytes_written: AtomicU64,
    /// Total expected size in bytes. 0 means unknown.
    total_size: AtomicU64,
    /// The time the transfer started.
    start_time: Instant,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            bytes_written: AtomicU64::new(0),
            total_size: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn update_bytes_written(&self, bytes: u64) {
        self.bytes_written.store(bytes, Ordering::Relaxed);
    }

    fn update_total_size(&self, total: u64) {
        self.total_size.store(total, Ordering::Relaxed);
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Format a progress log line from the current state.
///
/// Returns a human-readable string with percentage (if the total size is known),
/// number of bytes transferred, speed, and ETA.
pub fn format_progress_log(bytes_written: u64, total_size: u64, elapsed: Duration) -> String {
    let speed = if elapsed.as_secs_f64() > 0.0 {
        bytes_written as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let speed_display = HumanBytes(speed as u64);

    if total_size > 0 {
        let pct = (bytes_written as f64 / total_size as f64) * 100.0;
        let eta = if speed > 0.0 {
            let remaining = total_size.saturating_sub(bytes_written);
            let eta_secs = remaining as f64 / speed;
            format!(", ETA {}", HumanDuration(Duration::from_secs_f64(eta_secs)))
        } else {
            String::new()
        };
        format!(
            "Progress: {pct:.1}% ({} / {}) at {speed_display}/s{eta}",
            HumanBytes(bytes_written),
            HumanBytes(total_size),
        )
    } else {
        format!(
            "Progress: {} transferred at {speed_display}/s",
            HumanBytes(bytes_written),
        )
    }
}

/// Retry an async operation on transient errors, using a shared retry budget.
///
/// This is a macro rather than a function because the retried expression often
/// borrows `&mut self` on a captured variable (which prevents
/// the expression from being wrapped in a closure).
#[doc(hidden)]
#[macro_export]
macro_rules! retry_transient {
    ($max_retries:expr, $op:expr) => {{
        let mut n_retries: u32 = 0;
        loop {
            match $op.await {
                Ok(val) => break Ok(val),
                Err($crate::protocol::TransferError::Transient {
                    minimum_retry_delay: server_hint,
                    reason,
                    ..
                }) => {
                    n_retries += 1;
                    if n_retries > $max_retries {
                        break Err($crate::protocol::TransferError::Permanent {
                            reason: format!(
                                "exhausted {} retries (last error: {reason})",
                                $max_retries
                            ),
                        });
                    }
                    let delay = $crate::transfer::backoff_delay(n_retries - 1, server_hint);
                    tracing::warn!(
                        "Transient error on attempt {}/{}: {reason}. Retrying after {delay:?}.",
                        n_retries,
                        $max_retries
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}

/// Execute a transfer from `source_url` to `dest_url`.
///
/// Transient errors are automatically retried,
/// and many interruptions are automatically recovered.
///
/// Returns the total number of bytes written on success.
pub async fn execute_transfer(
    source_url: Url,
    dest_url: Url,
    config: &TransferConfig,
    progress: Option<Arc<ProgressState>>,
) -> Result<u64, TransferError> {
    match (
        resolve_source(&source_url, config)?,
        resolve_destination(&dest_url, config)?,
    ) {
        (crate::source::Source::Http(mut src), crate::destination::Destination::File(dest)) => {
            let writer = retry_transient!(3, dest.get_writer(dest_url.clone()))?;
            run_transfer(&mut src, writer, source_url, config, progress)
                .instrument(tracing::info_span!(
                    "transfer",
                    indicatif.pb_show = tracing::field::Empty
                ))
                .await
        }
    }
}

/// Protocol-agnostic transfer loop.
///
/// Streams bytes from `source` to `writer`, retrying transient errors
/// and handling offset mismatches (e.g. servers that don't support range requests).
pub async fn run_transfer<S: SourceProtocol, W: DestinationWriter>(
    source: &mut S,
    mut writer: W,
    source_url: Url,
    config: &TransferConfig,
    progress: Option<Arc<ProgressState>>,
) -> Result<u64, TransferError> {
    let mut retry_count: u32 = 0;
    let mut total_bytes_written: u64 = 0;

    loop {
        // Ask the source for a reader. The protocol handles all resume logic
        // internally (Range headers, ETags, etc.) and tells us only the byte
        // offset from which the reader will stream.
        let (reader, read_start) = retry_transient!(
            config.max_retries,
            source.get_reader(source_url.clone(), total_bytes_written)
        )?;

        // Configure the progress bar based on whether total size is known.
        let span = Span::current();
        if let Some(total) = read_start.total_size {
            span.pb_set_style(
                &ProgressStyle::with_template(
                    "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                )
                .unwrap()
                .progress_chars("#>-"),
            );
            span.pb_set_length(total);
            span.pb_set_position(total_bytes_written);
            if let Some(ref ps) = progress {
                ps.update_total_size(total);
                ps.update_bytes_written(total_bytes_written);
            }
        } else {
            span.pb_set_style(
                &ProgressStyle::with_template("{spinner:.green} {bytes} ({bytes_per_sec})")
                    .unwrap(),
            );
        }

        // If the source is streaming from a different offset than we expected,
        // the destination must be reset.
        if read_start.offset != total_bytes_written {
            if read_start.offset != 0 && retry_count > config.max_retries {
                return Err(TransferError::Permanent {
                    reason: format!(
                        "Source streaming from offset {} but we requested to start from {total_bytes_written}. This is more degenerate than simply ignoring a streaming offset, so we can't reasonably recover.",
                        read_start.offset
                    ),
                });
            }

            tracing::info!(
                "Source streaming from the start (we requested offset {total_bytes_written}). This source probably does not support range transfers. Restarting the transfer from the start."
            );
            retry_transient!(3, writer.truncate_and_reset())?;

            // Reset counters
            total_bytes_written = 0;
            Span::current().pb_set_position(0);
            if let Some(ref ps) = progress {
                ps.update_bytes_written(0);
            }
        }

        // Stream bytes from reader to writer.
        let mut stream = pin!(reader.stream_bytes());
        let mut stream_failed = false;

        while let Some(result) = stream.next().await {
            match result {
                // Successfully received bytes; try to write and handle failures.
                // TODO: Can we clean this up at all?
                Ok(bytes) => match writer.write(&bytes).await {
                    Ok(()) => {
                        total_bytes_written += bytes.len() as u64;
                        Span::current().pb_set_position(total_bytes_written);
                        if let Some(ref ps) = progress {
                            ps.update_bytes_written(total_bytes_written);
                        }
                    }
                    Err(TransferError::Transient {
                        consumed_byte_count,
                        minimum_retry_delay: server_hint,
                        reason,
                    }) => {
                        // The writer tells us exactly how many bytes it has persisted.
                        // Sync our counter to reality and retry via the outer loop.
                        total_bytes_written = consumed_byte_count;
                        Span::current().pb_set_position(total_bytes_written);
                        if let Some(ref ps) = progress {
                            ps.update_bytes_written(total_bytes_written);
                        }

                        retry_count += 1;
                        if retry_count > config.max_retries {
                            return Err(TransferError::Permanent {
                                reason: format!(
                                    "exhausted {} retries (last error: {reason})",
                                    config.max_retries
                                ),
                            });
                        }

                        let delay = backoff_delay(retry_count - 1, server_hint);
                        tracing::warn!(
                            "Transient write error after {consumed_byte_count} bytes: {reason}. Will resume after {delay:?}."
                        );

                        tokio::time::sleep(delay).await;
                        stream_failed = true;
                        break;
                    }
                    Err(e @ TransferError::Permanent { .. }) => return Err(e),
                },
                // Transient failure streaming
                Err(TransferError::Transient {
                    consumed_byte_count: _,
                    minimum_retry_delay: server_hint,
                    reason,
                }) => {
                    retry_count += 1;
                    if retry_count > config.max_retries {
                        return Err(TransferError::Permanent {
                            reason: format!(
                                "exhausted {} retries (last error: {reason})",
                                config.max_retries
                            ),
                        });
                    }

                    let delay = backoff_delay(retry_count - 1, server_hint);
                    tracing::warn!(
                        "Transient error during streaming on attempt {retry_count}/{}: {reason}. \
                         Retrying after {delay:?}.",
                        config.max_retries
                    );
                    tokio::time::sleep(delay).await;
                    stream_failed = true;
                    break;
                }
                // Permanent failure streaming
                Err(e @ TransferError::Permanent { .. }) => return Err(e),
            }
        }

        if !stream_failed {
            break;
        }
        // Otherwise, loop back to get_reader with the corrected total_bytes_written.
    }

    // At this point, we received all bytes (transfer errors always abort early)!
    // NB: finalize(self) consumes the writer, so transient errors cannot be retried.
    // In practice, file flush/rename failures are rare and typically permanent.
    writer.finalize().await?;

    tracing::info!("Transfer complete: {total_bytes_written} bytes written.");
    Ok(total_bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases_exponentially() {
        // Each successive attempt should produce a larger base delay
        // (before jitter), so even the jittered values should trend upward.
        let hint = Duration::ZERO;
        let d0 = backoff_delay(0, hint);
        let d1 = backoff_delay(1, hint);
        let d2 = backoff_delay(2, hint);
        let d3 = backoff_delay(3, hint);

        // The un-jittered exponential values are 1s, 2s, 4s, 8s.
        // With ±25% jitter the ranges are [0.75, 1.25], [1.5, 2.5], [3.0, 5.0], [6.0, 10.0].
        // So each should be strictly greater than the previous lower-bound.
        assert!(d0 >= Duration::from_millis(750), "d0={d0:?}");
        assert!(d1 > d0, "d1={d1:?} should be > d0={d0:?}");
        assert!(d2 > d1, "d2={d2:?} should be > d1={d1:?}");
        assert!(d3 > d2, "d3={d3:?} should be > d2={d2:?}");
    }

    #[test]
    fn backoff_respects_server_hint_as_floor() {
        let server_hint = Duration::from_secs(30);

        // Attempt 0: exponential would be ~1s, but server says 30s.
        // Jitter only applies to the exponential component, so the
        // server hint is a hard floor — we must never go below it.
        let delay = backoff_delay(0, server_hint);
        assert!(
            delay >= server_hint,
            "delay {delay:?} must be >= server hint {server_hint:?}"
        );
    }

    #[test]
    fn backoff_caps_at_maximum() {
        // Very high attempt number should still be capped at BACKOFF_MAX.
        let delay = backoff_delay(20, Duration::ZERO);
        assert!(
            delay <= BACKOFF_MAX,
            "delay {delay:?} exceeds maximum {BACKOFF_MAX:?}"
        );
    }

    #[test]
    fn backoff_honors_server_hint_beyond_cap() {
        // Server hints beyond BACKOFF_MAX are honored (per RFC 9110).
        let huge_hint = Duration::from_secs(300);
        let delay = backoff_delay(0, huge_hint);
        assert!(
            delay >= huge_hint,
            "delay {delay:?} must honor server hint {huge_hint:?} even beyond cap"
        );
    }

    #[test]
    fn progress_log_with_known_total() {
        let msg = format_progress_log(500_000_000, 1_000_000_000, Duration::from_secs(50));
        assert!(msg.contains("50.0%"), "expected percentage, got: {msg}");
        assert!(msg.contains("MiB"), "expected MiB unit, got: {msg}");
        assert!(msg.contains("/s"), "expected speed, got: {msg}");
        assert!(msg.contains("ETA"), "expected ETA, got: {msg}");
    }

    #[test]
    fn progress_log_unknown_total() {
        let msg = format_progress_log(500_000_000, 0, Duration::from_secs(50));
        assert!(
            msg.contains("transferred"),
            "expected 'transferred', got: {msg}"
        );
        assert!(!msg.contains('%'), "should not have percentage, got: {msg}");
        assert!(!msg.contains("ETA"), "should not have ETA, got: {msg}");
    }

    #[test]
    fn progress_log_zero_elapsed() {
        // Should not panic on zero elapsed time.
        let msg = format_progress_log(0, 1000, Duration::ZERO);
        assert!(msg.contains("0.0%"), "expected 0%, got: {msg}");
    }

    #[test]
    fn progress_log_complete() {
        let msg = format_progress_log(1000, 1000, Duration::from_secs(10));
        assert!(msg.contains("100.0%"), "expected 100%, got: {msg}");
    }
}
