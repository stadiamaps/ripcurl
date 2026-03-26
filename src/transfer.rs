//! Protocol-agnostic transfer orchestration.

use crate::destination::resolve_destination;
use crate::protocol::{
    DestinationProtocol, DestinationWriter, SourceProtocol, SourceReader, TransferError,
};
use crate::source::resolve_source;
use futures_util::StreamExt;
use indicatif::ProgressStyle;
use std::pin::pin;
use std::time::Duration;
use tracing::{Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

/// Configuration for a transfer operation.
pub struct TransferConfig {
    /// Maximum number of retry attempts for transient errors.
    pub max_retries: u32,
    /// If true, overwrite existing files at the destination.
    pub overwrite: bool,
}

/// Retry an async operation on transient errors, using a shared retry budget.
///
/// This is a macro rather than a function because the retried expression often
/// borrows `&mut self` on a captured variable.
macro_rules! retry_transient {
    ($max_retries:expr, $op:expr) => {{
        let mut n_retries = 0;
        loop {
            match $op.await {
                Ok(val) => break Ok(val),
                Err(TransferError::Transient {
                    retry_delay,
                    reason,
                    ..
                }) => {
                    n_retries += 1;
                    if n_retries > $max_retries {
                        break Err(TransferError::Permanent {
                            reason: format!(
                                "exhausted {} retries (last error: {reason})",
                                $max_retries
                            ),
                        });
                    }
                    let retry_delay = retry_delay.min(Duration::from_secs(3));
                    tracing::warn!(
                        "Transient error on attempt {}/{}: {reason}. Retrying after {retry_delay:?}.",
                        n_retries, $max_retries
                    );
                    tokio::time::sleep(retry_delay).await;
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
) -> Result<u64, TransferError> {
    match (
        resolve_source(&source_url, config)?,
        resolve_destination(&dest_url, config)?,
    ) {
        (crate::source::Source::Http(mut src), crate::destination::Destination::File(dest)) => {
            let writer = retry_transient!(3, dest.get_writer(dest_url.clone()))?;
            run_transfer(&mut src, writer, source_url, config)
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
            total_bytes_written = 0;
            Span::current().pb_set_position(0);
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
                    }
                    Err(TransferError::Transient {
                        consumed_byte_count,
                        retry_delay,
                        reason,
                    }) => {
                        // The writer tells us exactly how many bytes it has persisted.
                        // Sync our counter to reality and retry via the outer loop.
                        total_bytes_written = consumed_byte_count;
                        Span::current().pb_set_position(total_bytes_written);

                        retry_count += 1;
                        if retry_count > config.max_retries {
                            return Err(TransferError::Permanent {
                                reason: format!(
                                    "exhausted {} retries (last error: {reason})",
                                    config.max_retries
                                ),
                            });
                        }

                        tracing::warn!(
                            "Transient write error after {consumed_byte_count} bytes: {reason}. Will resume after {retry_delay:?}."
                        );

                        tokio::time::sleep(retry_delay).await;
                        stream_failed = true;
                        break;
                    }
                    Err(e @ TransferError::Permanent { .. }) => return Err(e),
                },
                // Transient failure streaming
                Err(TransferError::Transient {
                    consumed_byte_count: _,
                    retry_delay,
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

                    tracing::warn!(
                        "Transient error during streaming on attempt {retry_count}/{}: {reason}. \
                         Retrying after {retry_delay:?}.",
                        config.max_retries
                    );
                    tokio::time::sleep(retry_delay).await;
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
