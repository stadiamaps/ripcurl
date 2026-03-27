//! # File protocol
//!
//! This is the implementation for the file protocol.
//! This assumes a file system that's accessible from the local machine.

use super::{DestinationProtocol, DestinationWriter, TransferError};
use std::io::ErrorKind;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

/// Controls how the file destination writes data.
pub enum WriteMode {
    /// Write directly to the target path. Fails if the file already exists.
    CreateNew,
    /// Write directly to the target path. Overwrites if the file already exists.
    Overwrite,
    // TODO: Atomic: writes to a temp file and then renames atomically on finalize. Failed/corrupt clean up the temp file.
}

pub struct FileProtocol {
    mode: WriteMode,
}

impl FileProtocol {
    pub fn new(mode: WriteMode) -> Self {
        FileProtocol { mode }
    }
}

impl DestinationProtocol for FileProtocol {
    type Writer = FileWriter;

    async fn get_writer(&self, url: Url) -> Result<Self::Writer, TransferError> {
        if url.scheme() != "file" {
            return Err(TransferError::Permanent {
                reason: format!("unsupported scheme {} for file protocol", url.scheme()),
            });
        };

        let final_path = url.to_file_path().map_err(|_| TransferError::Permanent {
            reason: format!("URL ({url}) does not seem to be a file path"),
        })?;

        let file = match &self.mode {
            WriteMode::CreateNew => File::create_new(&final_path)
                .await
                .map_err(|e| map_io_error(e, 0))?,
            WriteMode::Overwrite => File::create(&final_path)
                .await
                .map_err(|e| map_io_error(e, 0))?,
        };

        Ok(FileWriter {
            file,
            path: final_path,
            bytes_written: 0,
            finalized: false,
        })
    }
}

pub struct FileWriter {
    file: File,
    path: std::path::PathBuf,
    bytes_written: u64,
    finalized: bool,
}

impl DestinationWriter for FileWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), TransferError> {
        let mut bytes = bytes;

        while !bytes.is_empty() {
            match self
                .file
                .write(bytes)
                .await
                .map_err(|e| map_io_error(e, self.bytes_written))
            {
                Ok(n) => {
                    if n == 0 {
                        return Err(TransferError::Permanent {
                            reason: "attempted write, but zero bytes consumed (this is unlikely to resolve itself)".to_string(),
                        });
                    }

                    self.bytes_written += n as u64;
                    bytes = &bytes[n..];
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn finalize(mut self) -> Result<(), TransferError> {
        self.file
            .flush()
            .await
            .map_err(|e| map_io_error(e, self.bytes_written))?;

        self.finalized = true;
        Ok(())
    }

    async fn truncate_and_reset(&mut self) -> Result<(), TransferError> {
        self.file
            .set_len(0)
            .await
            .map_err(|e| map_io_error(e, self.bytes_written))?;
        self.file
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| map_io_error(e, self.bytes_written))?;
        self.bytes_written = 0;
        Ok(())
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if !self.finalized {
            match std::fs::remove_file(&self.path) {
                Ok(()) => {
                    tracing::info!(
                        path = %self.path.display(),
                        "Cleaned up partial file (transfer was not finalized)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        path = %self.path.display(),
                        error = %e,
                        "Failed to clean up partial file"
                    );
                }
            }
        }
    }
}

/// Maps an I/O error to a [`TransferError`], classifying by error kind.
fn map_io_error(e: std::io::Error, bytes_written: u64) -> TransferError {
    match e.kind() {
        // Transient failures
        ErrorKind::Interrupted
        | ErrorKind::TimedOut
        | ErrorKind::ResourceBusy
        | ErrorKind::ExecutableFileBusy
        | ErrorKind::Deadlock => TransferError::Transient {
            consumed_byte_count: bytes_written,
            minimum_retry_delay: Default::default(),
            reason: e.to_string(),
        },
        // Permanent failures with human-friendly messages
        ErrorKind::AlreadyExists => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The destination file already exists. Use --overwrite if you want to write over it."
                    .to_string(),
            }
        }
        ErrorKind::NotFound => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The path does not exist. Check that all parent directories are valid."
                    .to_string(),
            }
        }
        ErrorKind::PermissionDenied => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "Permission denied. Check file and directory permissions.".to_string(),
            }
        }
        ErrorKind::ReadOnlyFilesystem => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The filesystem is read-only.".to_string(),
            }
        }
        ErrorKind::StorageFull => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "No space left on the disk.".to_string(),
            }
        }
        ErrorKind::QuotaExceeded => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "Disk quota exceeded.".to_string(),
            }
        }
        ErrorKind::FileTooLarge => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The file is too large for the filesystem.".to_string(),
            }
        }
        ErrorKind::IsADirectory => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The destination is a directory, not a file.".to_string(),
            }
        }
        ErrorKind::NotADirectory => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "A component of the path is not a directory.".to_string(),
            }
        }
        ErrorKind::InvalidFilename => {
            tracing::debug!("IO error detail: {e}");
            TransferError::Permanent {
                reason: "The filename is invalid.".to_string(),
            }
        }
        // Remaining permanent failures — uncommon or already reasonably descriptive.
        ErrorKind::ConnectionRefused
        | ErrorKind::WouldBlock
        | ErrorKind::DirectoryNotEmpty
        | ErrorKind::Unsupported
        | ErrorKind::UnexpectedEof
        | ErrorKind::OutOfMemory
        | ErrorKind::TooManyLinks
        | ErrorKind::CrossesDevices
        | ErrorKind::WriteZero
        | ErrorKind::NotSeekable
        // The stale network file handle is not obviously retryable,
        // so we mark this as a permanent failure.
        // Any retry would need to be from scratch as we don't necessarily know how many bytes
        // were successfully written.
        | ErrorKind::StaleNetworkFileHandle
        | ErrorKind::Other => TransferError::Permanent {
            reason: e.to_string(),
        },
        // Known error variants that are not expected with file operations.
        ErrorKind::ConnectionReset
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkUnreachable
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::AddrInUse
        | ErrorKind::AddrNotAvailable
        | ErrorKind::NetworkDown
        | ErrorKind::BrokenPipe
        | ErrorKind::InvalidInput
        | ErrorKind::InvalidData
        | ErrorKind::ArgumentListTooLong => {
            tracing::warn!(
                "Unexpected error of kind {} encountered during file I/O. Please report this.",
                e.kind()
            );
            TransferError::Permanent {
                reason: e.to_string(),
            }
        }
        _ => {
            tracing::warn!(
                "Error of unhandled kind {} encountered during file I/O. Please report this.",
                e.kind()
            );
            TransferError::Permanent {
                reason: e.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_io_error_preserves_bytes_written() {
        let err = std::io::Error::new(ErrorKind::Interrupted, "interrupted");
        match map_io_error(err, 42) {
            TransferError::Transient {
                consumed_byte_count,
                ..
            } => assert_eq!(consumed_byte_count, 42),
            other => panic!("expected Transient, got: {other:?}"),
        }
    }

    #[test]
    fn test_map_io_error_friendly() {
        let err = std::io::Error::new(ErrorKind::AlreadyExists, "File exists");
        match map_io_error(err, 0) {
            TransferError::Permanent { reason } => {
                assert!(
                    reason.contains("already exists"),
                    "expected 'already exists', got: {reason}"
                );
                assert!(
                    reason.contains("--overwrite"),
                    "expected '--overwrite' hint, got: {reason}"
                );
            }
            other => panic!("expected Permanent, got: {other:?}"),
        }
    }
}
