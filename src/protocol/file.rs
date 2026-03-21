//! # File protocol
//!
//! This is the implementation for the file protocol.
//! This assumes a file system that's accessible from the local machine.

use super::{DestinationProtocol, DestinationWriter, TransferError};
use std::io::ErrorKind;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

// TODO: Specify if writes be direct overwrite or tmp atomic
pub struct FileProtocol;

impl FileProtocol {
    pub fn new() -> Self {
        FileProtocol
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

        let path = url.to_file_path().map_err(|_| TransferError::Permanent {
            reason: format!("URL ({url}) does not seem to be a file path"),
        })?;

        let file = File::create_new(path)
            .await
            .map_err(|e| map_io_error(e, 0))?;

        Ok(FileWriter {
            file,
            bytes_written: 0,
        })
    }
}

pub struct FileWriter {
    file: File,
    bytes_written: u64,
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
            .map_err(|e| map_io_error(e, self.bytes_written))
    }
}

fn map_io_error(e: std::io::Error, bytes_written: u64) -> TransferError {
    match e.kind() {
        // Transient failures
        ErrorKind::Interrupted
        | ErrorKind::TimedOut
        | ErrorKind::ResourceBusy
        | ErrorKind::ExecutableFileBusy
        | ErrorKind::Deadlock => TransferError::Transient {
            consumed_byte_count: bytes_written,
            retry_delay: Default::default(),
            reason: e.to_string(),
        },
        // Permanent failures
        ErrorKind::NotFound
        | ErrorKind::PermissionDenied
        | ErrorKind::ConnectionRefused
        | ErrorKind::ReadOnlyFilesystem
        | ErrorKind::AlreadyExists
        | ErrorKind::WouldBlock
        | ErrorKind::NotADirectory
        | ErrorKind::IsADirectory
        | ErrorKind::DirectoryNotEmpty
        | ErrorKind::Unsupported
        | ErrorKind::UnexpectedEof
        | ErrorKind::OutOfMemory
        | ErrorKind::TooManyLinks
        | ErrorKind::InvalidFilename
        | ErrorKind::CrossesDevices
        | ErrorKind::WriteZero
        | ErrorKind::StorageFull
        | ErrorKind::NotSeekable
        | ErrorKind::QuotaExceeded
        | ErrorKind::FileTooLarge
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
