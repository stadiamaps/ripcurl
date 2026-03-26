mod common;

use ripcurl::protocol::{DestinationWriter, TransferError};

use common::mock_protocols::MockWriter;

#[tokio::test]
async fn test_mock_writer_partial_write_buffer_consistency() {
    let mut writer = MockWriter::new().fail_transiently_at(50, "disk busy");

    // Write a 100-byte chunk; only 50 should be persisted
    let chunk = vec![0xABu8; 100];
    let result = writer.write(&chunk).await;

    assert!(matches!(
        result,
        Err(TransferError::Transient {
            consumed_byte_count: 50,
            ..
        })
    ));
    assert_eq!(writer.written.len(), 50);
    assert!(writer.written.iter().all(|&b| b == 0xAB));
}

#[tokio::test]
async fn test_mock_writer_error_after_multiple_writes() {
    let mut writer = MockWriter::new().fail_transiently_at(80, "disk busy");

    // First write: 50 bytes, under threshold -- succeeds
    let chunk1 = vec![1u8; 50];
    assert!(writer.write(&chunk1).await.is_ok());
    assert_eq!(writer.written.len(), 50);

    // Second write: 50 bytes, would bring total to 100, exceeds threshold at 80
    // Only 30 bytes (80 - 50) should be written
    let chunk2 = vec![2u8; 50];
    let result = writer.write(&chunk2).await;

    assert!(matches!(
        result,
        Err(TransferError::Transient {
            consumed_byte_count: 80,
            ..
        })
    ));
    assert_eq!(writer.written.len(), 80);
    assert!(writer.written[..50].iter().all(|&b| b == 1));
    assert!(writer.written[50..80].iter().all(|&b| b == 2));
}

#[tokio::test]
async fn test_mock_writer_error_at_zero() {
    let mut writer = MockWriter::new().fail_transiently_at(0, "immediate failure");

    let chunk = vec![1u8; 10];
    let result = writer.write(&chunk).await;

    assert!(matches!(
        result,
        Err(TransferError::Transient {
            consumed_byte_count: 0,
            ..
        })
    ));
    assert_eq!(writer.written.len(), 0);
}

#[tokio::test]
async fn test_mock_writer_error_exactly_on_boundary() {
    let mut writer = MockWriter::new().fail_transiently_at(50, "disk busy");

    // Write exactly 50 bytes -- matches threshold exactly
    let chunk = vec![1u8; 50];
    let result = writer.write(&chunk).await;

    // 0 + 50 >= 50 but bytes_to_write = 50 which is NOT < 50, so all bytes written normally
    assert!(result.is_ok());
    assert_eq!(writer.written.len(), 50);

    // The error is still armed; the NEXT write will trigger it
    let chunk2 = vec![2u8; 1];
    let result2 = writer.write(&chunk2).await;
    assert!(matches!(result2, Err(TransferError::Transient { .. })));
    assert_eq!(writer.written.len(), 50); // no additional bytes written
}
