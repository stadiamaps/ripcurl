mod common;

use bytes::Bytes;
use common::mock_protocols::{MockReaderResult, MockSource};
use common::transfer_test::generate_pattern;
use futures_util::StreamExt;
use ripcurl::protocol::TransferError;
use ripcurl::stream::stream_from_source;
use std::pin::pin;
use std::time::Duration;
use url::Url;

fn test_url() -> Url {
    Url::parse("http://test.invalid/file.bin").unwrap()
}

/// Collect all bytes from a stream, returning the concatenated result.
async fn collect_stream(
    stream: impl futures_core::Stream<Item = Result<Bytes, TransferError>>,
) -> Result<Vec<u8>, TransferError> {
    let mut stream = pin!(stream);
    let mut bytes = Vec::new();
    while let Some(result) = stream.next().await {
        bytes.extend_from_slice(&result?);
    }
    Ok(bytes)
}

#[tokio::test]
async fn test_stream_happy_path() {
    tokio::time::pause();

    let data = generate_pattern(100);
    let source = MockSource::new(vec![MockReaderResult::Ok {
        offset: 0,
        total_size: Some(100),
        supports_random_access: true,
        chunks: vec![Ok(Bytes::copy_from_slice(&data))],
    }]);

    let (stream, info) = stream_from_source(source, test_url(), 3).await.unwrap();

    assert_eq!(info.total_size, Some(100));
    let bytes = collect_stream(stream).await.unwrap();
    assert_eq!(bytes, data);
}

#[tokio::test]
async fn test_stream_happy_path_multiple_chunks() {
    tokio::time::pause();

    let data = generate_pattern(100);
    let source = MockSource::new(vec![MockReaderResult::Ok {
        offset: 0,
        total_size: Some(100),
        supports_random_access: true,
        chunks: vec![
            Ok(Bytes::copy_from_slice(&data[..40])),
            Ok(Bytes::copy_from_slice(&data[40..70])),
            Ok(Bytes::copy_from_slice(&data[70..])),
        ],
    }]);

    let (stream, _) = stream_from_source(source, test_url(), 3).await.unwrap();
    let bytes = collect_stream(stream).await.unwrap();
    assert_eq!(bytes, data);
}

#[tokio::test]
async fn test_stream_rejects_non_resumable_source() {
    tokio::time::pause();

    let source = MockSource::new(vec![MockReaderResult::Ok {
        offset: 0,
        total_size: Some(100),
        supports_random_access: false,
        chunks: vec![Ok(Bytes::from_static(b"irrelevant"))],
    }]);

    match stream_from_source(source, test_url(), 3).await {
        Err(TransferError::Permanent { reason }) => {
            assert!(
                reason.contains("random access"),
                "expected 'random access' in error, got: {reason}"
            );
        }
        Err(other) => panic!("expected Permanent error, got: {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[tokio::test]
async fn test_stream_transient_error_then_resume() {
    tokio::time::pause();

    let data = generate_pattern(100);
    let source = MockSource::new(vec![
        // First reader: yields 50 bytes then a transient error
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![
                Ok(Bytes::copy_from_slice(&data[..50])),
                Err(TransferError::Transient {
                    consumed_byte_count: 50,
                    minimum_retry_delay: Duration::from_millis(1),
                    reason: "simulated stream error".into(),
                }),
            ],
        },
        // Retry: resumes from byte 50
        MockReaderResult::Ok {
            offset: 50,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![Ok(Bytes::copy_from_slice(&data[50..]))],
        },
    ]);

    let (stream, _) = stream_from_source(source, test_url(), 3).await.unwrap();
    let bytes = collect_stream(stream).await.unwrap();
    assert_eq!(bytes, data);
}

#[tokio::test]
async fn test_stream_permanent_error_mid_stream() {
    tokio::time::pause();

    let data = generate_pattern(100);
    let source = MockSource::new(vec![MockReaderResult::Ok {
        offset: 0,
        total_size: Some(100),
        supports_random_access: true,
        chunks: vec![
            Ok(Bytes::copy_from_slice(&data[..50])),
            Err(TransferError::Permanent {
                reason: "resource gone".into(),
            }),
        ],
    }]);

    let (stream, _) = stream_from_source(source, test_url(), 3).await.unwrap();

    // The stream should yield 50 bytes, then error.
    let mut stream = pin!(stream);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.len(), 50);

    match stream.next().await {
        Some(Err(TransferError::Permanent { reason })) => {
            assert!(
                reason.contains("resource gone"),
                "expected 'resource gone', got: {reason}"
            );
        }
        other => panic!("expected Permanent error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_stream_retry_exhaustion() {
    tokio::time::pause();

    let source = MockSource::new(vec![
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![Err(TransferError::Transient {
                consumed_byte_count: 0,
                minimum_retry_delay: Duration::from_millis(1),
                reason: "always failing".into(),
            })],
        },
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![Err(TransferError::Transient {
                consumed_byte_count: 0,
                minimum_retry_delay: Duration::from_millis(1),
                reason: "always failing".into(),
            })],
        },
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![Err(TransferError::Transient {
                consumed_byte_count: 0,
                minimum_retry_delay: Duration::from_millis(1),
                reason: "always failing".into(),
            })],
        },
    ]);

    let (stream, _) = stream_from_source(source, test_url(), 2).await.unwrap();

    match collect_stream(stream).await {
        Err(TransferError::Permanent { reason }) => {
            assert!(
                reason.contains("exhausted"),
                "expected 'exhausted' in error, got: {reason}"
            );
        }
        other => panic!("expected Permanent error from retry exhaustion, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_stream_offset_mismatch_on_retry() {
    tokio::time::pause();

    let data = generate_pattern(100);
    let source = MockSource::new(vec![
        // First reader: yields 50 bytes then transient error
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![
                Ok(Bytes::copy_from_slice(&data[..50])),
                Err(TransferError::Transient {
                    consumed_byte_count: 50,
                    minimum_retry_delay: Duration::from_millis(1),
                    reason: "connection reset".into(),
                }),
            ],
        },
        // Retry: server can't resume, returns from offset 0
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(100),
            supports_random_access: true,
            chunks: vec![Ok(Bytes::copy_from_slice(&data))],
        },
    ]);

    let (stream, _) = stream_from_source(source, test_url(), 3).await.unwrap();

    match collect_stream(stream).await {
        Err(TransferError::Permanent { reason }) => {
            assert!(
                reason.contains("Resume failed"),
                "expected 'Resume failed' in error, got: {reason}"
            );
        }
        other => panic!("expected Permanent error from offset mismatch, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_stream_zero_bytes() {
    tokio::time::pause();

    let source = MockSource::new(vec![MockReaderResult::Ok {
        offset: 0,
        total_size: Some(0),
        supports_random_access: true,
        chunks: vec![],
    }]);

    let (stream, info) = stream_from_source(source, test_url(), 3).await.unwrap();
    assert_eq!(info.total_size, Some(0));

    let bytes = collect_stream(stream).await.unwrap();
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn test_stream_get_reader_transient_then_success() {
    tokio::time::pause();

    let data = generate_pattern(50);
    let source = MockSource::new(vec![
        // First get_reader fails transiently
        MockReaderResult::Err(TransferError::Transient {
            consumed_byte_count: 0,
            minimum_retry_delay: Duration::from_millis(1),
            reason: "connection refused".into(),
        }),
        // Retry succeeds
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(50),
            supports_random_access: true,
            chunks: vec![Ok(Bytes::copy_from_slice(&data))],
        },
    ]);

    let (stream, _) = stream_from_source(source, test_url(), 3).await.unwrap();
    let bytes = collect_stream(stream).await.unwrap();
    assert_eq!(bytes, data);
}

#[tokio::test]
async fn test_stream_get_reader_permanent_error() {
    tokio::time::pause();

    let source = MockSource::new(vec![MockReaderResult::Err(TransferError::Permanent {
        reason: "not found".into(),
    })]);

    match stream_from_source(source, test_url(), 3).await {
        Err(TransferError::Permanent { .. }) => {
            // Happy path :)
        }
        Err(other) => panic!("expected Permanent error, got: {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[tokio::test]
async fn test_stream_multiple_transient_recoveries() {
    tokio::time::pause();

    let data = generate_pattern(120);
    let source = MockSource::new(vec![
        // First reader: 40 bytes, then transient
        MockReaderResult::Ok {
            offset: 0,
            total_size: Some(120),
            supports_random_access: true,
            chunks: vec![
                Ok(Bytes::copy_from_slice(&data[..40])),
                Err(TransferError::Transient {
                    consumed_byte_count: 40,
                    minimum_retry_delay: Duration::from_millis(1),
                    reason: "error 1".into(),
                }),
            ],
        },
        // Second reader: 40 more bytes, then transient
        MockReaderResult::Ok {
            offset: 40,
            total_size: Some(120),
            supports_random_access: true,
            chunks: vec![
                Ok(Bytes::copy_from_slice(&data[40..80])),
                Err(TransferError::Transient {
                    consumed_byte_count: 40,
                    minimum_retry_delay: Duration::from_millis(1),
                    reason: "error 2".into(),
                }),
            ],
        },
        // Third reader: final 40 bytes
        MockReaderResult::Ok {
            offset: 80,
            total_size: Some(120),
            supports_random_access: true,
            chunks: vec![Ok(Bytes::copy_from_slice(&data[80..]))],
        },
    ]);

    let (stream, _) = stream_from_source(source, test_url(), 5).await.unwrap();
    let bytes = collect_stream(stream).await.unwrap();
    assert_eq!(bytes, data);
}
