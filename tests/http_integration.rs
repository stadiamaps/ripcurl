mod common;

use common::test_server::{RequestRule, ServerConfig, TestServer};
use futures_util::StreamExt;
use ripcurl::protocol::http::HttpSourceProtocol;
use ripcurl::protocol::{SourceProtocol, SourceReader, TransferError};
use std::pin::pin;
use std::time::Duration;

/// Collect all bytes from a SourceReader.
async fn collect_reader(reader: impl SourceReader) -> Result<Vec<u8>, TransferError> {
    let mut bytes = Vec::new();
    let mut stream = pin!(reader.stream_bytes());
    while let Some(result) = stream.next().await {
        bytes.extend_from_slice(&result?);
    }
    Ok(bytes)
}

#[tokio::test]
async fn test_basic_download() {
    let content_size = 10_000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: true,
        }],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();
    let (reader, offset) = source.get_reader(server.url("/file"), 0).await.unwrap();

    assert_eq!(offset.offset, 0);
    assert_eq!(offset.total_size, Some(content_size as u64));

    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    // Verify content correctness
    let expected = common::test_server::generate_content(content_size);
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn test_range_request_resume() {
    let content_size = 10_000;
    let server = TestServer::start(ServerConfig::new(content_size, vec![]).with_fallback(
        RequestRule::Serve {
            support_ranges: true,
        },
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First request: get first 5000 bytes
    let (reader, offset) = source.get_reader(server.url("/file"), 0).await.unwrap();
    assert_eq!(offset.offset, 0);
    let first_bytes = collect_reader(reader).await.unwrap();
    assert_eq!(first_bytes.len(), content_size);

    // Second request: resume from byte 5000
    let (reader, offset) = source.get_reader(server.url("/file"), 5000).await.unwrap();
    assert_eq!(offset.offset, 5000);

    let remaining_bytes = collect_reader(reader).await.unwrap();
    assert_eq!(remaining_bytes.len(), 5000);

    // Verify the resumed bytes match the expected content
    let expected = common::test_server::generate_content(content_size);
    assert_eq!(remaining_bytes, &expected[5000..]);

    // Verify server received Range header
    let headers = server.last_request_headers("/file").unwrap();
    assert!(headers.get("range").is_some());
}

#[tokio::test]
async fn test_server_no_range_support() {
    let content_size = 1000;
    let server = TestServer::start(
        ServerConfig::new(content_size, vec![]).with_fallback(RequestRule::ServeIgnoringRange),
    )
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First request establishes connection
    let (reader, offset) = source.get_reader(server.url("/file"), 0).await.unwrap();
    assert_eq!(offset.offset, 0);
    let _ = collect_reader(reader).await.unwrap();

    // Second request: ask for offset 500 but server will return full content (200)
    let (reader, offset) = source.get_reader(server.url("/file"), 500).await.unwrap();
    // Server returned 200 instead of 206, so offset should be 0
    assert_eq!(offset.offset, 0);

    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);
}

#[tokio::test]
async fn test_503_then_success() {
    // Do NOT use tokio::time::pause() in tests that do real HTTP I/O.
    // Paused time makes reqwest's connect/read timeouts fire immediately or hang
    // indefinitely, since they rely on real wall-clock time, not tokio's virtual clock.
    // Only use pause() with fully-mocked protocols (e.g. transfer_orchestration tests).

    let content_size = 1000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![
            RequestRule::Error {
                status: 503,
                retry_after: Some("0".to_string()),
            },
            RequestRule::Error {
                status: 503,
                retry_after: Some("0".to_string()),
            },
            RequestRule::Serve {
                support_ranges: true,
            },
        ],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First two calls should fail with transient errors
    let err = source.get_reader(server.url("/file"), 0).await.unwrap_err();
    assert!(matches!(err, TransferError::Transient { .. }));

    let err = source.get_reader(server.url("/file"), 0).await.unwrap_err();
    assert!(matches!(err, TransferError::Transient { .. }));

    // Third call should succeed
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    assert_eq!(server.request_count("/file"), 3);
}

#[tokio::test]
async fn test_429_with_retry_after() {
    let server = TestServer::start(ServerConfig::new(
        100,
        vec![
            RequestRule::Error {
                status: 429,
                retry_after: Some("1".to_string()),
            },
            RequestRule::Serve {
                support_ranges: true,
            },
        ],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First call: 429
    let err = source.get_reader(server.url("/file"), 0).await.unwrap_err();
    match err {
        TransferError::Transient { retry_delay, .. } => {
            assert_eq!(retry_delay, Duration::from_secs(1));
        }
        other => panic!("expected Transient, got: {other:?}"),
    }

    // Second call: success
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), 100);
}

#[tokio::test]
async fn test_connection_drop_mid_stream() {
    let content_size = 10_000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::PartialThenDrop {
            bytes_before_drop: 5000,
        }],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();
    let result = source.get_reader(server.url("/file"), 0).await;

    // The server sends valid HTTP headers (200 OK, Content-Length: 10000) but truncates
    // the body at 5000 bytes. Depending on timing and TCP buffering, the connection
    // closure can be detected at two different levels:
    //
    // 1. get_reader succeeds (headers received), then the body stream yields partial
    //    data followed by an error or premature EOF.
    // 2. get_reader itself fails if the connection reset propagates before reqwest
    //    finishes processing the response.
    //
    // Both are valid outcomes of a mid-transfer connection drop.
    match result {
        Ok((reader, _)) => {
            let mut total = 0u64;
            let mut stream = pin!(reader.stream_bytes());
            let mut got_error = false;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(bytes) => total += bytes.len() as u64,
                    Err(_) => {
                        got_error = true;
                        break;
                    }
                }
            }

            assert!(
                got_error || total < content_size as u64,
                "should either get a stream error or fewer bytes ({total}) than expected ({content_size})"
            );
        }
        Err(err) => {
            // Connection reset surfaced at request level — still a valid failure mode.
            assert!(
                matches!(
                    err,
                    TransferError::Transient { .. } | TransferError::Permanent { .. }
                ),
                "unexpected error variant: {err:?}"
            );
        }
    }
}

#[tokio::test]
async fn test_404_permanent() {
    let server = TestServer::start(ServerConfig::new(
        100,
        vec![RequestRule::Error {
            status: 404,
            retry_after: None,
        }],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();
    let err = source.get_reader(server.url("/file"), 0).await.unwrap_err();

    assert!(matches!(err, TransferError::Permanent { .. }));
}

#[tokio::test]
async fn test_etag_included_on_resume() {
    let content_size = 1000;
    let server = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_fallback(RequestRule::Serve {
                support_ranges: true,
            })
            .with_etag(Some("\"my-etag-123\"".to_string())),
    )
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First request to cache the ETag
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Second request should include If-Match
    let (reader, _) = source.get_reader(server.url("/file"), 500).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    let headers = server.last_request_headers("/file").unwrap();
    let if_match = headers
        .get("if-match")
        .expect("If-Match header should be present on resume")
        .to_str()
        .unwrap();
    assert_eq!(if_match, "\"my-etag-123\"");
}

#[tokio::test]
async fn test_redirect_chain() {
    let content_size = 500;
    let server = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_path_rules(
                "/start",
                vec![RequestRule::Redirect {
                    status: 302,
                    location: "/middle".to_string(),
                }],
            )
            .with_path_rules(
                "/middle",
                vec![RequestRule::Redirect {
                    status: 301,
                    location: "/final".to_string(),
                }],
            )
            .with_path_rules(
                "/final",
                vec![RequestRule::Serve {
                    support_ranges: true,
                }],
            ),
    )
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // Request to /start should follow redirects and get the content from /final
    let (reader, offset) = source.get_reader(server.url("/start"), 0).await.unwrap();
    assert_eq!(offset.offset, 0);

    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    let expected = common::test_server::generate_content(content_size);
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn test_redirect_to_different_server() {
    let content_size = 500;

    // Second server serves the actual content
    let server2 = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: true,
        }],
    ))
    .await;

    // First server redirects to the second server
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_path_rules(
        "/download",
        vec![RequestRule::Redirect {
            status: 302,
            location: server2.url("/file").to_string(),
        }],
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();
    let (reader, _) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();

    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    let expected = common::test_server::generate_content(content_size);
    assert_eq!(bytes, expected);
}

// Regression test:
// `.expect()` used to panic when `get_reader(_, 0)` is called a second time
// with cached state from a previous request and the server returns a different ETag.
//
// Scenario: initial request caches ETag, resume attempt gets 200 (server ignores Range),
// retry from offset 0 sees a different ETag → current code panics in `.expect()`.
#[tokio::test]
async fn test_etag_change_after_range_failure_does_not_panic() {
    let content_size = 1000;
    let server = TestServer::start(
        ServerConfig::new(
            content_size,
            vec![
                // Step 1: initial request — serves with ETag v1
                RequestRule::WithHeaders {
                    etag: Some("\"etag-v1\"".to_string()),
                    last_modified: None,
                    then: Box::new(RequestRule::Serve {
                        support_ranges: true,
                    }),
                },
                // Step 2: resume attempt — server ignores Range, returns 200 with ETag v1
                RequestRule::WithHeaders {
                    etag: Some("\"etag-v1\"".to_string()),
                    last_modified: None,
                    then: Box::new(RequestRule::ServeIgnoringRange),
                },
                // Step 3: retry from 0 — resource has changed, new ETag
                RequestRule::WithHeaders {
                    etag: Some("\"etag-v2\"".to_string()),
                    last_modified: None,
                    then: Box::new(RequestRule::Serve {
                        support_ranges: true,
                    }),
                },
            ],
        )
        .with_etag(None)
        .with_last_modified(None),
    )
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // Step 1: initial download, caches ETag v1
    let (reader, offset) = source.get_reader(server.url("/file"), 0).await.unwrap();
    assert_eq!(offset.offset, 0);
    let _ = collect_reader(reader).await.unwrap();

    // Step 2: resume from 500, server returns 200 (ignores Range) → offset resets to 0
    let (reader, offset) = source.get_reader(server.url("/file"), 500).await.unwrap();
    assert_eq!(offset.offset, 0);
    let _ = collect_reader(reader).await.unwrap();

    // Step 3: retry from 0 with different ETag — should NOT panic
    let (reader, offset) = source.get_reader(server.url("/file"), 0).await.unwrap();
    assert_eq!(offset.offset, 0);
    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);
}

// Regression test: On 206 Partial Content, `response.content_length()`
// returns the partial body size, not the total resource size.
// `total_size` should come from the `Content-Range` header instead.
#[tokio::test]
async fn test_206_reports_total_size_from_content_range() {
    let content_size = 10_000;
    let server = TestServer::start(ServerConfig::new(content_size, vec![]).with_fallback(
        RequestRule::Serve {
            support_ranges: true,
        },
    ))
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // First request to cache server metadata
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume from 5000 — total_size should be 10000 (from Content-Range), not 5000
    let (_reader, offset) = source.get_reader(server.url("/file"), 5000).await.unwrap();
    assert_eq!(offset.offset, 5000);
    assert_eq!(
        offset.total_size,
        Some(content_size as u64),
        "total_size should reflect the full resource size from Content-Range, not the partial Content-Length"
    );
}

// Regression test: if a previously-present ETag disappears from a 206 response,
// `update_cached_state` doesn't detect this as a conflict. The cached ETag
// is silently dropped, potentially allowing corrupt resumed data.
//
// The protocol should detect the disappearing header and restart.
#[tokio::test]
async fn test_disappearing_etag_detected_on_resume() {
    let content_size = 1000;
    let server = TestServer::start(
        ServerConfig::new(
            content_size,
            vec![
                // Step 1: initial request with ETag
                RequestRule::WithHeaders {
                    etag: Some("\"etag-v1\"".to_string()),
                    last_modified: None,
                    then: Box::new(RequestRule::Serve {
                        support_ranges: true,
                    }),
                },
                // Step 2: resume — 206 WITHOUT ETag (header disappeared)
                RequestRule::WithHeaders {
                    etag: None,
                    last_modified: None,
                    then: Box::new(RequestRule::Serve {
                        support_ranges: true,
                    }),
                },
                // Step 3: restart after conflict detection
                RequestRule::WithHeaders {
                    etag: Some("\"etag-v2\"".to_string()),
                    last_modified: None,
                    then: Box::new(RequestRule::Serve {
                        support_ranges: true,
                    }),
                },
            ],
        )
        .with_etag(None)
        .with_last_modified(None),
    )
    .await;

    let mut source = HttpSourceProtocol::new().unwrap();

    // Step 1: initial download, caches ETag
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Step 2: resume from 500. Server sends 206 but without ETag.
    // After the fix: should detect conflict and restart from 0.
    let (_reader, offset) = source.get_reader(server.url("/file"), 500).await.unwrap();

    assert_eq!(
        offset.offset, 0,
        "should restart from 0 after detecting disappearing ETag"
    );
    assert_eq!(
        server.request_count("/file"),
        3,
        "expected: initial + failed resume + restart"
    );
}
