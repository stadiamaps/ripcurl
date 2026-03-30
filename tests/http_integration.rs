mod common;

use common::test_server::{RequestRule, ServerConfig, TestServer};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

    // First call: 429
    let err = source.get_reader(server.url("/file"), 0).await.unwrap_err();
    match err {
        TransferError::Transient {
            minimum_retry_delay,
            ..
        } => {
            assert_eq!(minimum_retry_delay, Duration::from_secs(1));
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

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

#[tokio::test]
async fn test_custom_headers_sent() {
    let content_size = 500;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: true,
        }],
    ))
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-test"),
        HeaderValue::from_static("hello"),
    );

    let mut source = HttpSourceProtocol::new(headers).unwrap();
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    let req_headers = server.last_request_headers("/file").unwrap();
    assert_eq!(
        req_headers.get("x-test").unwrap().to_str().unwrap(),
        "hello"
    );
}

#[tokio::test]
async fn test_custom_headers_sent_on_resume() {
    let content_size = 1000;
    let server = TestServer::start(ServerConfig::new(content_size, vec![]).with_fallback(
        RequestRule::Serve {
            support_ranges: true,
        },
    ))
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-custom"),
        HeaderValue::from_static("persist"),
    );

    let mut source = HttpSourceProtocol::new(headers).unwrap();

    // Initial request
    let (reader, _) = source.get_reader(server.url("/file"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume request
    let (reader, offset) = source.get_reader(server.url("/file"), 500).await.unwrap();
    assert_eq!(offset.offset, 500);
    let _ = collect_reader(reader).await.unwrap();

    // Custom header should be present on the resume request
    let req_headers = server.last_request_headers("/file").unwrap();
    assert_eq!(
        req_headers.get("x-custom").unwrap().to_str().unwrap(),
        "persist"
    );
}

#[tokio::test]
async fn test_auth_header_stripped_on_cross_host_redirect() {
    let content_size = 500;

    // server2 serves the actual content
    let server2 = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: true,
        }],
    ))
    .await;

    // server1 redirects to server2 (different port = different origin)
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_path_rules(
        "/download",
        vec![RequestRule::Redirect {
            status: 302,
            location: server2.url("/file").to_string(),
        }],
    ))
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret-token"),
    );

    let mut source = HttpSourceProtocol::new(headers).unwrap();
    let (reader, _) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();
    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    // server1 should have received the Authorization header
    let s1_headers = server1.last_request_headers("/download").unwrap();
    assert!(
        s1_headers.get("authorization").is_some(),
        "server1 should receive the Authorization header"
    );

    // server2 should NOT have received the Authorization header (stripped on cross-origin redirect)
    let s2_headers = server2.last_request_headers("/file").unwrap();
    assert!(
        s2_headers.get("authorization").is_none(),
        "server2 should NOT receive the Authorization header after cross-origin redirect"
    );
}

#[tokio::test]
async fn test_auth_header_preserved_on_same_host_redirect() {
    let content_size = 500;
    let server = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_path_rules(
                "/start",
                vec![RequestRule::Redirect {
                    status: 302,
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

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer keep-me"),
    );

    let mut source = HttpSourceProtocol::new(headers).unwrap();
    let (reader, _) = source.get_reader(server.url("/start"), 0).await.unwrap();
    let bytes = collect_reader(reader).await.unwrap();
    assert_eq!(bytes.len(), content_size);

    // Same-host redirect should preserve the Authorization header
    let final_headers = server.last_request_headers("/final").unwrap();
    assert_eq!(
        final_headers
            .get("authorization")
            .expect("Authorization header should be preserved on same-host redirect")
            .to_str()
            .unwrap(),
        "Bearer keep-me"
    );
}

// --- Redirect + ETag/Resume integration tests ---
//
// These tests verify that ETag/Last-Modified-based resume works correctly
// when requests go through redirect chains (the S3/R2 presigned URL pattern).
// The key invariant: retries always go to the *original* URL, not the redirect
// target, because presigned URLs expire and auth headers must reach the origin.

#[tokio::test]
async fn test_resume_headers_survive_cross_server_redirect() {
    let content_size = 1000;

    // server2 (CDN) serves actual content with ETag and Last-Modified
    let server2 = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_fallback(RequestRule::Serve {
                support_ranges: true,
            })
            .with_etag(Some("\"cdn-etag\"".to_string()))
            .with_last_modified(Some("Sun, 01 Jan 2025 00:00:00 GMT".to_string())),
    )
    .await;

    // server1 (origin) always redirects to server2
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_fallback(
        RequestRule::Redirect {
            status: 302,
            location: server2.url("/file").to_string(),
        },
    ))
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret-token"),
    );

    let mut source = HttpSourceProtocol::new(headers).unwrap();

    // First request: follows redirect, caches ETag/Last-Modified from CDN
    let (reader, offset) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();
    assert_eq!(offset.offset, 0);
    let _ = collect_reader(reader).await.unwrap();

    // Resume: sends Range + If-Match + If-Unmodified-Since through the redirect chain
    let (reader, offset) = source
        .get_reader(server1.url("/download"), 500)
        .await
        .unwrap();
    assert_eq!(offset.offset, 500);
    let remaining = collect_reader(reader).await.unwrap();
    assert_eq!(remaining.len(), 500);

    // Verify content
    let expected = common::test_server::generate_content(content_size);
    assert_eq!(remaining, &expected[500..]);

    // Verify CDN received conditional + range headers but NOT authorization
    let s2_headers = server2.last_request_headers("/file").unwrap();
    assert!(
        s2_headers.get("range").is_some(),
        "Range header should survive cross-origin redirect"
    );
    assert_eq!(
        s2_headers.get("if-match").unwrap().to_str().unwrap(),
        "\"cdn-etag\"",
        "If-Match should survive cross-origin redirect"
    );
    assert_eq!(
        s2_headers
            .get("if-unmodified-since")
            .unwrap()
            .to_str()
            .unwrap(),
        "Sun, 01 Jan 2025 00:00:00 GMT",
        "If-Unmodified-Since should survive cross-origin redirect"
    );
    assert!(
        s2_headers.get("authorization").is_none(),
        "Authorization must NOT leak to CDN after cross-origin redirect"
    );

    // Verify origin received authorization
    let s1_headers = server1.last_request_headers("/download").unwrap();
    assert!(
        s1_headers.get("authorization").is_some(),
        "Origin server should receive Authorization header"
    );
}

#[tokio::test]
async fn test_etag_continuity_through_redirect_happy_path() {
    let content_size = 1000;
    let server = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_path_rules(
                "/download",
                vec![
                    RequestRule::Redirect {
                        status: 302,
                        location: "/file".to_string(),
                    },
                    RequestRule::Redirect {
                        status: 302,
                        location: "/file".to_string(),
                    },
                ],
            )
            .with_path_rules(
                "/file",
                vec![
                    RequestRule::WithHeaders {
                        etag: Some("\"v1\"".to_string()),
                        last_modified: None,
                        then: Box::new(RequestRule::Serve {
                            support_ranges: true,
                        }),
                    },
                    RequestRule::WithHeaders {
                        etag: Some("\"v1\"".to_string()),
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

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

    // First request: redirect → /file, caches ETag "v1"
    let (reader, _) = source.get_reader(server.url("/download"), 0).await.unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume: redirect → /file, ETag "v1" matches → continuity check passes
    let (reader, offset) = source
        .get_reader(server.url("/download"), 500)
        .await
        .unwrap();
    assert_eq!(
        offset.offset, 500,
        "resume should succeed when ETag is consistent through redirects"
    );

    let remaining = collect_reader(reader).await.unwrap();
    assert_eq!(remaining.len(), 500);

    let expected = common::test_server::generate_content(content_size);
    assert_eq!(remaining, &expected[500..]);

    assert_eq!(server.request_count("/download"), 2);
    assert_eq!(server.request_count("/file"), 2);
}

#[tokio::test]
async fn test_cross_server_redirect_etag_mismatch_triggers_restart() {
    let content_size = 1000;

    // CDN serves content with per-request ETag overrides
    let server2 = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_path_rules(
                "/file",
                vec![
                    // Request 1: initial download
                    RequestRule::WithHeaders {
                        etag: Some("\"v1\"".to_string()),
                        last_modified: None,
                        then: Box::new(RequestRule::Serve {
                            support_ranges: true,
                        }),
                    },
                    // Request 2: resume — ETag changed (resource updated on CDN)
                    RequestRule::WithHeaders {
                        etag: Some("\"v2\"".to_string()),
                        last_modified: None,
                        then: Box::new(RequestRule::Serve {
                            support_ranges: true,
                        }),
                    },
                    // Request 3: restart after conflict detection
                    RequestRule::WithHeaders {
                        etag: Some("\"v2\"".to_string()),
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

    // Origin always redirects to CDN
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_fallback(
        RequestRule::Redirect {
            status: 302,
            location: server2.url("/file").to_string(),
        },
    ))
    .await;

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

    // First request: caches ETag "v1" from CDN
    let (reader, _) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume: CDN returns ETag "v2" → mismatch detected → internal restart from byte 0
    let (_reader, offset) = source
        .get_reader(server1.url("/download"), 500)
        .await
        .unwrap();
    assert_eq!(
        offset.offset, 0,
        "should restart from 0 when ETag changes through redirect"
    );

    assert_eq!(
        server2.request_count("/file"),
        3,
        "expected: initial + mismatched resume + restart"
    );
    assert_eq!(
        server1.request_count("/download"),
        3,
        "restart should go through the original URL (redirect chain)"
    );
}

#[tokio::test]
async fn test_412_through_redirect_triggers_restart() {
    let content_size = 1000;

    // CDN: first serves normally, then returns 412, then serves again
    let server2 = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_path_rules(
                "/file",
                vec![
                    // Request 1: initial download
                    RequestRule::WithHeaders {
                        etag: Some("\"v1\"".to_string()),
                        last_modified: None,
                        then: Box::new(RequestRule::Serve {
                            support_ranges: true,
                        }),
                    },
                    // Request 2: 412 Precondition Failed (resource changed since If-Match)
                    RequestRule::Error {
                        status: 412,
                        retry_after: None,
                    },
                    // Request 3: restart after 412
                    RequestRule::WithHeaders {
                        etag: Some("\"v2\"".to_string()),
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

    // Origin always redirects
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_fallback(
        RequestRule::Redirect {
            status: 302,
            location: server2.url("/file").to_string(),
        },
    ))
    .await;

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

    // Initial download
    let (reader, _) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume: CDN returns 412 → protocol restarts from byte 0
    let (_reader, offset) = source
        .get_reader(server1.url("/download"), 500)
        .await
        .unwrap();
    assert_eq!(
        offset.offset, 0,
        "should restart from 0 after 412 through redirect"
    );

    assert_eq!(
        server2.request_count("/file"),
        3,
        "expected: initial + 412 resume + restart"
    );
    assert_eq!(
        server1.request_count("/download"),
        3,
        "restart should traverse the redirect chain again"
    );
}

#[tokio::test]
async fn test_redirect_target_changes_between_requests() {
    let content_size = 1000;

    // CDN node A
    let server2a = TestServer::start(
        ServerConfig::new(
            content_size,
            vec![RequestRule::Serve {
                support_ranges: true,
            }],
        )
        .with_etag(Some("\"node-a-etag\"".to_string())),
    )
    .await;

    // CDN node B
    let server2b = TestServer::start(
        ServerConfig::new(content_size, vec![])
            .with_fallback(RequestRule::Serve {
                support_ranges: true,
            })
            .with_etag(Some("\"node-b-etag\"".to_string())),
    )
    .await;

    // Origin: first request → node A, subsequent requests → node B
    let server1 = TestServer::start(ServerConfig::new(0, vec![]).with_path_rules(
        "/download",
        vec![
            RequestRule::Redirect {
                status: 302,
                location: server2a.url("/file").to_string(),
            },
            RequestRule::Redirect {
                status: 302,
                location: server2b.url("/file").to_string(),
            },
            RequestRule::Redirect {
                status: 302,
                location: server2b.url("/file").to_string(),
            },
        ],
    ))
    .await;

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();

    // First request → node A, caches ETag "node-a-etag"
    let (reader, _) = source
        .get_reader(server1.url("/download"), 0)
        .await
        .unwrap();
    let _ = collect_reader(reader).await.unwrap();

    // Resume → node B, ETag "node-b-etag" ≠ "node-a-etag" → mismatch → restart via node B
    let (_reader, offset) = source
        .get_reader(server1.url("/download"), 500)
        .await
        .unwrap();
    assert_eq!(
        offset.offset, 0,
        "should restart when redirect target changes and ETags differ"
    );

    assert_eq!(
        server2a.request_count("/file"),
        1,
        "node A: initial request only"
    );
    assert_eq!(
        server2b.request_count("/file"),
        2,
        "node B: mismatched resume + restart"
    );
}

// Streaming API integration tests

#[tokio::test]
async fn test_stream_source_with_accept_ranges() {
    let content_size = 5_000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: true,
        }],
    ))
    .await;

    let config = ripcurl::transfer::TransferConfig {
        max_retries: 3,
        overwrite: false,
        custom_http_headers: vec![],
    };

    let (stream, info) = ripcurl::stream::stream_from_url(server.url("/file"), &config)
        .await
        .unwrap();

    assert_eq!(info.total_size, Some(content_size as u64));

    let mut stream = pin!(stream);
    let mut bytes = Vec::new();
    while let Some(result) = stream.next().await {
        bytes.extend_from_slice(&result.unwrap());
    }

    let expected = common::test_server::generate_content(content_size);
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn test_stream_source_rejects_no_accept_ranges() {
    let content_size = 5_000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::Serve {
            support_ranges: false,
        }],
    ))
    .await;

    let config = ripcurl::transfer::TransferConfig {
        max_retries: 3,
        overwrite: false,
        custom_http_headers: vec![],
    };

    // NB: the test server does not send `Accept-Ranges: bytes`.
    match ripcurl::stream::stream_from_url(server.url("/file"), &config).await {
        Err(TransferError::Permanent { reason }) => {
            assert!(
                reason.contains("random access"),
                "expected 'random access' in error, got: {reason}"
            );
        }
        Err(other) => panic!("expected Permanent error about random access, got: {other:?}"),
        Ok(_) => panic!("expected error for source without random access, got Ok"),
    }
}

/// Regression test: errors during body streaming via `bytes_stream()` are
/// tagged as `is_decode()` by reqwest. These must be classified as transient
/// so the transfer orchestrator can retry from the last good offset.
#[tokio::test]
async fn test_decode_error_mid_stream_is_transient() {
    let content_size = 10_000;
    let bytes_before_error = 5_000;
    let server = TestServer::start(ServerConfig::new(
        content_size,
        vec![RequestRule::PartialThenError { bytes_before_error }],
    ))
    .await;

    let mut source = HttpSourceProtocol::new(Default::default()).unwrap();
    let (reader, _offset) = source.get_reader(server.url("/file"), 0).await.unwrap();

    // Stream bytes until we hit the error
    let mut total = 0u64;
    let mut stream = pin!(reader.stream_bytes());
    let mut error = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => total += bytes.len() as u64,
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }

    let err = error.expect("should have received a stream error");

    match err {
        TransferError::Transient {
            consumed_byte_count,
            minimum_retry_delay,
            reason,
        } => {
            assert_eq!(
                consumed_byte_count, total,
                "consumed_byte_count should match bytes received before error"
            );
            assert_eq!(minimum_retry_delay, Duration::from_secs(1));
            assert!(
                reason.contains("decode"),
                "error message should mention decode: {reason}"
            );
        }
        TransferError::Permanent { reason } => {
            panic!("BUG: decode error was classified as Permanent (should be Transient): {reason}");
        }
    }
}
