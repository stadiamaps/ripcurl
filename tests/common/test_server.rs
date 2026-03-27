//! Configurable axum-based HTTP test server for integration tests.
//!
//! Each test creates its own `TestServer` on a random port.
//! Server behavior is controlled by a queue of [`RequestRule`]s per URL path.

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Deterministic content of `size` bytes: `[0, 1, 2, ..., 255, 0, 1, ...]`.
pub fn generate_content(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// What the server should do when it receives a request.
#[derive(Debug, Clone)]
pub enum RequestRule {
    /// Serve the configured content normally.
    /// If `support_ranges` is true and a `Range` header is present, respond with 206.
    Serve { support_ranges: bool },
    /// Return an HTTP error status code.
    Error {
        status: u16,
        retry_after: Option<String>,
    },
    /// Stream `bytes_before_drop` bytes of content, then abort the stream.
    PartialThenDrop { bytes_before_drop: usize },
    /// Stream `bytes_before_error` bytes of content, then yield an error in the stream.
    PartialThenError { bytes_before_error: usize },
    /// Serve full content with 200, ignoring any `Range` header.
    ServeIgnoringRange,
    /// Send a redirect response.
    Redirect { status: u16, location: String },
    /// Wait `duration` then apply the inner rule.
    Delay {
        duration: Duration,
        then: Box<RequestRule>,
    },
    /// Override identity headers for this request, then apply the inner rule.
    /// Replaces the server-level etag/last_modified entirely.
    WithHeaders {
        etag: Option<String>,
        last_modified: Option<String>,
        then: Box<RequestRule>,
    },
}

/// Configuration for a [`TestServer`].
pub struct ServerConfig {
    /// Content the server will serve.
    pub content: Vec<u8>,
    /// Per-path rule queues. When a request arrives at a path, its rule is
    /// popped from the front of that path's queue.
    pub path_rules: HashMap<String, VecDeque<RequestRule>>,
    /// Default rule queue for paths that don't have a specific entry.
    pub default_rules: VecDeque<RequestRule>,
    /// Fallback rule used when a path's queue (or default queue) is exhausted.
    pub fallback_rule: RequestRule,
    /// ETag value to include in responses (if set).
    pub etag: Option<String>,
    /// Last-Modified value to include in responses (if set).
    pub last_modified: Option<String>,
}

impl ServerConfig {
    /// Quick config: serve `content_size` bytes with the given default rules.
    pub fn new(content_size: usize, default_rules: Vec<RequestRule>) -> Self {
        Self {
            content: generate_content(content_size),
            path_rules: HashMap::new(),
            default_rules: VecDeque::from(default_rules),
            fallback_rule: RequestRule::Serve {
                support_ranges: true,
            },
            etag: Some("\"test-etag\"".to_string()),
            last_modified: Some("Sun, 01 Jan 2025 00:00:00 GMT".to_string()),
        }
    }

    /// Add a per-path rule queue.
    pub fn with_path_rules(mut self, path: &str, rules: Vec<RequestRule>) -> Self {
        self.path_rules
            .insert(path.to_string(), VecDeque::from(rules));
        self
    }

    /// Set the fallback rule (used when queues are exhausted).
    pub fn with_fallback(mut self, rule: RequestRule) -> Self {
        self.fallback_rule = rule;
        self
    }

    /// Set the ETag header value.
    pub fn with_etag(mut self, etag: Option<String>) -> Self {
        self.etag = etag;
        self
    }

    /// Set the Last-Modified header value.
    pub fn with_last_modified(mut self, last_modified: Option<String>) -> Self {
        self.last_modified = last_modified;
        self
    }
}

/// Shared mutable state for the server.
pub struct ServerState {
    config: ServerConfig,
    /// Number of requests received per path.
    pub request_counts: HashMap<String, u32>,
    /// Headers from the most recent request per path.
    pub last_request_headers: HashMap<String, HeaderMap>,
}

impl ServerState {
    fn pop_rule(&mut self, path: &str) -> RequestRule {
        // Try path-specific queue first
        if let Some(queue) = self.config.path_rules.get_mut(path) {
            if let Some(rule) = queue.pop_front() {
                return rule;
            }
        }
        // Then default queue
        if let Some(rule) = self.config.default_rules.pop_front() {
            return rule;
        }
        // Fallback
        self.config.fallback_rule.clone()
    }
}

pub type SharedState = Arc<Mutex<ServerState>>;

/// A running test HTTP server.
pub struct TestServer {
    pub base_url: url::Url,
    pub state: SharedState,
    _task: JoinHandle<()>,
}

impl TestServer {
    /// Start a new test server with the given configuration.
    pub async fn start(config: ServerConfig) -> Self {
        let state = Arc::new(Mutex::new(ServerState {
            config,
            request_counts: HashMap::new(),
            last_request_headers: HashMap::new(),
        }));

        let app = Router::new()
            .route("/{*path}", get(handle_request))
            .route("/", get(handle_request))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test server");
        let port = listener.local_addr().unwrap().port();
        let base_url = url::Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();

        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        Self {
            base_url,
            state,
            _task: task,
        }
    }

    /// Get the full URL for a given path on this server.
    pub fn url(&self, path: &str) -> url::Url {
        self.base_url.join(path).unwrap()
    }

    /// Get the number of requests received for a given path.
    pub fn request_count(&self, path: &str) -> u32 {
        let state = self.state.lock().unwrap();
        state.request_counts.get(path).copied().unwrap_or(0)
    }

    /// Get the headers from the most recent request to a given path.
    pub fn last_request_headers(&self, path: &str) -> Option<HeaderMap> {
        let state = self.state.lock().unwrap();
        state.last_request_headers.get(path).cloned()
    }
}

/// Parse a `Range: bytes=N-` header, returning the start offset.
fn parse_range_header(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get("range")?.to_str().ok()?;
    let rest = value.strip_prefix("bytes=")?;
    let start_str = rest.strip_suffix('-')?;
    start_str.parse().ok()
}

async fn handle_request(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
) -> Response {
    let path = uri.path().to_string();

    let (rule, content, etag, last_modified) = {
        let mut st = state.lock().unwrap();
        // Track request
        *st.request_counts.entry(path.clone()).or_insert(0) += 1;
        st.last_request_headers
            .insert(path.clone(), headers.clone());
        let rule = st.pop_rule(&path);
        let content = st.config.content.clone();
        let etag = st.config.etag.clone();
        let last_modified = st.config.last_modified.clone();
        (rule, content, etag, last_modified)
    };

    apply_rule(
        rule,
        &headers,
        &content,
        etag.as_deref(),
        last_modified.as_deref(),
    )
    .await
}

async fn apply_rule(
    rule: RequestRule,
    headers: &HeaderMap,
    content: &[u8],
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Response {
    match rule {
        RequestRule::Serve { support_ranges } => {
            let range_start = if support_ranges {
                parse_range_header(headers)
            } else {
                None
            };

            if let Some(start) = range_start {
                let start = start as usize;
                if start >= content.len() {
                    return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                }

                let slice = &content[start..];
                let mut builder = Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("content-length", slice.len().to_string())
                    .header(
                        "content-range",
                        format!("bytes {}-{}/{}", start, content.len() - 1, content.len()),
                    )
                    .header("accept-ranges", "bytes");

                if let Some(etag) = etag {
                    builder = builder.header("etag", etag);
                }
                if let Some(lm) = last_modified {
                    builder = builder.header("last-modified", lm);
                }

                builder
                    .body(Body::from(slice.to_vec()))
                    .unwrap()
                    .into_response()
            } else {
                // Full response
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-length", content.len().to_string());

                if support_ranges {
                    builder = builder.header("accept-ranges", "bytes");
                } else {
                    builder = builder.header("accept-ranges", "none");
                }

                if let Some(etag) = etag {
                    builder = builder.header("etag", etag);
                }
                if let Some(lm) = last_modified {
                    builder = builder.header("last-modified", lm);
                }

                builder
                    .body(Body::from(content.to_vec()))
                    .unwrap()
                    .into_response()
            }
        }

        RequestRule::Error {
            status,
            retry_after,
        } => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut builder = Response::builder().status(status);
            if let Some(ra) = retry_after {
                builder = builder.header("retry-after", ra);
            }
            builder
                .body(Body::from(format!("{status}")))
                .unwrap()
                .into_response()
        }

        RequestRule::PartialThenDrop { bytes_before_drop } => {
            let partial = content[..bytes_before_drop.min(content.len())].to_vec();
            // Advertise the full content-length but only send partial bytes,
            // then abruptly end the stream. This simulates a connection drop.
            let body_stream = stream::iter(vec![Ok::<_, std::io::Error>(partial)]);

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", content.len().to_string());

            if let Some(etag) = etag {
                builder = builder.header("etag", etag);
            }

            builder
                .body(Body::from_stream(body_stream))
                .unwrap()
                .into_response()
        }

        RequestRule::PartialThenError { bytes_before_error } => {
            let partial = content[..bytes_before_error.min(content.len())].to_vec();

            // Use a channel so we can control timing: send partial data first,
            // yield to let it flush to the client, then send the error.
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(1);
            tokio::spawn(async move {
                tx.send(Ok(partial)).await.unwrap();
                // Yield to ensure the partial data is flushed to the client
                // before we send the error.
                tokio::task::yield_now().await;
                tx.send(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "simulated stream error for testing",
                )))
                .await
                .unwrap();
            });
            let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", content.len().to_string());

            if let Some(etag) = etag {
                builder = builder.header("etag", etag);
            }

            builder
                .body(Body::from_stream(body_stream))
                .unwrap()
                .into_response()
        }

        RequestRule::ServeIgnoringRange => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", content.len().to_string())
                .header("accept-ranges", "none");

            if let Some(etag) = etag {
                builder = builder.header("etag", etag);
            }
            if let Some(lm) = last_modified {
                builder = builder.header("last-modified", lm);
            }

            builder
                .body(Body::from(content.to_vec()))
                .unwrap()
                .into_response()
        }

        RequestRule::Redirect { status, location } => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::FOUND);
            Response::builder()
                .status(status)
                .header("location", HeaderValue::from_str(&location).unwrap())
                .body(Body::empty())
                .unwrap()
                .into_response()
        }

        RequestRule::Delay { duration, then } => {
            tokio::time::sleep(duration).await;
            Box::pin(apply_rule(*then, headers, content, etag, last_modified)).await
        }

        RequestRule::WithHeaders {
            etag: override_etag,
            last_modified: override_lm,
            then,
        } => {
            Box::pin(apply_rule(
                *then,
                headers,
                content,
                override_etag.as_deref(),
                override_lm.as_deref(),
            ))
            .await
        }
    }
}
