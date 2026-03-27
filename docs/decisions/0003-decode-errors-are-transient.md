---
status: "accepted"
date: 2026-03-27
---

# Classify response decode errors as transient

## Context

ripcurl classifies HTTP errors as either transient (retryable) or permanent (fatal).
Testing in a production environment revealed that some mid-stream errors
are surfaced as "decoding errors" by reqwest, causing a permanent transfer failure.

reqwest's `bytes_stream()` wraps all errors from the response body stream as
"decode" errors (`is_decode() == true`). This includes connection resets,
HTTP/2 frame corruption, incomplete messages, and decompression failures —
all of which are transient by nature.
This [GitHub issue](https://github.com/seanmonstar/reqwest/issues/2839) is not the exact same one we hit,
but illustrates the general way reqwest handles things.

## Decision

Classify `reqwest::Error::is_decode()` as `TransferError::Transient`,
with a 1-second minimum retry delay (server hint floor), matching `is_body()`.
The transfer orchestrator applies exponential backoff with jitter on top of this floor (see ADR 0002),
so we won't hammer the server.

## Consequences

- Downloads survive mid-stream corruption by retrying from the last good offset.
- Genuinely permanent decode errors (e.g. server always sends corrupt data) will
  exhaust the retry budget instead of failing immediately.
