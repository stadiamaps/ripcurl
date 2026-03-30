---
status: "accepted"
date: 2026-03-30
---

# Streaming API requires source with random access

## Context

ripcurl's `execute_transfer` writes bytes directly from source to destination,
handling transient errors by retrying with resume or restarting from scratch.
When the source doesn't support random access (e.g. byte-range requests),
the transfer orchestrator can truncate the destination and re-download from the beginning.

A new streaming API (`stream_source`) returns a `Stream<Item = Result<Bytes, TransferError>>`
for library consumers who want to process bytes directly
(write to stdout, pipe to another destination, etc.).
Unlike file transfers, a stream consumer cannot "un-consume" bytes that have already been yielded.

## Decision

The streaming API requires the source to support random access
(e.g. `Accept-Ranges: bytes` for HTTP).
If the source does not advertise random access support,
`stream_source` returns a permanent error upfront,
before yielding any bytes.

Random access capability is exposed as a `supports_random_access` field on `ReadOffset`,
discovered from the server's response headers.
This keeps the information tied to the response that discovered it (typestate-correct)
rather than as mutable state on the protocol object.

If a transient error occurs mid-stream and the server returns data
from a different offset than requested on retry (offset mismatch),
the stream terminates with a permanent error.

## Consequences

- Sources without random access (e.g. servers that don't send `Accept-Ranges`)
  cannot be used with the streaming API, even if the transfer would succeed without interruption.
  This is a conservative tradeoff designed to prevent surprises when such a transfer inevitably
  gets cut off.
- The existing `execute_transfer` / `run_transfer` path is unaffected
  and continues to handle sources without random access by restarting.
- Library consumers get a clean `Stream` interface
  with retry/resume handled transparently.