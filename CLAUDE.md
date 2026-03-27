# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

- **Build:** `cargo build`
- **Run all tests:** `cargo nextest run --no-fail-fast` (or `just test`)
- **Run a single test:** `cargo nextest run test_name`
- **Run unit tests only:** `cargo nextest run --lib`
- **Smoke tests (requires nushell + httpbin):** `just smoke-test`

Uses Rust edition 2024 and requires cargo-nextest for the test runner.

## Architecture

ripcurl is a resilient file transfer CLI. It transfers from a **source** (e.g. HTTP) to a **destination** (e.g. local file), with automatic retry and resume on transient failures.

### Core traits (`src/protocol/mod.rs`)

The protocol layer defines four traits that all protocol implementations conform to:

- **`SourceProtocol`** — opens a reader from a URL at a given byte offset (for resume support)
- **`SourceReader`** — streams bytes as an async `Stream<Item = Result<Bytes, TransferError>>`
- **`DestinationProtocol`** — opens a writer to a URL
- **`DestinationWriter`** — writes bytes, supports `truncate_and_reset` (for restart after failed resume), and `finalize` (flush + commit)

### Error classification (`TransferError`)

All errors are classified as either `Transient` (retryable, carries `retry_delay` and `consumed_byte_count`) or `Permanent` (fatal). This distinction drives the retry logic in the transfer orchestrator.

### Transfer orchestration (`src/transfer.rs`)

`run_transfer` is the protocol-agnostic transfer loop. It:
1. Requests a reader at the current byte offset
2. Streams bytes from reader to writer
3. On transient errors, retries with the corrected offset
4. Handles offset mismatches (server can't resume) by truncating the destination and restarting

The `retry_transient!` macro retries any async operation on transient errors up to `max_retries`. It's a macro because the retried expression often borrows `&mut self`.

### Protocol resolution (`src/source.rs`, `src/destination.rs`)

`resolve_source` and `resolve_destination` map URL schemes to protocol implementations. Currently: HTTP/HTTPS sources, file:// destinations. New protocols are added by implementing the traits and adding a match arm.

### CLI (`src/main.rs`)

Schema-less URLs default to `file://`. Progress bar via tracing-indicatif (disabled in CI via `$CI` env var).

## Test structure

- **`tests/transfer_orchestration.rs`** — uses a custom `transfer_test!` DSL macro to test the orchestration layer with mock protocols. Steps describe what happens (bytes read, errors injected) and the framework wires up mocks and validates consistency.
- **`tests/http_integration.rs`** — tests `HttpSourceProtocol` against a real axum `TestServer` (in `tests/common/test_server.rs`) with configurable `RequestRule` queues for fault injection.
- **`tests/file_integration.rs`** — tests `FileProtocol`/`FileWriter` directly against the filesystem using tempfiles.
- **`tests/common/mock_protocols.rs`** — `MockSource` and `MockWriter` with scripted responses and error injection at byte thresholds.
- **`tests_integration/smoke.nu`** — nushell script that runs the binary against httpbin.org.

Do NOT use `tokio::time::pause()` in tests that do real HTTP I/O (integration tests with TestServer). It breaks reqwest's internal timeouts. Only use it in fully-mocked orchestration tests.

### ADRs

A log of significant architectural decisions is kept under `docs/decisions`
using the lightweight Markdown ADR format.
Keep ADRs short and to the point.
