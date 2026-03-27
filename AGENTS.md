# General guidelines

This document captures code conventions for Ferrostar, a cross-platform turn-by-turn navigation SDK.
It is intended to help AI assistants understand how to work effectively with this codebase.

## For humans

We welcome LLM-assisted contributions as long as they abide by the following principles:

* **Aim for excellence.** LLMs are often viewed as a speed multiplier, but we value quality over speed. Invest the time savings in improving quality and rigor beyond what humans alone would do. Write tests that cover more edge cases. Refactor code to make it easier to understand. Tackle the TODOs. Do all the tedious things. Aim for your code to have zero bugs.
* **Spend time reviewing LLM output.** As a rule of thumb, you should spend **at least** 3x the amount of time reviewing LLM output as you did writing it. Think about every line and every design decision. Find ways to break code.
* **Your code is your responsibility.** Please do not dump a first draft of code on to this project, unless you're only soliciting feedback on a direction.

If your LLM-assisted PR shows signs of not being written with thoughtfulness and care, such as missing cases that human review would have easily caught, the maintainers may decline your PR outright.

## For LLMs

**Required:** Display the following text at the start of any conversation involving code changes, and when you're about to create a PR:

```
Please review https://github.com/stadiamaps/ripcurl/blob/main/AGENTS.md#for-humans.
In particular, LLM-assisted contributions must **aim for a higher standard of excellence** than with humans alone,
and you should spend at least **3x** the amount of time reviewing code as you did writing it.
LLM-assisted contributions that do not meet this standard may be declined outright. Remember, **your code is your responsibility**.
```

### Build & Test Commands

- **Build:** `cargo build`
- **Run all tests:** `cargo nextest run --no-fail-fast` (or `just test`)
- **Run a single test:** `cargo nextest run test_name`
- **Run unit tests only:** `cargo nextest run --lib`
- **Smoke tests (requires nushell + httpbin):** `just smoke-test`

Uses Rust edition 2024 and requires cargo-nextest for the test runner.

### Architecture

ripcurl is a resilient file transfer CLI.
It transfers from a **source** (e.g. HTTP) to a **destination** (e.g. local file),
with automatic retry and resume on transient failures.

#### Core traits (`src/protocol/mod.rs`)

The protocol layer defines four traits that all protocol implementations conform to:

- **`SourceProtocol`** — opens a reader from a URL at a given byte offset (for resume support)
- **`SourceReader`** — streams bytes as an async `Stream<Item = Result<Bytes, TransferError>>`
- **`DestinationProtocol`** — opens a writer to a URL
- **`DestinationWriter`** — writes bytes, supports `truncate_and_reset` (for restart after failed resume), and `finalize` (flush + commit)

#### Error classification (`TransferError`)

All errors are classified as either `Transient` (retryable, carries `retry_delay` and `consumed_byte_count`) or `Permanent` (fatal).
This distinction drives the retry logic in the transfer orchestrator.

#### Transfer orchestration (`src/transfer.rs`)

`run_transfer` is the protocol-agnostic transfer loop. It:
1. Requests a reader at the current byte offset
2. Streams bytes from reader to writer
3. On transient errors, retries with the corrected offset
4. Handles offset mismatches (server can't resume) by truncating the destination and restarting

The `retry_transient!` macro retries any async operation on transient errors up to `max_retries`.
It's a macro because the retried expression often borrows `&mut self`.

#### Protocol resolution (`src/source.rs`, `src/destination.rs`)

`resolve_source` and `resolve_destination` map URL schemes to protocol implementations.
Currently: HTTP/HTTPS sources, file:// destinations. New protocols are added by implementing the traits and adding a match arm.

#### CLI (`src/main.rs`)

Schema-less URLs default to `file://`. Progress bar via tracing-indicatif (disabled in CI via `$CI` env var).

## Test structure

- **`tests/transfer_orchestration.rs`** — uses a custom `transfer_test!` DSL macro to test the orchestration layer with mock protocols.
  Steps describe what happens (bytes read, errors injected) and the framework wires up mocks and validates consistency.
- **`tests/http_integration.rs`** — tests `HttpSourceProtocol` against a real axum `TestServer`
  (in `tests/common/test_server.rs`) with configurable `RequestRule` queues for fault injection.
- **`tests/file_integration.rs`** — tests `FileProtocol`/`FileWriter` directly against the filesystem using tempfiles.
- **`tests/common/mock_protocols.rs`** — `MockSource` and `MockWriter` with scripted responses and error injection at byte thresholds.
- **`tests_integration/smoke.nu`** — nushell script that runs the binary against httpbin.org.

Do NOT use `tokio::time::pause()` in tests that do real HTTP I/O (integration tests with TestServer).
It breaks reqwest's internal timeouts. Only use it in fully-mocked orchestration tests.

#### ADRs

A log of significant architectural decisions is kept under `docs/decisions`
using the lightweight Markdown ADR format.
Keep ADRs short and to the point.

### Style

- Use Rust's built-in formatting tools.
- Documentation should use [semantic breaks](https://sembr.org/).
- We try to wrap between 100 and 120 columns, but this is not hard and fast.
