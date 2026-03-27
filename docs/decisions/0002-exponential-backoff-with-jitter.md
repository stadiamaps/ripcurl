---
status: "accepted"
date: 2026-03-27
---

# Exponential backoff with jitter for retry delays

## Context

When a connection is cut, a download stalls, or some similar event occurs,
we can retry the request after a delay.
Some servers indicate how long you should wait before retrying,
but this is far from universal.

## Decision

Use [exponential backoff with jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
across all retry sites.
Server `Retry-After` is a hard floor (per RFC 9110).

## Consequences

- Retries back off progressively instead of hammering at a fixed interval.
- `Retry-After` is always respected as a minimum in our backoff calculations.

