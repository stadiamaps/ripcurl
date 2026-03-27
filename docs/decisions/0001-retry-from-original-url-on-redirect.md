---
status: "accepted""
date: 2026-03-27
---

# Retry from original URL after redirect on transient failure

## Context

ripcurl supports HTTP sources that redirect to a different URL before serving content.
A common pattern is an authenticated API endpoint that redirects to a signed CDN URL (e.g., CloudFlare R2, Amazon S3 presigned URLs):

1. Client sends `GET https://api.example.com/file` with `Authorization: Bearer <token>`
2. Server responds with `302` to `https://cdn.example.com/file?sig=abc&expires=1711500000`
3. reqwest follows the redirect, stripping `Authorization` (cross-origin), and streams content from the CDN

When a transient error interrupts the transfer, the retry loop must decide which URL to use for the resume attempt:
the **original URL** or the **final (redirected) URL**.

## Decision

Always retry from the **original URL**.
The transfer orchestrator passes `source_url` (the user-provided URL) on every call to `get_reader`, and reqwest re-follows the redirect chain on each attempt.

Any custom headers in the original transfer request are also sent when attempting to resume (enabling regeneration of a fresh signed URL if needed, for example).

## Consequences

- Good - Signed URLs expire. S3/R2 presigned URLs typically have a TTL of minutes. Caching and reusing the redirect target would fail with 403 after expiry. Retrying from the original URL obtains a fresh signed URL on each attempt.

- Good - Conditional headers survive redirects. `Range`, `If-Match`, and `If-Unmodified-Since` are not classified as sensitive by reqwest and are preserved through cross-origin redirects. This means the CDN correctly evaluates resume conditions (byte offset, ETag matching) even though the request originates from the initial URL.

- Good - ETag continuity works naturally. ETags cached from the first response (served by the CDN after redirect) are properties of the stored object, not the URL. On resume, `If-Match` reaches the CDN via the redirect chain, and the CDN validates it against the same object. If the object changed, the CDN returns 412 or a different ETag, and the protocol restarts the transfer from scratch.

- Neutral - Each retry incurs an extra round-trip through the redirect chain. For the use cases ripcurl targets (large file transfers with infrequent retries), this is negligible.

- Bad - If the original server is temporarily unavailable but the CDN is still serving, retries will fail at the redirect step rather than succeeding directly against the CDN.
