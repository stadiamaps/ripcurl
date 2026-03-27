# ripcurl

File transfers are hard.
Ripcurl's goal is to make them easy,
without needing to remember an alphabet soup of command line flags,
or resort to writing contorted loops in a shell script.

## Quickstart

```shell
ripcurl https://example.com/really-big-file.tar.gz /path/to/really-big-file.tar.gz
```

That's it!
Without needing to ask, ripcurl will:

- Log important events in the transfer lifecycle
- Automatically retry on transient errors (network failures, some server error codes, stalled connections)
- Show status updates, or not, depending on your environment (periodic logs in CI; animated progress in a TTY)
- Cowardly refuse to overwrite an existing file unless you signal that's what you really wanted with `--overwrite`

## Why would you want this?

Even today, file transfers over the internet can be flaky.
You probably don't notice this often for smaller transfers (up to a few gigabytes),
but if you're regularly slinging 100GB+ files around, you've probably experienced a transfer failure.

Some domain-specific tools (like the `aws s3` CLI) are more reliable than others.
But if you regularly deal with HTTP, you need to remember quite a few command line switches,
and you typically need to orchestrate retries on your own.

At Stadia Maps, we deal with a _lot_ of file transfers in our data pipelines,
so we built this tool out of our own pain.
It's infuriating and wastes everyone's time when a plain old `curl` or `wget`
fails mid-transfer in a CI pipeline.

We like tools that "just work",
so with ripcurl our aim is to create a [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/)
rather than a labryinth of switches and manual orchestration.

## Goals

* **Sane defaults for transfers**. No need to specify that you want to follow redirects
  or other things that you _probably_ want, but usually forget to ask.
  Ripcurl should essentially be zero-config for most users.
* **Resilient.** Ripcurl will automatically retry failed transfers when possible.
* **Well-tested.** Ripcurl has an extensive test suite covering dozens of cases we've seen in the wild.

## Non-goals

This is a focused, opinionated project.
The following are non-goals:

* Support for dozens of protocols. This project does not have the same scope as, say, `curl`.
  (That said, open an issue if you'd like support for a new protocol so we can discuss.)
* Granular configurability. We're trying to make the most straightforward tool for the vast majority of file transfer needs.
* Non-file-like use cases. Ripcurl isn't a swiss army knife; it's a focused tool for file transfers.
  If you want a CLI tool for interacting with everything from a REST API to streams,
  then other tools are a better fit.

## FAQs

### What's with the name?

It's an homage to curl in the style of `ripgrep` or `ripunzip`.
Don't read too much into it; it's not serious,
and I mostly thought it sounded funny and "fast."
It's also the name of an Australian surfwear company, apparently.

### Why not `<some other tool>`

- Most other tools don't automatically follows redirects.
- Some other tools don't understand which headers are safe to forward cross-origin.
  This can be a problem if you do things like authenticate to an API which redirects you to a pre-signed S3 URL for the actual download.
  It's also a security problem.
- Most other tools won't persistently retry to finish the transfer.

### Why doesn't ripcurl do X?

See the non-goals section, probably.
Other tools are better at these things.
