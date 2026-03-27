# ripcurl

File transfers are hard.
Ripcurl's goal is to make them easy,
without needing to remember an alphabet soup of command line flags,
or resort to writing contorted loops in a shell script.
With ripcurl, we want you to fall into the [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/).

## Goals

* Zero (or minimal) config required for most users. It should just work out of the box.
* Sane defaults. No need to specify that you want to follow redirects
  or other things that you _probably_ want, but usually forget to ask.
* Resilient. Ripcurl will automatically retry failed transfers when possible.

# Non-goals

This is a focused, opinionated project.
The following are non-goals:

* Support for every single protocol. This project does not have the same scope as `curl`.
  (That said, open an issue if you'd like support for a new protocol so we can discuss.)
* Extreme configurability. This is supposed to be a simple tool, and will sometimes have opinions.
  We'll try to make things configurable within reason, but this is not a pure neutral client like `curl` out of the box.
* Ripcurl isn't a swiss army knife; it's a focused tool for file transfers.
  If you want a CLI tool for interacting with everything from a REST API to streams,
  then other tools are a better fit.

# What's with the name?

It's an homage to curl in the manner of ripgrep.
Don't read too much into it; it's not serious,
and I mostly thought it sounded funny and "fast."
It's also the name of an Australian surfwear company, apparently.
