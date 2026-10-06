# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Use GitHub's private vulnerability reporting instead:
**Security → Report a vulnerability** on the repository. Include what you found, how to reproduce it and what it lets an
attacker do.

You can expect an acknowledgement within a few days and a fix or a mitigation plan for confirmed problems before the details
are made public.

## Supported versions

Only the latest release and the default branch are supported.

## What is in scope

The library's source and its pipelines. A report about a dependency belongs to that dependency, unless this library uses it
in a way that makes it exploitable.

## How the repository defends itself

Every change passes a pipeline that scans the resolved dependencies for known vulnerabilities, scans the whole history for
committed secrets, and builds the library from a pinned toolchain. Actions are pinned by commit, and releases carry a
bill of materials and an attestation of where they were built. The same checks run weekly, so a vulnerability published
tomorrow is found without a commit.
