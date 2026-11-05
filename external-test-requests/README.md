# External test requests

This folder contains specific tests that a maintainer can run on an independent
machine when this development VM cannot provide the required environment or
reliable measurements. A request identifies the exact revision, environment,
commands, evidence to return, and limitations. A request is not completed until
its results have been returned and reviewed.

| Request | Status | Purpose |
| --- | --- | --- |
| [001 — compound document range comparison](001-compound-range-benchmark.md) | Requested; results pending | Repeat a checked comparison with SQLite, PostgreSQL, and MongoDB on quieter local storage. [Mac setup](001-mac-mini.md). |

Development continues while a request is running. The pinned revision in each
request stays fixed even if `main` changes. Put results for the Mac mini run of
request 001 under [results/001-mac-mini](results/001-mac-mini/README.md), in a
new subdirectory for each run, and tell us the branch or commit containing
them. We will review the results against the measured revision.

Use a disposable test environment and synthetic data. Each request describes
the resources it creates; do not run it against a production database. Review
results and logs before sharing them because they may contain machine details,
filesystem paths, or other environment information. Never commit credentials,
tokens, private keys, or customer data.

Requests for physical power-loss validation and an independent off-host
backup/restore drill are still tracked in the
[foundation follow-up](../docs/FOUNDATION_FOLLOW_UP.md#deferred-to-later-codex-sessions).
The benchmark above does not satisfy either check.
