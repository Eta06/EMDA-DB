# Request 001: Mac mini results

Create a new subdirectory for each run, for example `20261106T120000Z/`.
Do not replace earlier results. Each directory should contain the available
`primary.json`, `repeat.json`, `smoke.json`, `host.txt`, and `mac-host.txt`,
plus a short `notes.md` describing the VM configuration, host activity,
failures, and any redactions. Keep partial or noisy results and identify them;
they are useful evidence too.

Review files before committing them: filesystem paths or machine details can
appear in reports and logs. Never commit passwords, access tokens, keys,
serial numbers, customer data, or other private information. Preserve timing
and failure evidence; record any redactions in the notes. A commit or branch
containing these files lets us review them without blocking ongoing work.
