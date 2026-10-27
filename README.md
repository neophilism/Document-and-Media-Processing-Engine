# Document and Media Processing Engine

Reusable country-neutral engine. Initial capabilities and limits are documented in [architecture](docs/ARCHITECTURE.md); the canonical work plan is [registry/prs.json](registry/prs.json).

```sh
npm ci
npm run check
```

All research and jurisdiction configuration belong in downstream deployments. Implementation, local tests, CI, merge and live acceptance are separate.

The opt-in `./translation` interface runs a host-supplied local translator under bounded admission and result handling; see [translation limits and runtime responsibilities](docs/TRANSLATION.md).

`./text-jobs` provides the dedicated-database [text job ledger](docs/TEXT_JOBS.md), with one-shot consumption before processor I/O and mandatory trusted authorization/outcome verification.
