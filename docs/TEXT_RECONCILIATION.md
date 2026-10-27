# Explicit private text-output reconciliation

`./text-reconciliation` exports `reconcileTextOutput(jobs, outputs, context, request, clock, options?)`. It coordinates the accepted `TextProcessingJobs` and `PrivateTextOutputs` services in the same Processing-owned operational database, with its own private encrypted object root. It adds no database schema, provider connection or HTTP listener. A host must authenticate the human manager and supply trusted scope, roles, purpose, request identity and current clock; request strings and consumed snapshots are not credentials. Both owning services still enforce their captured listed identities. Recreate configured services when host identity policy changes.

The request has exactly three fields: `jobId`, `consumedRevision` and `output`. Choose the output action explicitly:

- `recover-stored` verifies a previously committed private artifact/receipt. It never upgrades a pending intent into a stored receipt.
- `finalize-existing` invokes the accepted manager-only pending finalizer. It authenticates already-existing encrypted bytes and current source/policy, then may publish immutable metadata/receipt records. It never writes output bytes or dispatches processor work. An already stored result goes through current verification.

Both actions require an exact consumed edition (or its matching completed successor). Independent verification binds the output service's execution to the job ledger snapshot as well as its actual encrypted output and current source. Hosts must configure the services against the same processing database; matching job IDs alone do not establish an execution binding. Private outcome proof re-reads authorized source bytes and recomputes UTF8 extraction for verification, even though no processing job is restarted. Use a properly configured `outputs.verifyOutcome` on the ledger as well; the coordinator does not replace the ledger's mandatory verifier.

After output proof, the coordinator rereads the owning ledger and completes the exact consumed revision through the existing verified command API. A deterministic completion command binds job and consumed revision. An already completed matching outcome returns a fresh verified acknowledgement; current policy/source denial still prevents replay. A competing completion may cause uncertainty; the caller must explicitly request reconciliation again. There is no automatic replay, execution retry, lease consumption, no-output settlement, orphan cleanup, plaintext download, send or publication control.

`completed` returns only job ID/revision and exact private outcome references. `reconciliation-required` returns only a diagnostic phase (`inspection`, `output` or `completion`). A phase does **not** certify rollback or absence of an effect. An output publication may have committed even if its acknowledgement was lost, and output publication/completion are separate transactions. A completion error may hide a committed completed job. No failure produces permission to execute again. Do not queue a replacement job to bypass uncertainty. A later explicit `recover-stored` can verify a committed outcome and finish or acknowledge completion.

The trusted clock is sampled before each stage and must not regress. The default admission deadline is 10 seconds, configurable from 10 to 30000 milliseconds. Cancellation/deadline gates prevent admission of later stages and propagate cancellation into output verification; every owned signal closes on exit. Begun SQL/filesystem operations are awaited, never raced: they may outlive cancellation or the deadline. A durably acknowledged completion remains completed even if cancellation arrived while SQL acknowledgement was delayed. Blocking JavaScript cannot be preempted. The stage gates are not a whole-operation wall-clock guarantee, and callbacks must remain independently bounded/read-only. Cross-service policy changes are not atomically fenced.

For a host that already authenticated a permitted manager and constructed the two owning services:

```ts
const result = await reconcileTextOutput(jobs, outputs, authenticatedContext, {
  jobId: "review-text-1",
  consumedRevision: 3,
  output: "recover-stored",
}, trustedClock, { signal: requestCancellation });
```

This typed integration does not authenticate end users or expose a network endpoint. TLS/session/CSRF/host authorization, durable operator request UX, production source/key/storage qualification, stopped/quiescent proof services and safe cleanup remain separate prerequisites. Missing receipts or expired leases never prove absent output or stopped execution.

Development tests use actual canonical migrations and restricted PGlite roles, real temporary AES-GCM objects and controlled host source/current-policy callbacks. Twelve new groups cover existing pending finalization, stored recovery, completed replay/current denial, role/scope/purpose/revision admission, lost output/completion acknowledgements, completion rollback, cancellation/deadlines/regressed clocks, captured inputs/receivers, competing managers and same-ID foreign execution bindings. This adds no real PostgreSQL network-session, whole-process crash/power-loss, browser, deployed identity/source/storage or live acceptance evidence. Existing PostgreSQL/Docker regression CI remains required on the final head.

Local checkpoint: all 106 ordinary groups/typecheck/build/registry pass, and all twelve new reconciliation groups pass through installed compiled public exports. Prior source ab24da5f3ed24c6328536873ee9b708dcf006e20 CI40556884749 passed ordinary checks, PostgreSQL and Docker OCR, but its Docker PDF extraction exceeded the existing five-second runtime limit. No PDF runtime change is included. This failed attempt is not final-head acceptance; all final-head jobs must pass before merge.
