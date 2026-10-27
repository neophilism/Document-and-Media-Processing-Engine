import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobFailure } from "../src/text-jobs.ts";
import { setup, c, w, input, outcome } from "./text-job-fixture.ts";
const proof: TextJobFailure = {
  receipt: { ...input.source, kind: "processing-receipt", id: "stopped", revision: 1 },
  reason: "processor-failed", processorStopped: true, noDerivative: true,
};
// Deliberately prevent the timer from running before the callback resolves.
function blockedTrue() {
  const until = performance.now() + 30;
  while (performance.now() < until) {}
  return Promise.resolve(true);
}
for (const operation of ["consume", "complete", "settle", "retry"] as const) {
  test(`blocked late ${operation} verification rejects before any ledger transition`, async (t) => {
    const f = await setup(t, {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: async () => true,
    });
    await f.jobs.lease(w, "one", 1);
    if (operation !== "consume") await f.jobs.consume(w, "one", 2);
    if (operation === "retry") await f.jobs.settleNoOutput(w, "one", 3, proof, "failed");
    const before = await f.jobs.read(c, "one");
    const auditBefore = await scoped(f.db, c, (tx) => tx.audits());
    const late = new TextProcessingJobs(f.db, f.policy, {
      authorizeStart: blockedTrue, verifyOutcome: blockedTrue, verifyNoOutput: blockedTrue,
    }, () => c.now);
    const invoke = () => operation === "consume" ? late.consume(w, "one", 2)
      : operation === "complete" ? late.complete(w, "one", 3, outcome, "late")
      : operation === "settle" ? late.settleNoOutput(w, "one", 3, proof, "late")
      : late.retryFailed(c, "one", 4, "late");
    await assert.rejects(invoke(), { code: "verification_timeout" });
    assert.deepEqual(await f.jobs.read(c, "one"), before);
    await scoped(f.db, c, async (tx) => {
      assert.deepEqual(await tx.audits(), auditBefore);
      assert.ok(verifyAudit(await tx.audits()));
      assert.equal((await tx.list("text-processing-job")).length, 1);
    });
  });
}
