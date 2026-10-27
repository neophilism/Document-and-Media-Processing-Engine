import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobFailure } from "../src/text-jobs.ts";
import { c, w, input, outcome, setup } from "./text-job-fixture.ts";
const proof: TextJobFailure = {
  receipt: {
    ...input.source,
    kind: "processing-receipt",
    id: "no-output",
    revision: 4,
  },
  reason: "processor-failed",
  processorStopped: true,
  noDerivative: true,
};
async function consumed(t: test.TestContext) {
  const checks = {
    authorizeStart: async () => true,
    verifyOutcome: async () => true,
    verifyNoOutput: async () => true,
  };
  const fixture = await setup(t, checks);
  await fixture.jobs.lease(w, "one", 1);
  await fixture.jobs.consume(w, "one", 2);
  return fixture;
}
test("verified no-output settles terminal failure with immutable consumed history and atomic audit/outbox", async (t) => {
  const { jobs, db } = await consumed(t);
  const failure = await jobs.settleNoOutput(w, "one", 3, proof, "failure");
  assert.equal(failure.revision, 4);
  assert.equal(failure.job.status, "failed");
  assert.deepEqual(failure.job.failure, proof);
  assert.equal(failure.job.outcome, null);
  assert.equal(failure.job.consumedAt, c.now);
  assert.deepEqual(
    await jobs.settleNoOutput(w, "one", 3, proof, "failure"),
    failure,
  );
  await assert.rejects(jobs.lease(w, "one", 4));
  await assert.rejects(jobs.consume(w, "one", 4));
  await assert.rejects(jobs.cancel(c, "one", 4));
  await assert.rejects(jobs.complete(w, "one", 4, outcome, "late"));
  await scoped(db, c, async (tx) => {
    assert.ok(verifyAudit(await tx.audits()));
    assert.equal(
      (await tx.audits()).filter(
        (e) => e.type === "processing.no-output.verified",
      ).length,
      1,
    );
    const events = [];
    for (let i = 0; i < 4; i++) {
      const e = await tx.claim("inspector");
      if (!e) break;
      events.push(e.topic);
      await tx.acknowledge(e);
    }
    assert.deepEqual(events.toSorted(), [
      "processing.consumed",
      "processing.failed",
      "processing.queued",
    ]);
  });
});
test("absence denial and invalid/foreign no-output proof retain consumed uncertainty", async (t) => {
  const { jobs, db, policy } = await consumed(t);
  for (const checks of [
    { authorizeStart: async () => true, verifyOutcome: async () => true },
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: async () => false,
    },
  ])
    await assert.rejects(
      new TextProcessingJobs(db, policy, checks, () => c.now).settleNoOutput(
        w,
        "one",
        3,
        proof,
        "no",
      ),
    );
  for (const bad of [
    { ...proof, processorStopped: false },
    { ...proof, noDerivative: false },
    { ...proof, reason: "exception" },
    { ...proof, extra: "secret diagnostic" },
    { ...proof, receipt: { ...proof.receipt, projectId: "foreign" } },
  ])
    await assert.rejects(
      jobs.settleNoOutput(w, "one", 3, bad as TextJobFailure, "bad"),
    );
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
});
test("failure replay binds receipt reason purpose actor and current verification rather than accepting stale authority", async (t) => {
  const { jobs, db, policy } = await consumed(t);
  await jobs.settleNoOutput(w, "one", 3, proof, "failure");
  await assert.rejects(
    jobs.settleNoOutput(
      w,
      "one",
      3,
      { ...proof, reason: "runtime-expired" },
      "failure",
    ),
    { code: "idempotency_conflict" },
  );
  await assert.rejects(
    jobs.settleNoOutput({ ...w, purpose: "other" }, "one", 3, proof, "failure"),
    { code: "purpose_denied" },
  );
  const revoked = new TextProcessingJobs(
    db,
    { ...policy, workerIds: [] },
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: async () => true,
    },
    () => c.now,
  );
  await assert.rejects(revoked.settleNoOutput(w, "one", 3, proof, "failure"), {
    code: "worker_required",
  });
  const denied = new TextProcessingJobs(
    db,
    policy,
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: async () => false,
    },
    () => c.now,
  );
  await assert.rejects(denied.settleNoOutput(w, "one", 3, proof, "failure"), {
    code: "verification_denied",
  });
});
