import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobFailure } from "../src/text-jobs.ts";
import { DomainError } from "../src/contracts.ts";
import { setup, c, w, input, outcome } from "./text-job-fixture.ts";
const proof: TextJobFailure = {
  receipt: {
    ...input.source,
    kind: "processing-receipt",
    id: "stopped",
    revision: 1,
  },
  reason: "processor-failed",
  processorStopped: true,
  noDerivative: true,
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
    if (operation === "retry")
      await f.jobs.settleNoOutput(w, "one", 3, proof, "failed");
    const before = await f.jobs.read(c, "one");
    const auditBefore = await scoped(f.db, c, (tx) => tx.audits());
    const late = new TextProcessingJobs(
      f.db,
      f.policy,
      {
        authorizeStart: blockedTrue,
        verifyOutcome: blockedTrue,
        verifyNoOutput: blockedTrue,
      },
      () => c.now,
    );
    const invoke = () =>
      operation === "consume"
        ? late.consume(w, "one", 2)
        : operation === "complete"
          ? late.complete(w, "one", 3, outcome, "late")
          : operation === "settle"
            ? late.settleNoOutput(w, "one", 3, proof, "late")
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

test("timely success, denial, nonboolean and sanitized callback failure all close verifier signals", async (t) => {
  const f = await setup(t);
  await f.jobs.lease(w, "one", 1);
  const signals: AbortSignal[] = [];
  for (const [result, code] of [
    [false, "verification_denied"],
    [1, "verification_denied"],
    [Error("private fixture diagnostic"), "verification_unavailable"],
    [
      new DomainError("private_code", "private fixture diagnostic"),
      "verification_unavailable",
    ],
  ] as const) {
    const jobs = new TextProcessingJobs(
      f.db,
      { ...f.policy, verificationTimeoutMs: 1000 },
      {
        authorizeStart: async (_j, _c, signal) => {
          signals.push(signal);
          if (result instanceof Error) throw result;
          return result as boolean;
        },
        verifyOutcome: async () => true,
      },
      () => c.now,
    );
    await assert.rejects(jobs.consume(w, "one", 2), (e: unknown) => {
      assert.ok(e instanceof DomainError);
      assert.equal(e.code, code);
      assert.equal(String(e).includes("private fixture"), false);
      return true;
    });
    assert.equal(signals.at(-1)?.aborted, true);
    assert.equal((await f.jobs.read(w, "one")).revision, 2);
  }
  const jobs = new TextProcessingJobs(
    f.db,
    { ...f.policy, verificationTimeoutMs: 1000 },
    {
      authorizeStart: async (_j, _c, signal) => {
        signals.push(signal);
        return true;
      },
      verifyOutcome: async () => true,
    },
    () => c.now,
  );
  await jobs.consume(w, "one", 2);
  assert.ok(signals.every((s) => s.aborted));
});

test("late asynchronous proof cannot resume the ledger after timeout", async (t) => {
  const f = await setup(t);
  await f.jobs.lease(w, "one", 1);
  let finish!: (value: boolean) => void;
  let signal: AbortSignal | undefined;
  const jobs = new TextProcessingJobs(
    f.db,
    f.policy,
    {
      authorizeStart: async (_j, _c, s) => {
        signal = s;
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
      verifyOutcome: async () => true,
    },
    () => c.now,
  );
  const before = await scoped(f.db, c, (tx) => tx.audits());
  await assert.rejects(jobs.consume(w, "one", 2), {
    code: "verification_timeout",
  });
  assert.equal(signal?.aborted, true);
  finish(true);
  await new Promise((r) => setImmediate(r));
  assert.equal((await f.jobs.read(w, "one")).revision, 2);
  assert.deepEqual(await scoped(f.db, c, (tx) => tx.audits()), before);
});

test("cached success, failure and retry receipts still require timely fresh verification", async (t) => {
  const f = await setup(t, {
    authorizeStart: async () => true,
    verifyOutcome: async () => true,
    verifyNoOutput: async () => true,
  });
  await f.jobs.lease(w, "one", 1);
  await f.jobs.consume(w, "one", 2);
  const completed = await f.jobs.complete(w, "one", 3, outcome, "done");
  await f.jobs.submit(c, { ...input, id: "failed" }, "second");
  await f.jobs.lease(w, "failed", 1);
  await f.jobs.consume(w, "failed", 2);
  const failed = await f.jobs.settleNoOutput(w, "failed", 3, proof, "failed");
  const child = await f.jobs.retryFailed(c, "failed", 4, "retry");
  const late = new TextProcessingJobs(
    f.db,
    f.policy,
    {
      authorizeStart: blockedTrue,
      verifyOutcome: blockedTrue,
      verifyNoOutput: blockedTrue,
    },
    () => c.now,
  );
  for (const operation of [
    () => late.complete(w, "one", 3, outcome, "done"),
    () => late.settleNoOutput(w, "failed", 3, proof, "failed"),
    () => late.retryFailed(c, "failed", 4, "retry"),
  ])
    await assert.rejects(operation(), { code: "verification_timeout" });
  assert.deepEqual(await f.jobs.read(w, "one"), completed);
  assert.deepEqual(await f.jobs.read(w, "failed"), failed);
  assert.deepEqual(await f.jobs.read(w, child.job.id), child);
});
