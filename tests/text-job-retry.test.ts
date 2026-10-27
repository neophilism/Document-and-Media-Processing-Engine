import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJob, TextJobFailure } from "../src/text-jobs.ts";
import { digest } from "../src/contracts.ts";
import { c, w, input, outcome, setup } from "./text-job-fixture.ts";

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
async function failed(t: test.TestContext) {
  const f = await setup(t, {
    authorizeStart: async () => true,
    verifyOutcome: async () => true,
    verifyNoOutput: async () => true,
  });
  await f.jobs.lease(w, "one", 1);
  await f.jobs.consume(w, "one", 2);
  await f.jobs.settleNoOutput(w, "one", 3, proof, "failed");
  return f;
}
test("explicit retry preserves failed history and exact source, queues one linked child and requires new lease/start verification", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  const parent = await jobs.read(c, "one");
  const child = await jobs.retryFailed(c, "one", 4, "retry");
  assert.equal(child.revision, 1);
  assert.equal(child.job.status, "queued");
  assert.equal(child.job.workerId, null);
  assert.equal(child.job.workerKind, null);
  assert.equal(child.job.consumedAt, null);
  assert.equal(child.job.leaseExpiresAt, null);
  assert.equal(child.job.outcome, null);
  assert.equal(child.job.failure, undefined);
  assert.deepEqual(child.job.source, input.source);
  assert.equal(child.job.sourceSha256, input.sourceSha256);
  assert.equal(child.job.sourceBytes, input.sourceBytes);
  assert.equal(child.job.language, input.language);
  assert.equal(child.job.profile, parent.job.profile);
  assert.equal(child.job.purpose, c.purpose);
  assert.deepEqual(child.job.retry, {
    rootJobId: "one",
    generation: 1,
    parentJobId: "one",
    parentFailedRevision: 4,
    failureReceipt: proof.receipt,
  });
  assert.deepEqual(await jobs.read(c, "one"), parent);
  // Original settlement replay remains valid: retry never changes its parent.
  assert.deepEqual(
    await jobs.settleNoOutput(w, "one", 3, proof, "failed"),
    parent,
  );
  await assert.rejects(jobs.consume(w, child.job.id, 1), {
    code: "lease_denied",
  });
  await jobs.lease(w, child.job.id, 1);
  const denied = new TextProcessingJobs(
    db,
    policy,
    { ...checks, authorizeStart: async () => false },
    () => c.now,
  );
  await assert.rejects(denied.consume(w, child.job.id, 2), {
    code: "verification_denied",
  });
  assert.equal((await jobs.read(w, child.job.id)).job.status, "leased");
  await jobs.consume(w, child.job.id, 2);
  // A command replay is the original queue receipt, never another permit.
  assert.deepEqual(await jobs.retryFailed(c, "one", 4, "retry"), child);
  assert.equal((await jobs.read(c, child.job.id)).job.status, "consumed");
  await jobs.complete(w, child.job.id, 3, outcome, "done-child");
  await scoped(db, c, async (tx) => {
    assert.ok(verifyAudit(await tx.audits()));
    assert.equal(
      (await tx.audits()).filter((a) => a.type === "processing.retry.queued")
        .length,
      1,
    );
    assert.equal((await tx.list<TextJob>("text-processing-job")).length, 2);
  });
});
test("retry rejects workers, wrong scope/purpose, stale editions and every nonfailed status before proof verification", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  let calls = 0;
  const tested = new TextProcessingJobs(
    db,
    policy,
    {
      ...checks,
      verifyNoOutput: async () => {
        calls++;
        return true;
      },
    },
    () => c.now,
  );
  await assert.rejects(tested.retryFailed(w, "one", 4, "worker"), {
    code: "manager_required",
  });
  await assert.rejects(
    tested.retryFailed({ ...c, projectId: "other" }, "one", 4, "scope"),
  );
  await assert.rejects(
    tested.retryFailed({ ...c, purpose: "other" }, "one", 4, "purpose"),
    { code: "purpose_denied" },
  );
  for (const revision of [0, 1, 3, 5, NaN, Infinity, 4.5])
    await assert.rejects(tested.retryFailed(c, "one", revision, "stale"));
  for (const status of [
    "queued",
    "leased",
    "consumed",
    "completed",
    "cancelled",
  ] as const) {
    const id = status;
    await jobs.submit(c, { ...input, id }, `submit-${id}`);
    let revision = 1;
    if (["leased", "consumed", "completed"].includes(status)) {
      await jobs.lease(w, id, revision++);
      if (status !== "leased") {
        await jobs.consume(w, id, revision++);
        if (status === "completed")
          await jobs.complete(w, id, revision++, outcome, "done");
      }
    } else if (status === "cancelled") await jobs.cancel(c, id, revision++);
    await assert.rejects(tested.retryFailed(c, id, revision, `retry-${id}`));
  }
  assert.equal(calls, 0);
});
test("competing managers and command ids cannot create two successors or reset an advanced child", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  const other = { ...c, actor: { ...c.actor, id: "other-owner" } };
  const peer = new TextProcessingJobs(
    db,
    { ...policy, managerIds: ["owner", "other-owner"] },
    checks,
    () => c.now,
  );
  const results = await Promise.allSettled([
    jobs.retryFailed(c, "one", 4, "first"),
    peer.retryFailed(other, "one", 4, "second"),
  ]);
  const win = results.find((r) => r.status === "fulfilled");
  assert.ok(win?.status === "fulfilled");
  assert.equal(results.filter((r) => r.status === "rejected").length, 1);
  await jobs.lease(w, win.value.job.id, 1);
  await jobs.consume(w, win.value.job.id, 2);
  await assert.rejects(peer.retryFailed(other, "one", 4, "third"));
  assert.equal((await jobs.read(w, win.value.job.id)).revision, 3);
  await scoped(db, c, async (tx) => {
    assert.equal((await tx.list("text-processing-job")).length, 2);
    assert.equal(
      (await tx.audits()).filter((a) => a.type === "processing.retry.queued")
        .length,
      1,
    );
    const events = [];
    for (let i = 0; i < 8; i++) {
      const e = await tx.claim("inspector");
      if (!e) break;
      if (e.topic === "processing.queued") events.push(e);
      await tx.acknowledge(e);
    }
    assert.equal(events.length, 2); // one root + one child
  });
});
test("post-COMMIT lost retry acknowledgement replays the same child with no duplicate event", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  let commits = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const value = await db.transaction(fn);
      if (++commits === 2) throw Error("lost retry acknowledgement");
      return value;
    },
    close: async () => {},
  };
  const uncertain = new TextProcessingJobs(flaky, policy, checks, () => c.now);
  await assert.rejects(
    uncertain.retryFailed(c, "one", 4, "retry"),
    /lost retry acknowledgement/,
  );
  const child = await jobs.retryFailed(c, "one", 4, "retry");
  assert.equal(child.revision, 1);
  assert.equal(child.job.retry?.parentJobId, "one");
  assert.deepEqual(await jobs.read(c, child.job.id), child);
  await scoped(db, c, async (tx) =>
    assert.equal(
      (await tx.audits()).filter((a) => a.type === "processing.retry.queued")
        .length,
      1,
    ),
  );
});
