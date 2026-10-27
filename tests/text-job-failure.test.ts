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

test("lost no-output acknowledgement reconciles one receipt and never a second failure event", async (t) => {
  const { jobs, db, policy, checks } = await consumed(t);
  let commits = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const value = await db.transaction(fn);
      if (++commits === 2) throw Error("lost failure acknowledgement");
      return value;
    },
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(flaky, policy, checks, () => c.now).settleNoOutput(
      w,
      "one",
      3,
      proof,
      "failure",
    ),
    /lost failure acknowledgement/,
  );
  const replay = await jobs.settleNoOutput(w, "one", 3, proof, "failure");
  assert.equal(replay.revision, 4);
  assert.equal(replay.job.status, "failed");
  await scoped(db, c, async (tx) =>
    assert.equal(
      (await tx.audits()).filter(
        (a) => a.type === "processing.no-output.verified",
      ).length,
      1,
    ),
  );
});
test("post-outbox failure rolls back no-output revision audit receipt and idempotency together", async (t) => {
  const { jobs, db, policy, checks } = await consumed(t);
  let injected = false;
  const bad: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const result = await q.query<T>(sql, v);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              v?.includes("processing.failed")
            ) {
              injected = true;
              throw Error("injected failure");
            }
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(bad, policy, checks, () => c.now).settleNoOutput(
      w,
      "one",
      3,
      proof,
      "failure",
    ),
    /injected failure/,
  );
  assert.equal(injected, true);
  assert.equal((await jobs.read(w, "one")).revision, 3);
  await scoped(db, c, async (tx) =>
    assert.equal(
      (await tx.audits()).filter(
        (a) => a.type === "processing.no-output.verified",
      ).length,
      0,
    ),
  );
  assert.equal(
    (await jobs.settleNoOutput(w, "one", 3, proof, "failure")).revision,
    4,
  );
});
test("late timeout exceptional and nonboolean no-output verification cannot settle consumed uncertainty", async (t) => {
  const { jobs, db, policy } = await consumed(t);
  let signal: AbortSignal | undefined, finish!: (v: boolean) => void;
  const hanging = new TextProcessingJobs(
    db,
    policy,
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: (_j, _p, _r, _c, s) => {
        signal = s;
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    },
    () => c.now,
  );
  await assert.rejects(hanging.settleNoOutput(w, "one", 3, proof, "failure"), {
    code: "verification_timeout",
  });
  assert.equal(signal?.aborted, true);
  finish(true);
  await new Promise((resolve) => setImmediate(resolve));
  for (const callback of [
    async () => 1 as unknown as boolean,
    async () => {
      throw Error("private diagnostics must not escape");
    },
  ]) {
    const failing = new TextProcessingJobs(
      db,
      policy,
      {
        authorizeStart: async () => true,
        verifyOutcome: async () => true,
        verifyNoOutput: callback,
      },
      () => c.now,
    );
    await assert.rejects(
      failing.settleNoOutput(w, "one", 3, proof, "failure"),
      (error) => {
        assert.ok(
          ["verification_denied", "verification_unavailable"].includes(
            (error as { code: string }).code,
          ),
        );
        assert.equal(String(error).includes("private diagnostics"), false);
        return true;
      },
    );
  }
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
});
test("competing success and stopped-no-output receipts serialize to one terminal revision", async (t) => {
  const { jobs, db } = await consumed(t);
  const results = await Promise.allSettled([
    jobs.complete(w, "one", 3, outcome, "success"),
    jobs.settleNoOutput(w, "one", 3, proof, "failure"),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(results.filter((r) => r.status === "rejected").length, 1);
  const current = await jobs.read(w, "one");
  assert.equal(current.revision, 4);
  assert.ok(["completed", "failed"].includes(current.job.status));
  await scoped(db, c, async (tx) => {
    assert.ok(verifyAudit(await tx.audits()));
    const topics = [];
    for (let n = 0; n < 4; n++) {
      const e = await tx.claim("inspector");
      if (!e) break;
      topics.push(e.topic);
      await tx.acknowledge(e);
    }
    assert.equal(
      topics.filter((x) =>
        ["processing.failed", "processing.completed"].includes(x),
      ).length,
      1,
    );
  });
});
test("stale proof rejects before verification; manager recovery and mutable inputs retain exact captured binding", async (t) => {
  const { jobs, db, policy } = await consumed(t);
  let calls = 0,
    started!: () => void,
    finish!: (v: boolean) => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const recovery = new TextProcessingJobs(
    db,
    { ...policy, verificationTimeoutMs: 1000 },
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => true,
      verifyNoOutput: async (job, failure, revision, context) => {
        calls++;
        assert.equal(job.source.id, input.source.id);
        assert.equal(failure.receipt.id, proof.receipt.id);
        assert.equal(revision, 3);
        assert.equal(context.actor.id, c.actor.id);
        started();
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    },
    () => c.now,
  );
  await assert.rejects(recovery.settleNoOutput(c, "one", 2, proof, "stale"), {
    code: "revision_conflict",
  });
  assert.equal(calls, 0);
  const supplied = structuredClone(proof),
    context = structuredClone(c);
  const pending = recovery.settleNoOutput(
    context,
    "one",
    3,
    supplied,
    "recover",
  );
  await ready;
  (supplied.receipt as { id: string }).id = "changed";
  (context.actor as { id: string }).id = "changed";
  finish(true);
  const failure = await pending;
  assert.equal(failure.job.failure?.receipt.id, proof.receipt.id);
  assert.equal(failure.job.status, "failed");
  assert.equal((await jobs.read(w, "one")).revision, 4);
});
