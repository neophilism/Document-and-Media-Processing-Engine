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

test("retry and historical queue replay require current manager mapping and fresh strict proof verification", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  const child = await jobs.retryFailed(c, "one", 4, "retry");
  const revoked = new TextProcessingJobs(
    db,
    { ...policy, managerIds: [] },
    checks,
    () => c.now,
  );
  await assert.rejects(revoked.retryFailed(c, "one", 4, "retry"), {
    code: "manager_required",
  });
  const without = new TextProcessingJobs(
    db,
    policy,
    {
      authorizeStart: checks.authorizeStart,
      verifyOutcome: checks.verifyOutcome,
    },
    () => c.now,
  );
  await assert.rejects(without.retryFailed(c, "one", 4, "retry"), {
    code: "verification_unavailable",
  });
  for (const callback of [
    async () => false,
    async () => 1 as unknown as boolean,
    async () => {
      throw Error("private receipt diagnostics");
    },
  ]) {
    const denied = new TextProcessingJobs(
      db,
      policy,
      { ...checks, verifyNoOutput: callback },
      () => c.now,
    );
    await assert.rejects(denied.retryFailed(c, "one", 4, "retry"), (error) => {
      assert.ok(
        ["verification_denied", "verification_unavailable"].includes(
          (error as { code: string }).code,
        ),
      );
      assert.equal(
        String(error).includes("private receipt diagnostics"),
        false,
      );
      return true;
    });
  }
  assert.deepEqual(await jobs.read(c, child.job.id), child);
});

test("timed-out retry proof cannot queue a late child and captured context/proof cannot be mutated by caller or verifier", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  let signal: AbortSignal | undefined, finish!: (v: boolean) => void;
  const late = new TextProcessingJobs(
    db,
    policy,
    {
      ...checks,
      verifyNoOutput: async (_j, _p, _r, _c, s) => {
        signal = s;
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    },
    () => c.now,
  );
  await assert.rejects(late.retryFailed(c, "one", 4, "late"), {
    code: "verification_timeout",
  });
  assert.equal(signal?.aborted, true);
  finish(true);
  await new Promise((resolve) => setImmediate(resolve));
  await scoped(db, c, async (tx) =>
    assert.equal((await tx.list("text-processing-job")).length, 1),
  );
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const guarded = new TextProcessingJobs(
    db,
    { ...policy, verificationTimeoutMs: 1000 },
    {
      ...checks,
      verifyNoOutput: async (j, p, revision, context) => {
        assert.equal(j.status, "failed");
        assert.equal(revision, 3);
        assert.deepEqual(p, proof);
        assert.equal(context.actor.id, "owner");
        (j.source as { id: string }).id = "verifier-changed";
        (p.receipt as { id: string }).id = "verifier-changed";
        (context.actor as { id: string }).id = "verifier-changed";
        entered();
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    },
    () => c.now,
  );
  const supplied = structuredClone(c);
  const pending = guarded.retryFailed(supplied, "one", 4, "retry");
  await ready;
  (supplied.actor as { id: string }).id = "caller-changed";
  (supplied as { purpose: string }).purpose = "caller-changed";
  finish(true);
  const child = await pending;
  assert.equal(child.job.submittedBy, "owner");
  assert.equal(child.job.purpose, c.purpose);
  assert.deepEqual(child.job.source, input.source);
  assert.deepEqual(child.job.retry?.failureReceipt, proof.receipt);
  assert.deepEqual((await jobs.read(c, "one")).job.failure, proof);
});

test("post-outbox rollback removes child audit and idempotency atomically; preexisting deterministic IDs never overwrite", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  let injected = false;
  const broken: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const value = await q.query<T>(sql, v);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              v?.includes("processing.queued")
            ) {
              injected = true;
              throw Error("injected after child outbox");
            }
            return value;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(broken, policy, checks, () => c.now).retryFailed(
      c,
      "one",
      4,
      "retry",
    ),
    /injected after child outbox/,
  );
  assert.equal(injected, true);
  await scoped(db, c, async (tx) => {
    assert.equal((await tx.list("text-processing-job")).length, 1);
    assert.equal(
      (await tx.audits()).filter((a) => a.type === "processing.retry.queued")
        .length,
      0,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
  const id = `retry-${digest({ parentJobId: "one", failedRevision: 4 })}`;
  await jobs.submit(c, { ...input, id }, "collision");
  await assert.rejects(jobs.retryFailed(c, "one", 4, "retry"), {
    code: "revision_conflict",
  });
  assert.equal((await jobs.read(c, id)).job.retry, undefined);
  assert.equal((await jobs.read(c, "one")).job.status, "failed");
  // A different parent demonstrates that the rolled-back command was not cached.
  await jobs.submit(c, { ...input, id: "second" }, "second");
  await jobs.lease(w, "second", 1);
  await jobs.consume(w, "second", 2);
  await jobs.settleNoOutput(w, "second", 3, proof, "second-failure");
  const child = await jobs.retryFailed(c, "second", 4, "retry");
  assert.equal(child.job.retry?.parentJobId, "second");
});

test("sixteen explicit generations preserve root lineage and each failure receipt; limit denies before new verification", async (t) => {
  const { jobs, db, policy, checks } = await failed(t);
  let currentId = "one";
  for (let generation = 1; generation <= 16; generation++) {
    const child = await jobs.retryFailed(
      c,
      currentId,
      4,
      `retry-${generation}`,
    );
    assert.equal(child.job.retry?.generation, generation);
    assert.equal(child.job.retry?.rootJobId, "one");
    assert.equal(child.job.retry?.parentJobId, currentId);
    assert.equal(child.job.failure, undefined);
    assert.equal(child.revision, 1);
    assert.equal((await jobs.read(c, currentId)).job.status, "failed");
    currentId = child.job.id;
    await jobs.lease(w, currentId, 1);
    await jobs.consume(w, currentId, 2);
    const nextProof = {
      ...proof,
      receipt: { ...proof.receipt, id: `stopped-${generation}` },
    };
    await jobs.settleNoOutput(
      w,
      currentId,
      3,
      nextProof,
      `failure-${generation}`,
    );
    if (generation < 16) {
      // Reuse of an earlier command for a different parent is never a new child.
      await assert.rejects(jobs.retryFailed(c, currentId, 4, "retry-1"), {
        code: "idempotency_conflict",
      });
    }
  }
  let called = false;
  const bounded = new TextProcessingJobs(
    db,
    policy,
    {
      ...checks,
      verifyNoOutput: async () => {
        called = true;
        return true;
      },
    },
    () => c.now,
  );
  await assert.rejects(bounded.retryFailed(c, currentId, 4, "too-many"), {
    code: "retry_limit",
  });
  assert.equal(called, false);
  await scoped(db, c, async (tx) => {
    assert.equal((await tx.list("text-processing-job")).length, 17);
    assert.equal(
      (await tx.audits()).filter((a) => a.type === "processing.retry.queued")
        .length,
      16,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
  await assert.rejects(
    jobs.submit(
      c,
      { ...input, id: "forged", retry: { generation: 1 } } as typeof input,
      "forged",
    ),
    { code: "invalid_job" },
  );
});
