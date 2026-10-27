import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobVerification } from "../src/text-jobs.ts";
import { c, w, input, outcome, setup } from "./text-job-fixture.ts";

test("durable text lifecycle commits exact revisions and audited private outcome once", async (t) => {
  const { jobs, db } = await setup(t);
  const lease = await jobs.lease(w, "one", 1);
  const permit = await jobs.consume(w, "one", lease.revision);
  assert.equal(permit.job.status, "consumed");
  const done = await jobs.complete(
    w,
    "one",
    permit.revision,
    outcome,
    "complete",
  );
  assert.equal(done.revision, 4);
  assert.deepEqual(done.job.outcome, outcome);
  assert.deepEqual(
    await jobs.complete(w, "one", permit.revision, outcome, "complete"),
    done,
  );
  await assert.rejects(jobs.consume(w, "one", lease.revision));
  await scoped(db, c, async (tx) => {
    assert.ok(verifyAudit(await tx.audits()));
    assert.equal((await tx.list("text-processing-job")).length, 1);
  });
});
test("lost consumption acknowledgement leaves uncertainty and never reissues execution", async (t) => {
  const { jobs, db, policy, checks } = await setup(t);
  await jobs.lease(w, "one", 1);
  let commits = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const value = await db.transaction(fn);
      if (++commits === 2) throw Error("lost after commit");
      return value;
    },
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(flaky, policy, checks, () => c.now).consume(
      w,
      "one",
      2,
    ),
    /lost after commit/,
  );
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
  await assert.rejects(jobs.consume(w, "one", 2));
  await assert.rejects(
    jobs.lease({ ...w, now: "2026-01-02T00:00:00Z" }, "one", 3),
  );
  await assert.rejects(jobs.cancel(c, "one", 3));
  assert.equal(
    (await jobs.complete(c, "one", 3, outcome, "recover")).job.status,
    "completed",
  );
});
test("expired unconsumed work can be re-leased but prior revisions and actors cannot consume it", async (t) => {
  const { jobs, time } = await setup(t);
  await jobs.lease(w, "one", 1, 1000);
  const later = { ...w, now: "2026-01-01T00:00:02Z" };
  time.now = later.now;
  await assert.rejects(jobs.consume(later, "one", 2));
  const lease = await jobs.lease(later, "one", 2);
  assert.equal(lease.revision, 3);
  await assert.rejects(jobs.consume(later, "one", 2));
  await assert.rejects(
    jobs.consume(
      { ...later, actor: { ...later.actor, id: "intruder" } },
      "one",
      3,
    ),
    { code: "worker_required" },
  );
  assert.equal((await jobs.consume(later, "one", 3)).job.status, "consumed");
});
test("scope purpose human administration and exact outcomes are enforced before mutation", async (t) => {
  const { jobs } = await setup(t);
  await assert.rejects(jobs.submit(w, { ...input, id: "bad" }, "bad"), {
    code: "manager_required",
  });
  await assert.rejects(jobs.read({ ...w, projectId: "foreign" }, "one"), {
    code: "scope_denied",
  });
  await assert.rejects(jobs.lease({ ...w, purpose: "other" }, "one", 1), {
    code: "purpose_denied",
  });
  await jobs.lease(w, "one", 1);
  await jobs.consume(w, "one", 2);
  await assert.rejects(
    jobs.complete(
      w,
      "one",
      3,
      { ...outcome, receipt: { ...outcome.receipt, projectId: "foreign" } },
      "bad",
    ),
    { code: "scope_denied" },
  );
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
});
test("mandatory bounded current authorization and exact receipt verification fail closed", async (t) => {
  let observed: AbortSignal | undefined;
  const checks: TextJobVerification = {
    authorizeStart: async () => false,
    verifyOutcome: async () => false,
  };
  const { jobs, db, policy } = await setup(t, checks);
  await jobs.lease(w, "one", 1);
  await assert.rejects(jobs.consume(w, "one", 2), {
    code: "verification_denied",
  });
  const hanging = new TextProcessingJobs(
    db,
    policy,
    {
      authorizeStart: (_j, _c, signal) => {
        observed = signal;
        return new Promise(() => {});
      },
      verifyOutcome: async () => true,
    },
    () => c.now,
  );
  await assert.rejects(hanging.consume(w, "one", 2), {
    code: "verification_timeout",
  });
  assert.equal(observed?.aborted, true);
  const allowed = new TextProcessingJobs(
    db,
    policy,
    {
      authorizeStart: async () => true,
      verifyOutcome: async () => false,
    },
    () => c.now,
  );
  await allowed.consume(w, "one", 2);
  await assert.rejects(allowed.complete(w, "one", 3, outcome, "bad"), {
    code: "verification_denied",
  });
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
});

test("lease expiry and manager cancellation during asynchronous authorization prevent consumption", async (t) => {
  let advance = () => {};
  const checks: TextJobVerification = {
    authorizeStart: async () => {
      advance();
      return true;
    },
    verifyOutcome: async () => true,
  };
  const { jobs, time, db, policy } = await setup(t, checks);
  await jobs.lease(w, "one", 1, 1000);
  advance = () => {
    time.now = "2026-01-01T00:00:02Z";
  };
  await assert.rejects(jobs.consume(w, "one", 2), { code: "lease_denied" });
  assert.equal((await jobs.read(w, "one")).job.status, "leased");
  let start!: () => void, finish!: (v: boolean) => void;
  const ready = new Promise<void>((r) => {
    start = r;
  });
  const delayed = new TextProcessingJobs(
    db,
    { ...policy, verificationTimeoutMs: 1000 },
    {
      authorizeStart: () => {
        start();
        return new Promise((r) => {
          finish = r;
        });
      },
      verifyOutcome: async () => true,
    },
    () => c.now,
  );
  const pending = delayed.consume(w, "one", 2);
  await ready;
  await jobs.cancel(c, "one", 2);
  finish(true);
  await assert.rejects(pending, { code: "revision_conflict" });
  assert.equal((await jobs.read(w, "one")).job.status, "cancelled");
});
test("concurrent consumption returns only one permit and never silently replays the winner", async (t) => {
  const { jobs } = await setup(t);
  await jobs.lease(w, "one", 1);
  const results = await Promise.allSettled([
    jobs.consume(w, "one", 2),
    jobs.consume(w, "one", 2),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(results.filter((r) => r.status === "rejected").length, 1);
  assert.equal((await jobs.read(w, "one")).revision, 3);
});
test("post-insert failure rolls back consumption record audit and outbox together", async (t) => {
  const { jobs, db, policy, checks } = await setup(t);
  await jobs.lease(w, "one", 1);
  let injected = false;
  const bad: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const result = await q.query<T>(sql, v);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              v?.includes("processing.consumed")
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
    new TextProcessingJobs(bad, policy, checks, () => c.now).consume(
      w,
      "one",
      2,
    ),
    /injected failure/,
  );
  assert.equal(injected, true);
  assert.equal((await jobs.read(w, "one")).revision, 2);
  assert.equal((await jobs.consume(w, "one", 2)).revision, 3);
  await scoped(db, c, async (tx) => {
    assert.ok(verifyAudit(await tx.audits()));
    const topics: string[] = [];
    for (let n = 0; n < 3; n++) {
      const e = await tx.claim("inspector");
      if (!e) break;
      topics.push(e.topic);
      await tx.acknowledge(e);
    }
    assert.deepEqual(topics.toSorted(), [
      "processing.consumed",
      "processing.queued",
    ]);
  });
});
test("lost completion acknowledgement reconciles one exact receipt after fresh authority checks", async (t) => {
  const { jobs, db, policy, checks } = await setup(t);
  await jobs.lease(w, "one", 1);
  await jobs.consume(w, "one", 2);
  let count = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const value = await db.transaction(fn);
      if (++count === 2) throw Error("lost completion ack");
      return value;
    },
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(flaky, policy, checks, () => c.now).complete(
      w,
      "one",
      3,
      outcome,
      "complete",
    ),
    /lost completion ack/,
  );
  assert.equal(
    (await jobs.complete(w, "one", 3, outcome, "complete")).revision,
    4,
  );
  await assert.rejects(
    jobs.complete(
      w,
      "one",
      3,
      { ...outcome, sha256: "c".repeat(64) },
      "complete",
    ),
    { code: "idempotency_conflict" },
  );
  const denied = new TextProcessingJobs(
    db,
    policy,
    { authorizeStart: async () => true, verifyOutcome: async () => false },
    () => c.now,
  );
  await assert.rejects(denied.complete(w, "one", 3, outcome, "complete"), {
    code: "verification_denied",
  });
  assert.equal((await jobs.read(w, "one")).revision, 4);
});

test("live time is checked after the database project lock rather than before storage contention", async (t) => {
  const { jobs, db, policy, checks } = await setup(t);
  await jobs.lease(w, "one", 1, 1000);
  let now = c.now,
    locked = false;
  const contended: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const result = await q.query<T>(sql, v);
            if (sql.includes("FOR UPDATE")) {
              locked = true;
              now = "2026-01-01T00:00:02Z";
            }
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new TextProcessingJobs(contended, policy, checks, () => now).consume(
      w,
      "one",
      2,
    ),
    { code: "lease_denied" },
  );
  assert.equal(locked, true);
  assert.equal((await jobs.read(w, "one")).revision, 2);
});
