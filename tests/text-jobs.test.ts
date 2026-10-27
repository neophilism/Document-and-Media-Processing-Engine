import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobVerification } from "../src/text-jobs.ts";
import type { Context } from "../src/contracts.ts";
const c: Context = {
  tenantId: "fiction",
  projectId: "processing",
  actor: { id: "owner", kind: "human", roles: ["owner"] },
  purpose: "review",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
const w: Context = {
  ...c,
  actor: { id: "worker", kind: "service", roles: ["process"] },
};
const input = {
  id: "one",
  source: {
    tenantId: c.tenantId,
    projectId: c.projectId,
    kind: "evidence",
    id: "text",
    revision: 2,
  },
  sourceSha256: "a".repeat(64),
  sourceBytes: 5,
  language: null,
};
const outcome = {
  derivative: {
    ...input.source,
    kind: "processed-artifact",
    id: "derived",
    revision: 3,
  },
  sha256: "b".repeat(64),
  receipt: {
    ...input.source,
    kind: "processing-receipt",
    id: "receipt",
    revision: 4,
  },
};
async function setup(
  t: test.TestContext,
  checks: TextJobVerification = {
    authorizeStart: async () => true,
    verifyOutcome: async () => true,
  },
) {
  const pg = new PGlite();
  await pg.exec(
    await readFile(
      new URL(
        "../migrations/001_foundation.sql",
        import.meta.resolve("@accountability/evidence/database"),
      ),
      "utf8",
    ),
  );
  await pg.exec(
    "CREATE ROLE processing_login LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; GRANT evidence_runtime TO processing_login; SET SESSION AUTHORIZATION processing_login",
  );
  const db: Database = {
    transaction: (fn) =>
      pg.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => ({
            rows: (await q.query(sql, v)).rows as T[],
          }),
        }),
      ),
    close: () => pg.close(),
  };
  t.after(() => pg.close());
  const policy = {
    tenantId: c.tenantId,
    projectId: c.projectId,
    managerIds: ["owner"],
    workerIds: ["worker"],
    verificationTimeoutMs: 10,
  };
  const jobs = new TextProcessingJobs(db, policy, checks);
  await jobs.submit(c, input, "submit");
  return { db, jobs, policy, checks };
}
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
    new TextProcessingJobs(flaky, policy, checks).consume(w, "one", 2),
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
  const { jobs } = await setup(t);
  await jobs.lease(w, "one", 1, 1000);
  const later = { ...w, now: "2026-01-01T00:00:02Z" };
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
  const hanging = new TextProcessingJobs(db, policy, {
    authorizeStart: (_j, _c, signal) => {
      observed = signal;
      return new Promise(() => {});
    },
    verifyOutcome: async () => true,
  });
  await assert.rejects(hanging.consume(w, "one", 2), {
    code: "verification_timeout",
  });
  assert.equal(observed?.aborted, true);
  const allowed = new TextProcessingJobs(db, policy, {
    authorizeStart: async () => true,
    verifyOutcome: async () => false,
  });
  await allowed.consume(w, "one", 2);
  await assert.rejects(allowed.complete(w, "one", 3, outcome, "bad"), {
    code: "verification_denied",
  });
  assert.equal((await jobs.read(w, "one")).job.status, "consumed");
});
