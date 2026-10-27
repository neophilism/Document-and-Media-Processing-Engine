import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { PrivateObjectStore } from "@accountability/evidence/objects";
import { PrivateTextOutputs } from "../src/text-outputs.ts";
import { extractUtf8 } from "../src/index.ts";
import { hashBytes } from "../src/contracts.ts";
import {
  postgresDatabase,
  scoped,
  verifyAudit,
} from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobVerification } from "../src/text-jobs.ts";
import {
  c as base,
  w as worker,
  input as original,
  outcome as result,
} from "../tests/text-job-fixture.ts";

// Intentionally refuses arbitrary or persistent database targets. CI owns this empty fixture only.
const supplied = process.env.DMP_TEST_ADMIN_URL;
assert.ok(
  process.env.DMP_TEST_DISPOSABLE_POSTGRES === "1" && supplied,
  "Explicit disposable PostgreSQL fixture configuration required",
);
const url = new URL(supplied);
assert.ok(
  ["postgres:", "postgresql:"].includes(url.protocol) &&
    ["localhost", "127.0.0.1"].includes(url.hostname) &&
    url.pathname === "/processing_ci" &&
    url.username === "postgres" &&
    !url.search &&
    !url.hash,
  "Only the designated empty loopback fixture is accepted",
);
const admin = postgresDatabase(url.toString());
before(async () => {
  await admin.transaction(async (q) => {
    assert.equal(
      (
        await q.query<{ fresh: boolean }>(
          "SELECT to_regclass('public.evidence_project') IS NULL AS fresh",
        )
      ).rows[0]!.fresh,
      true,
      "Fixture must be empty; never reset a database here",
    );
    const migration = await readFile(
      new URL(
        "../migrations/001_foundation.sql",
        import.meta.resolve("@accountability/evidence/database"),
      ),
      "utf8",
    );
    assert.equal(migration.match(/^BEGIN;$/gm)?.length, 1);
    assert.equal(migration.match(/^COMMIT;$/gm)?.length, 1);
    // The SDK transaction owns BEGIN/COMMIT; every actual migration statement is unchanged.
    await q.query(migration.replace(/^BEGIN;$/m, "").replace(/^COMMIT;$/m, ""));
    await q.query(
      "CREATE ROLE processing_network_login LOGIN PASSWORD 'fixture-runtime-only' NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; GRANT evidence_runtime TO processing_network_login",
    );
  });
});
after(() => admin.close());
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
function barrier() {
  let arrivals = 0;
  const ready = gate();
  return async () => {
    if (++arrivals === 2) ready.resolve();
    await ready.promise;
    return true;
  };
}
async function fixture(
  t: test.TestContext,
  id: string,
  checks?: TextJobVerification,
) {
  const context = { ...base, projectId: id },
    w = { ...worker, projectId: id };
  const policy = {
    tenantId: context.tenantId,
    projectId: id,
    managerIds: ["owner"],
    workerIds: ["worker"],
    verificationTimeoutMs: 5000,
  };
  const clock = { now: context.now };
  const verification = checks ?? {
    authorizeStart: async () => true,
    verifyOutcome: async () => true,
    verifyNoOutput: async () => true,
  };
  function connect(label: string) {
    const runtime = new URL(url);
    runtime.username = "processing_network_login";
    runtime.password = "fixture-runtime-only";
    runtime.searchParams.set("application_name", id + "-" + label);
    const db = postgresDatabase(runtime.toString());
    t.after(() => db.close());
    return db;
  }
  const a = connect("a"),
    b = connect("b");
  const pids = await Promise.all(
    [a, b].map((db) =>
      db.transaction((q) =>
        q.query<{ pid: number; login: string }>(
          "SELECT pg_backend_pid() AS pid, session_user AS login",
        ),
      ),
    ),
  );
  assert.notEqual(pids[0]!.rows[0]!.pid, pids[1]!.rows[0]!.pid);
  assert.ok(pids.every((p) => p.rows[0]!.login === "processing_network_login"));
  const jobsA = new TextProcessingJobs(
      a,
      policy,
      verification,
      () => clock.now,
    ),
    jobsB = new TextProcessingJobs(b, policy, verification, () => clock.now);
  await jobsA.submit(
    context,
    { ...original, source: { ...original.source, projectId: id } },
    "submit",
  );
  await jobsA.lease(w, "one", 1, 1000);
  return {
    context,
    w,
    policy,
    clock,
    verification,
    a,
    b,
    jobsA,
    jobsB,
    connect,
    outcome: {
      ...result,
      derivative: { ...result.derivative, projectId: id },
      receipt: { ...result.receipt, projectId: id },
    },
  };
}

test(
  "two independently authenticated network sessions issue one consumption permit",
  { timeout: 20000 },
  async (t) => {
    const start = barrier();
    const f = await fixture(t, "network-consume", {
      authorizeStart: start,
      verifyOutcome: async () => true,
    });
    const attempts = await Promise.allSettled([
      f.jobsA.consume(f.w, "one", 2),
      f.jobsB.consume(f.w, "one", 2),
    ]);
    assert.equal(attempts.filter((r) => r.status === "fulfilled").length, 1);
    assert.equal(attempts.filter((r) => r.status === "rejected").length, 1);
    assert.equal((await f.jobsB.read(f.w, "one")).revision, 3);
    await scoped(f.b, f.context, async (tx) => {
      assert.ok(verifyAudit(await tx.audits()));
      assert.equal(
        (await tx.audits()).filter(
          (e) => e.type === "processing.consumption.checked",
        ).length,
        1,
      );
    });
  },
);

test(
  "live clock is sampled after an observed PostgreSQL lock wait on a separate backend",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "network-expiry");
    const locked = gate(),
      release = gate();
    const hold = scoped(f.a, f.context, async (tx) => {
      await tx.audit("fixture.lock-held", { fixture: true });
      locked.resolve();
      await release.promise;
    });
    let consuming: Promise<unknown> | undefined;
    try {
      await Promise.race([locked.promise, hold]);
      consuming = f.jobsB.consume(f.w, "one", 2);
      // Attach immediately so a failed admission cannot become an unhandled rejection.
      const observed = consuming.then(
        (value) => ({ value }),
        (error) => ({ error }),
      );
      const deadline = Date.now() + 5000;
      let blocked = false;
      while (Date.now() < deadline) {
        const state = await admin.transaction((q) =>
          q.query<{ blocked: boolean }>(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid))>0) AS blocked",
            ["network-expiry-b"],
          ),
        );
        if (state.rows[0]!.blocked) {
          blocked = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      assert.equal(
        blocked,
        true,
        "A real blocked backend must be observed, not inferred from elapsed time",
      );
      f.clock.now = "2026-01-01T00:00:02Z";
      release.resolve();
      await hold;
      const outcome = await observed;
      assert.ok("error" in outcome);
      assert.equal((outcome.error as { code: string }).code, "lease_denied");
      assert.equal((await f.jobsA.read(f.w, "one")).revision, 2);
    } finally {
      release.resolve();
      await hold;
      await consuming?.catch(() => {});
    }
  },
);

test(
  "independent sessions cannot commit both success and verified no-output for one consumed revision",
  { timeout: 20000 },
  async (t) => {
    const finish = barrier();
    const f = await fixture(t, "network-terminal", {
      authorizeStart: async () => true,
      verifyOutcome: finish,
      verifyNoOutput: finish,
    });
    await f.jobsA.consume(f.w, "one", 2);
    const proof = {
      receipt: { ...f.outcome.receipt, id: "stopped" },
      reason: "processor-failed" as const,
      processorStopped: true as const,
      noDerivative: true as const,
    };
    const attempts = await Promise.allSettled([
      f.jobsA.complete(f.w, "one", 3, f.outcome, "success"),
      f.jobsB.settleNoOutput(f.w, "one", 3, proof, "failure"),
    ]);
    assert.equal(attempts.filter((r) => r.status === "fulfilled").length, 1);
    assert.equal(attempts.filter((r) => r.status === "rejected").length, 1);
    const current = await f.jobsB.read(f.w, "one");
    assert.equal(current.revision, 4);
    assert.ok(["completed", "failed"].includes(current.job.status));
    await scoped(f.b, f.context, async (tx) => {
      assert.ok(verifyAudit(await tx.audits()));
      const topics = [];
      for (let i = 0; i < 4; i++) {
        const event = await tx.claim("inspector");
        if (!event) break;
        topics.push(event.topic);
        await tx.acknowledge(event);
      }
      assert.equal(
        topics.filter((x) =>
          ["processing.completed", "processing.failed"].includes(x),
        ).length,
        1,
      );
    });
  },
);

test(
  "consumed state survives a lost network-driver acknowledgement and fresh login/pool",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "network-reopen");
    let transactions = 0;
    const uncertain: Database = {
      transaction: async (fn) => {
        const value = await f.b.transaction(fn);
        if (++transactions === 2)
          throw Error("fixture lost acknowledgement after COMMIT");
        return value;
      },
      close: async () => {},
    };
    await assert.rejects(
      new TextProcessingJobs(
        uncertain,
        f.policy,
        f.verification,
        () => f.clock.now,
      ).consume(f.w, "one", 2),
      /fixture lost acknowledgement/,
    );
    const fresh = new TextProcessingJobs(
      f.connect("fresh"),
      f.policy,
      f.verification,
      () => f.clock.now,
    );
    assert.equal((await fresh.read(f.w, "one")).job.status, "consumed");
    await assert.rejects(fresh.consume(f.w, "one", 2));
    await assert.rejects(fresh.lease(f.w, "one", 3));
    assert.equal(
      (await fresh.complete(f.context, "one", 3, f.outcome, "recover")).job
        .status,
      "completed",
    );
  },
);

test(
  "network runtime login cannot bypass scope or acquire migration/schema authority",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "network-role-boundary");
    const owner = new TextProcessingJobs(
      admin,
      f.policy,
      f.verification,
      () => f.clock.now,
    );
    await assert.rejects(owner.read(f.context, "one"), {
      code: "unsafe_database_role",
    });
    const lowered: Database = {
      transaction: (fn) =>
        admin.transaction(async (q) => {
          await q.query("SET LOCAL ROLE evidence_runtime");
          return fn(q);
        }),
      close: async () => {},
    };
    await assert.rejects(
      new TextProcessingJobs(
        lowered,
        f.policy,
        f.verification,
        () => f.clock.now,
      ).read(f.context, "one"),
      { code: "unsafe_database_role" },
    );
    await assert.rejects(
      f.a.transaction((q) =>
        q.query("CREATE TABLE public.processing_forbidden_fixture(id integer)"),
      ),
      { code: "42501" },
    );
    await assert.rejects(
      f.a.transaction((q) => q.query("SET LOCAL ROLE postgres")),
      { code: "42501" },
    );
    await f.a.transaction(async (q) => {
      await q.query("SET LOCAL ROLE evidence_runtime");
      const count = await q.query<{ count: string }>(
        "SELECT count(*)::text AS count FROM public.evidence_record",
      );
      assert.equal(count.rows[0]!.count, "0");
    });
    await scoped(
      f.a,
      { ...f.context, projectId: "unrelated-project" },
      async (tx) => assert.deepEqual(await tx.list("text-processing-job"), []),
    );
    assert.equal((await f.jobsB.read(f.w, "one")).revision, 2);
  },
);

test(
  "independent network managers queue only one successor and replay never resets its consumed state",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "network-retry");
    await f.jobsA.consume(f.w, "one", 2);
    const proof = {
      receipt: { ...f.outcome.receipt, id: "stopped" },
      reason: "processor-failed" as const,
      processorStopped: true as const,
      noDerivative: true as const,
    };
    await f.jobsA.settleNoOutput(f.w, "one", 3, proof, "failed");
    const checked = barrier();
    const secondContext = {
      ...f.context,
      actor: { ...f.context.actor, id: "second-owner" },
    };
    const policy = { ...f.policy, managerIds: ["owner", "second-owner"] };
    const checks = { ...f.verification, verifyNoOutput: checked };
    const left = new TextProcessingJobs(f.a, policy, checks, () => f.clock.now);
    const right = new TextProcessingJobs(
      f.b,
      policy,
      checks,
      () => f.clock.now,
    );
    const commands = ["retry-left", "retry-right"];
    const contexts = [f.context, secondContext];
    const attempts = await Promise.allSettled([
      left.retryFailed(contexts[0]!, "one", 4, commands[0]!),
      right.retryFailed(contexts[1]!, "one", 4, commands[1]!),
    ]);
    const winner = attempts.findIndex((r) => r.status === "fulfilled");
    assert.ok(winner >= 0);
    assert.equal(attempts.filter((r) => r.status === "rejected").length, 1);
    const result = attempts[winner]!;
    assert.ok(result.status === "fulfilled");
    const child = result.value;
    assert.equal(child.job.retry?.parentFailedRevision, 4);
    await f.jobsA.lease(f.w, child.job.id, 1);
    await f.jobsA.consume(f.w, child.job.id, 2);
    // A fresh pool and ordinary fixture verifier revalidate the failed proof.
    const fresh = new TextProcessingJobs(
      f.connect("retry-fresh"),
      policy,
      f.verification,
      () => f.clock.now,
    );
    assert.deepEqual(
      await fresh.retryFailed(contexts[winner]!, "one", 4, commands[winner]!),
      child,
    );
    assert.equal((await fresh.read(f.w, child.job.id)).job.status, "consumed");
    await assert.rejects(
      fresh.retryFailed(f.context, "one", 4, "another-command"),
    );
    assert.equal((await fresh.read(f.context, "one")).revision, 4);
    await scoped(f.b, f.context, async (tx) => {
      assert.equal((await tx.list("text-processing-job")).length, 2);
      assert.equal(
        (await tx.audits()).filter((e) => e.type === "processing.retry.queued")
          .length,
        1,
      );
      assert.ok(verifyAudit(await tx.audits()));
    });
  },
);

test(
  "independent PostgreSQL sessions finalize one pending encrypted output after lease expiry without duplicate publication",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "network-output-finalize");
    const bytes = Buffer.from("hello");
    const source = {
      source: { ...original.source, projectId: f.context.projectId },
      sha256: hashBytes(bytes),
      bytes,
      scanStatus: "clean" as const,
      processingAllowed: true,
    };
    await f.jobsA.submit(
      f.context,
      {
        ...original,
        id: "actual",
        source: source.source,
        sourceSha256: source.sha256,
      },
      "actual",
    );
    await f.jobsA.lease(f.w, "actual", 1);
    const consumed = await f.jobsA.consume(f.w, "actual", 2);
    const root = await mkdtemp(join(tmpdir(), "network-processing-output-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const objects = new PrivateObjectStore(
      root,
      randomBytes(32),
      "network-fixture-key",
    );
    const policy = {
      ...f.policy,
      managerIds: ["owner", "second"],
      readSource: async () => source,
      authorize: async () => true,
    };
    let injected = false;
    const uncertainDb: Database = {
      transaction: (fn) =>
        f.a.transaction((q) =>
          fn({
            query: async <T>(sql: string, values?: unknown[]) => {
              const result = await q.query<T>(sql, values);
              if (
                sql.includes("INSERT INTO public.evidence_outbox") &&
                values?.includes("processing.output.stored")
              ) {
                injected = true;
                throw Error("fixture rollback retains pending private output");
              }
              return result;
            },
          }),
        ),
      close: async () => {},
    };
    await assert.rejects(
      new PrivateTextOutputs(
        uncertainDb,
        objects,
        policy,
        () => f.clock.now,
      ).storePrivateText(
        consumed,
        extractUtf8(f.context, source),
        f.w,
        new AbortController().signal,
      ),
    );
    assert.equal(injected, true);
    objects.put = async () => {
      throw Error("finalization cannot rewrite encrypted bytes");
    };
    f.clock.now = "2026-01-01T00:02:00Z";
    const arrive = barrier();
    let admissions = 0;
    const options = {
      ...policy,
      authorize: async (_job: unknown, ctx: typeof f.context) => {
        assert.equal(ctx.now, f.clock.now);
        if (++admissions <= 2) return arrive();
        return true;
      },
    };
    const a = new PrivateTextOutputs(f.a, objects, options, () => f.clock.now),
      b = new PrivateTextOutputs(f.b, objects, options, () => f.clock.now);
    const outcomes = await Promise.all([
      a.finalizePending(f.context, "actual", 3, new AbortController().signal),
      b.finalizePending(
        { ...f.context, actor: { ...f.context.actor, id: "second" } },
        "actual",
        3,
        new AbortController().signal,
      ),
    ]);
    assert.deepEqual(outcomes[0], outcomes[1]);
    const outcome = outcomes[0]!;
    assert.equal(
      (await f.jobsA.read(f.context, "actual")).job.status,
      "consumed",
    );
    await scoped(f.b, f.context, async (tx) => {
      assert.equal((await tx.list("processing-private-text")).length, 1);
      assert.equal((await tx.list("processing-output-receipt")).length, 1);
      assert.equal(
        (await tx.get("processing-output-intent", outcome.receipt.id))
          ?.revision,
        2,
      );
      assert.ok(verifyAudit(await tx.audits()));
    });
    const outbox = await admin.transaction((q) =>
      q.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM public.evidence_outbox WHERE tenant_id=$1 AND project_id=$2 AND topic=$3",
        [f.context.tenantId, f.context.projectId, "processing.output.stored"],
      ),
    );
    assert.equal(outbox.rows[0]!.count, 1);
    const verifiedJobs = new TextProcessingJobs(
      f.b,
      f.policy,
      {
        authorizeStart: async () => true,
        verifyOutcome: b.verifyOutcome.bind(b),
      },
      () => f.clock.now,
    );
    assert.equal(
      (
        await verifiedJobs.complete(
          f.context,
          "actual",
          3,
          outcome,
          "network-finalized",
        )
      ).job.status,
      "completed",
    );
    assert.deepEqual(
      await a.finalizePending(
        f.context,
        "actual",
        3,
        new AbortController().signal,
      ),
      outcome,
    );
  },
);
