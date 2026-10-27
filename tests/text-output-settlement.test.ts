import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { PrivateObjectStore } from "@accountability/evidence/objects";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobFailure, TextJobSnapshot } from "../src/text-jobs.ts";
import { PrivateTextOutputs } from "../src/text-outputs.ts";
import { extractUtf8 } from "../src/index.ts";
import { digest, canonical, hashBytes } from "../src/contracts.ts";
import type { Json } from "../src/contracts.ts";
import { setup, c, w, input } from "./text-job-fixture.ts";
const proof: TextJobFailure = {
  receipt: {
    ...input.source,
    kind: "processing-receipt",
    id: "fixture-stopped",
    revision: 1,
  },
  reason: "processor-failed",
  processorStopped: true,
  noDerivative: true,
};
const signal = () => new AbortController().signal;
const json = (value: unknown): Json => JSON.parse(canonical(value));
async function fixture(
  t: test.TestContext,
  state: "pending" | "stored" | "none" = "pending",
) {
  const f = await setup(t),
    bytes = Buffer.from("hello");
  const root = await mkdtemp(join(tmpdir(), "text-output-settlement-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const objects = new PrivateObjectStore(root, randomBytes(32), "fixture-key");
  const policy = { ...f.policy, verificationTimeoutMs: 1000 };
  const options = {
    ...policy,
    authorize: async () => true,
    readSource: async () => ({
      source: input.source,
      sha256: hashBytes(bytes),
      bytes,
      scanStatus: "clean" as const,
      processingAllowed: true,
    }),
  };
  const outputs = new PrivateTextOutputs(
    f.db,
    objects,
    options,
    () => f.time.now,
  );
  // Deliberately permissive external proof tests the separate owned-record guard.
  // These callbacks are NOT qualified stopped/no-output proof services.
  const checks = {
    authorizeStart: async () => true,
    verifyOutcome: outputs.verifyOutcome.bind(outputs),
    verifyNoOutput: async () => true,
  };
  const jobs = new TextProcessingJobs(f.db, policy, checks, () => f.time.now);
  await jobs.submit(
    c,
    { ...input, id: "actual", sourceSha256: hashBytes(bytes) },
    "actual",
  );
  await jobs.lease(w, "actual", 1);
  const consumed = await jobs.consume(w, "actual", 2),
    derivative = extractUtf8(c, await options.readSource());
  if (state !== "none") {
    const db: Database = {
      transaction: (fn) =>
        f.db.transaction((q) =>
          fn({
            query: async <T>(sql: string, v?: unknown[]) => {
              const result = await q.query<T>(sql, v);
              if (
                state === "pending" &&
                sql.includes("INSERT INTO public.evidence_outbox") &&
                v?.includes("processing.output.stored")
              )
                throw Error(
                  "fixture rollback retains pre-write intent and private bytes",
                );
              return result;
            },
          }),
        ),
      close: async () => {},
    };
    const operation = new PrivateTextOutputs(
      db,
      objects,
      options,
      () => f.time.now,
    ).storePrivateText(consumed, derivative, w, signal());
    if (state === "pending") await assert.rejects(operation);
    else await operation;
  }
  return {
    ...f,
    jobs,
    outputs,
    options,
    objects,
    root,
    consumed,
    derivative,
    checks,
    policy,
  };
}
for (const state of ["pending", "stored"] as const) {
  test(`known ${state} private output prevents terminal no-output despite a permissive external proof`, async (t) => {
    const f = await fixture(t, state),
      before = await f.jobs.read(c, "actual");
    await assert.rejects(
      f.jobs.settleNoOutput(c, "actual", 3, proof, "no-output"),
      { code: "output_present" },
    );
    assert.deepEqual(await f.jobs.read(c, "actual"), before);
    const out = await f.outputs.finalizePending(c, "actual", 3, signal());
    assert.equal(
      (await f.jobs.complete(c, "actual", 3, out, "complete")).job.status,
      "completed",
    );
    await scoped(f.db, c, async (tx) => {
      assert.ok(verifyAudit(await tx.audits()));
      assert.equal(
        (await tx.audits()).filter(
          (a) => a.type === "processing.no-output.verified",
        ).length,
        0,
      );
    });
  });
}

test("an in-flight encrypted write remains uncertain as soon as its durable intent exists", async (t) => {
  const f = await fixture(t, "none");
  let entered!: () => void, release!: () => void;
  const ready = new Promise<void>((r) => {
      entered = r;
    }),
    wait = new Promise<void>((r) => {
      release = r;
    });
  const put = f.objects.put.bind(f.objects);
  f.objects.put = async (...args) => {
    entered();
    await wait;
    return put(...args);
  };
  const operation = f.outputs.storePrivateText(
    f.consumed,
    f.derivative,
    w,
    signal(),
  );
  await ready;
  try {
    await assert.rejects(
      f.jobs.settleNoOutput(c, "actual", 3, proof, "in-flight"),
      { code: "output_present" },
    );
    assert.equal((await f.jobs.read(c, "actual")).job.status, "consumed");
  } finally {
    release();
  }
  const out = await operation;
  assert.equal(
    await f.outputs.verifyOutcome(f.consumed.job, out, c, signal()),
    true,
  );
});

test("terminal settlement before intent admission fences a delayed writer before any object put", async (t) => {
  const f = await fixture(t, "none");
  let entered!: () => void,
    release!: () => void,
    writes = 0,
    first = true;
  const ready = new Promise<void>((r) => {
      entered = r;
    }),
    wait = new Promise<void>((r) => {
      release = r;
    });
  const outputs = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      authorize: async () => {
        if (first) {
          first = false;
          entered();
          await wait;
        }
        return true;
      },
    },
    () => f.time.now,
  );
  const put = f.objects.put.bind(f.objects);
  f.objects.put = async (...args) => {
    writes++;
    return put(...args);
  };
  const writing = outputs.storePrivateText(
    f.consumed,
    f.derivative,
    w,
    signal(),
  );
  // Observe rejection immediately so releasing the gate cannot produce an unhandled rejection.
  const rejected = assert.rejects(writing, { code: "output_unavailable" });
  await ready;
  try {
    const failed = await f.jobs.settleNoOutput(c, "actual", 3, proof, "first");
    assert.equal(failed.job.status, "failed");
  } finally {
    release();
  }
  await rejected;
  assert.equal(writes, 0);
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-output-intent"), []);
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.ok(verifyAudit(await tx.audits()));
  });
});

test("legacy failed and retry cached responses cannot bypass known-output guard; fingerprints remain exact", async (t) => {
  const f = await fixture(t),
    id = "actual",
    childId = `retry-${digest({ parentJobId: id, failedRevision: 4 })}`;
  // Explicit fixture-only SDK seeding of contradictory historical state. This is
  // not a new administrative API or a claim that the external proof was valid.
  const failed: TextJobSnapshot = {
    job: { ...f.consumed.job, status: "failed", failure: proof },
    revision: 4,
  };
  const child: TextJobSnapshot = {
    job: {
      ...f.consumed.job,
      id: childId,
      status: "queued",
      workerId: null,
      workerKind: null,
      leaseExpiresAt: null,
      consumedAt: null,
      submittedBy: c.actor.id,
      retry: {
        rootJobId: id,
        generation: 1,
        parentJobId: id,
        parentFailedRevision: 4,
        failureReceipt: proof.receipt,
      },
    },
    revision: 1,
  };
  await scoped(f.db, c, async (tx) => {
    await tx.idempotent(
      "legacy-failure",
      json({
        operation: "settle-text-no-output",
        id,
        consumedRevision: 3,
        failure: proof,
        purpose: c.purpose,
        actorKind: c.actor.kind,
      }),
      async () => {
        await tx.put(
          "text-processing-job",
          digest({ id }),
          3,
          json(failed.job),
        );
        await tx.audit("fixture.legacy.failure", { id });
        return json(failed);
      },
    );
    await tx.idempotent(
      "legacy-retry",
      json({
        operation: "retry-failed-text",
        id,
        failedRevision: 4,
        purpose: c.purpose,
        actorKind: c.actor.kind,
      }),
      async () => {
        await tx.put(
          "text-processing-job",
          digest({ id: childId }),
          0,
          json(child.job),
        );
        await tx.audit("fixture.legacy.retry", { id: childId });
        return json(child);
      },
    );
  });
  const before = await scoped(f.db, c, (tx) => tx.audits());
  for (const operation of [
    () => f.jobs.settleNoOutput(c, id, 3, proof, "legacy-failure"),
    () => f.jobs.retryFailed(c, id, 4, "legacy-retry"),
    () => f.jobs.retryFailed(c, id, 4, "new-retry"),
  ])
    await assert.rejects(operation(), { code: "output_present" });
  assert.deepEqual(await f.jobs.read(c, id), failed);
  assert.deepEqual(await f.jobs.read(c, childId), child);
  assert.deepEqual(await scoped(f.db, c, (tx) => tx.audits()), before);
});

for (const kind of [
  "processing-output-intent",
  "processing-private-text",
  "processing-output-receipt",
]) {
  test(`even an incomplete isolated ${kind} record conservatively denies no-output`, async (t) => {
    const f = await fixture(t, "none"),
      id = digest({ jobId: "actual", consumedRevision: 3 });
    await scoped(f.db, c, (tx) =>
      tx.put(kind, id, 0, { fixture: "incomplete-owned-record" }),
    );
    await assert.rejects(
      f.jobs.settleNoOutput(c, "actual", 3, proof, "incomplete"),
      { code: "output_present" },
    );
    assert.equal((await f.jobs.read(c, "actual")).job.status, "consumed");
  });
}
