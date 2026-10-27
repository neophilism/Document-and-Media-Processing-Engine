import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { PrivateObjectStore } from "@accountability/evidence/objects";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import { PrivateTextOutputs } from "../src/text-outputs.ts";
import { reconcileTextOutput } from "../src/text-reconciliation.ts";
import { extractUtf8 } from "../src/index.ts";
import { digest, hashBytes } from "../src/contracts.ts";
import { setup, c, w, input } from "./text-job-fixture.ts";
const request = {
  jobId: "actual",
  consumedRevision: 3,
  output: "finalize-existing" as const,
};
const freshSignal = () => new AbortController().signal;
function fault(db: Database, topic: string, afterCommit: boolean) {
  let injected = false;
  const wrapped: Database = {
    transaction: async (fn) => {
      let observed = false;
      const result = await db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              values?.includes(topic)
            ) {
              observed = true;
              if (!afterCommit && !injected) {
                injected = true;
                throw Error("private rollback diagnostic");
              }
            }
            return result;
          },
        }),
      );
      if (observed && afterCommit && !injected) {
        injected = true;
        throw Error("private lost acknowledgement");
      }
      return result;
    },
    close: async () => {},
  };
  return { db: wrapped, injected: () => injected };
}
async function fixture(t: test.TestContext, pending = true) {
  const f = await setup(t);
  const root = await mkdtemp(join(tmpdir(), "text-reconciliation-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const objects = new PrivateObjectStore(root, randomBytes(32), "fixture-key"),
    bytes = Buffer.from("hello");
  let allowed = true,
    reads = 0;
  const source = async () => {
    reads++;
    return {
      source: input.source,
      sha256: hashBytes(bytes),
      bytes,
      scanStatus: "clean" as const,
      processingAllowed: true,
    };
  };
  const jobPolicy = { ...f.policy, verificationTimeoutMs: 1000, managerIds: ["owner", "second"] };
  const options = {
    ...jobPolicy,
    readSource: source,
    authorize: async () => allowed,
  };
  const outputs = new PrivateTextOutputs(
    f.db,
    objects,
    options,
    () => f.time.now,
  );
  const jobs = new TextProcessingJobs(
    f.db,
    jobPolicy,
    {
      authorizeStart: async () => true,
      verifyOutcome: outputs.verifyOutcome.bind(outputs),
    },
    () => f.time.now,
  );
  await jobs.submit(
    c,
    { ...input, id: "actual", sourceSha256: hashBytes(bytes) },
    "actual",
  );
  await jobs.lease(w, "actual", 1);
  const consumed = await jobs.consume(w, "actual", 2),
    derivative = extractUtf8(c, await source());
  if (pending) {
    const rolled = fault(f.db, "processing.output.stored", false);
    await assert.rejects(
      new PrivateTextOutputs(
        rolled.db,
        objects,
        options,
        () => f.time.now,
      ).storePrivateText(consumed, derivative, w, freshSignal()),
    );
    assert.equal(rolled.injected(), true);
  } else await outputs.storePrivateText(consumed, derivative, w, freshSignal());
  const objectPath = join(
    root,
    digest({ tenantId: c.tenantId, projectId: c.projectId }),
    derivative.sha256,
  );
  const ciphertext = await readFile(objectPath);
  objects.put = async () => {
    throw Error("reconciliation cannot write bytes");
  };
  return {
    ...f,
    jobs,
    outputs,
    options,
    jobPolicy,
    objects,
    objectPath,
    ciphertext,
    source,
    consumed,
    reads: () => reads,
    deny: () => {
      allowed = false;
    },
  };
}

test("manager finalizes existing private bytes and verifies completion after execution lease expiry", async (t) => {
  const f = await fixture(t);
  f.time.now = "2026-01-01T00:02:00Z";
  const result = await reconcileTextOutput(
    f.jobs,
    f.outputs,
    c,
    request,
    () => f.time.now,
  );
  assert.equal(result.status, "completed");
  if (result.status !== "completed") return;
  assert.equal(result.revision, 4);
  assert.equal(result.outcome.receipt.kind, "processing-output-receipt");
  assert.deepEqual(await readFile(f.objectPath), f.ciphertext);
  assert.equal((await f.jobs.read(c, "actual")).job.status, "completed");
  await scoped(f.db, { ...c, now: f.time.now }, async (tx) => {
    assert.equal((await tx.list("processing-output-receipt")).length, 1);
    assert.equal((await tx.list("processing-private-text")).length, 1);
    assert.ok(verifyAudit(await tx.audits()));
    const topics: string[] = [];
    for (;;) {
      const event = await tx.claim("reviewer");
      if (!event) break;
      topics.push(event.topic);
      await tx.acknowledge(event);
    }
    assert.equal(topics.filter((s) => s === "processing.completed").length, 1);
    assert.equal(
      topics.filter((s) => s === "processing.output.stored").length,
      1,
    );
  });
});

test("recover-stored never silently finalizes pending work; explicit mode and completed replay stay verified", async (t) => {
  const f = await fixture(t);
  const before = await f.jobs.read(c, "actual");
  assert.deepEqual(
    await reconcileTextOutput(
      f.jobs,
      f.outputs,
      c,
      { ...request, output: "recover-stored" },
      () => f.time.now,
    ),
    { status: "reconciliation-required", phase: "output" },
  );
  assert.deepEqual(await f.jobs.read(c, "actual"), before);
  await scoped(f.db, c, async (tx) =>
    assert.deepEqual(await tx.list("processing-output-receipt"), []),
  );
  const done = await reconcileTextOutput(
    f.jobs,
    f.outputs,
    c,
    request,
    () => f.time.now,
  );
  const replay = await reconcileTextOutput(
    f.jobs,
    f.outputs,
    c,
    { ...request, output: "recover-stored" },
    () => f.time.now,
  );
  assert.equal(done.status, "completed");
  assert.deepEqual(replay, done);
  f.deny();
  assert.deepEqual(
    await reconcileTextOutput(f.jobs, f.outputs, c, request, () => f.time.now),
    { status: "reconciliation-required", phase: "output" },
  );
});

test("wrong authority scope purpose and stale edition cannot reach source or completion", async (t) => {
  const f = await fixture(t);
  const before = f.reads();
  for (const context of [
    { ...c, projectId: "foreign" },
    { ...c, purpose: "other" },
    { ...c, actor: { ...c.actor, id: "unlisted" } },
  ])
    assert.deepEqual(
      await reconcileTextOutput(
        f.jobs,
        f.outputs,
        context,
        request,
        () => f.time.now,
      ),
      { status: "reconciliation-required", phase: "inspection" },
    );
  for (const context of [w, { ...c, actor: { ...c.actor, roles: [] } }])
    await assert.rejects(
      reconcileTextOutput(
        f.jobs,
        f.outputs,
        context,
        request,
        () => f.time.now,
      ),
    );
  assert.deepEqual(
    await reconcileTextOutput(
      f.jobs,
      f.outputs,
      c,
      { ...request, consumedRevision: 4 },
      () => f.time.now,
    ),
    { status: "reconciliation-required", phase: "inspection" },
  );
  assert.equal(f.reads(), before);
  for (const supplied of [
    { ...request, extra: true },
    { ...request, output: "execute" },
    { ...request, consumedRevision: 2 },
    { ...request, consumedRevision: 3.5 },
  ])
    await assert.rejects(
      reconcileTextOutput(
        f.jobs,
        f.outputs,
        c,
        supplied as typeof request,
        () => f.time.now,
      ),
    );
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
});

test("lost output-publication acknowledgement leaves consumed work and needs a separate explicit recovery", async (t) => {
  const f = await fixture(t),
    lost = fault(f.db, "processing.output.stored", true);
  const outputs = new PrivateTextOutputs(
    lost.db,
    f.objects,
    f.options,
    () => f.time.now,
  );
  assert.deepEqual(
    await reconcileTextOutput(f.jobs, outputs, c, request, () => f.time.now),
    { status: "reconciliation-required", phase: "output" },
  );
  assert.equal(lost.injected(), true);
  assert.equal((await f.jobs.read(c, "actual")).job.status, "consumed");
  const recovered = await reconcileTextOutput(
    f.jobs,
    f.outputs,
    c,
    { ...request, output: "recover-stored" },
    () => f.time.now,
  );
  assert.equal(recovered.status, "completed");
  assert.deepEqual(await readFile(f.objectPath), f.ciphertext);
});
