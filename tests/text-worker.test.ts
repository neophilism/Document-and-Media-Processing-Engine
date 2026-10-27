import test from "node:test";
import assert from "node:assert/strict";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import { runLeasedTextJob } from "../src/text-worker.ts";
import type { TextWorkerServices } from "../src/text-worker.ts";
import { hashBytes } from "../src/contracts.ts";
import { c, w, input, outcome, setup } from "./text-job-fixture.ts";
const bytes = new TextEncoder().encode("hello");
async function fixture(t: test.TestContext) {
  const f = await setup(t);
  await f.jobs.submit(
    c,
    {
      ...input,
      id: "actual",
      sourceSha256: hashBytes(bytes),
      sourceBytes: bytes.length,
    },
    "actual",
  );
  await f.jobs.lease(w, "actual", 1);
  let reads = 0,
    stores = 0;
  const services: TextWorkerServices = {
    readSource: async (job) => {
      reads++;
      assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
      assert.equal(job.revision, 3);
      return {
        source: input.source,
        sha256: hashBytes(bytes),
        bytes,
        scanStatus: "clean",
        processingAllowed: true,
      };
    },
    storePrivateText: async (job, d) => {
      stores++;
      assert.equal(job.revision, 3);
      assert.equal(d.text, "hello");
      return { ...outcome, sha256: d.sha256 };
    },
  };
  return { ...f, services, counts: () => ({ reads, stores }) };
}
test("runner consumes before source I/O and completes exact private text through actual restricted ledger", async (t) => {
  const f = await fixture(t);
  const result = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    f.services,
    () => c.now,
  );
  assert.equal(result.status, "completed");
  assert.equal(
    (await f.jobs.read(w, "actual")).job.outcome?.sha256,
    hashBytes(bytes),
  );
  assert.deepEqual(f.counts(), { reads: 1, stores: 1 });
  await runLeasedTextJob(f.jobs, w, "actual", 2, f.services, () => c.now);
  assert.deepEqual(f.counts(), { reads: 1, stores: 1 });
});
test("lost consumption acknowledgement never invokes source or store and remains uncertain", async (t) => {
  const f = await fixture(t);
  let calls = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const value = await f.db.transaction(fn);
      if (++calls === 2) throw Error("private lost ack");
      return value;
    },
    close: async () => {},
  };
  const jobs = new TextProcessingJobs(flaky, f.policy, f.checks, () => c.now);
  assert.deepEqual(
    await runLeasedTextJob(jobs, w, "actual", 2, f.services, () => c.now),
    {
      status: "reconciliation-required",
      phase: "consumption",
      consumedRevision: null,
    },
  );
  assert.deepEqual(f.counts(), { reads: 0, stores: 0 });
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});
test("wrong source hashes byte lengths references quarantine and nonboolean permission cannot reach storage", async (t) => {
  const f = await fixture(t);
  for (const [i, bad] of [
    { sha256: "b".repeat(64) },
    { bytes: new Uint8Array() },
    { source: { ...input.source, revision: 7 } },
    { scanStatus: "quarantined" },
    { processingAllowed: 1 },
  ].entries()) {
    const id = `bad-${i}`;
    await f.jobs.submit(
      c,
      { ...input, id, sourceSha256: hashBytes(bytes), sourceBytes: 5 },
      id,
    );
    await f.jobs.lease(w, id, 1);
    let stores = 0;
    const result = await runLeasedTextJob(
      f.jobs,
      w,
      id,
      2,
      {
        readSource: async () =>
          ({
            source: input.source,
            sha256: hashBytes(bytes),
            bytes,
            scanStatus: "clean",
            processingAllowed: true,
            ...bad,
          }) as never,
        storePrivateText: async () => {
          stores++;
          return outcome;
        },
      },
      () => c.now,
    );
    assert.equal(result.status, "reconciliation-required");
    assert.equal(stores, 0);
    assert.equal((await f.jobs.read(w, id)).job.status, "consumed");
  }
});
test("source timeout and late response cannot invoke storage or clear consumed uncertainty", async (t) => {
  const f = await fixture(t);
  let finish!: (v: unknown) => void, signal: AbortSignal | undefined;
  const result = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    {
      ...f.services,
      readSource: async (_j, _c, s) => {
        signal = s;
        return new Promise((resolve) => {
          finish = resolve as typeof finish;
        });
      },
    },
    () => c.now,
    { timeoutMs: 1000 },
  );
  assert.equal(result.status, "reconciliation-required");
  assert.equal(signal?.aborted, true);
  finish({
    source: input.source,
    sha256: hashBytes(bytes),
    bytes,
    scanStatus: "clean",
    processingAllowed: true,
  });
  await new Promise((r) => setImmediate(r));
  assert.equal(f.counts().stores, 0);
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});

test("storage timeout and late commit stay unresolved without auto-settlement or completion", async (t) => {
  const f = await fixture(t);
  let finish!: (v: typeof outcome) => void,
    stored = false;
  const result = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    {
      ...f.services,
      storePrivateText: async () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    () => c.now,
    { timeoutMs: 1000 },
  );
  assert.equal(result.status, "reconciliation-required");
  if (result.status === "reconciliation-required")
    assert.equal(result.phase, "storage");
  stored = true;
  finish({ ...outcome, sha256: hashBytes(bytes) });
  await new Promise((r) => setImmediate(r));
  assert.equal(stored, true);
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});
test("lost completion acknowledgement and delayed acknowledged completion preserve actual durable state", async (t) => {
  const f = await fixture(t);
  let n = 0;
  const flaky: Database = {
    transaction: async (fn) => {
      const v = await f.db.transaction(fn);
      if (++n === 4) throw Error("private completion diagnostics");
      return v;
    },
    close: async () => {},
  };
  const jobs = new TextProcessingJobs(flaky, f.policy, f.checks, () => c.now);
  const result = await runLeasedTextJob(
    jobs,
    w,
    "actual",
    2,
    f.services,
    () => c.now,
  );
  assert.deepEqual(result, {
    status: "reconciliation-required",
    phase: "completion",
    consumedRevision: 3,
  });
  assert.equal((await f.jobs.read(w, "actual")).job.status, "completed");
  await f.jobs.submit(
    c,
    { ...input, id: "delayed", sourceSha256: hashBytes(bytes), sourceBytes: 5 },
    "delayed",
  );
  await f.jobs.lease(w, "delayed", 1);
  let count = 0;
  const signal = new AbortController();
  const delayed: Database = {
    transaction: async (fn) => {
      const v = await f.db.transaction(fn);
      if (++count === 4) {
        signal.abort();
        await new Promise((r) => setTimeout(r, 10));
      }
      return v;
    },
    close: async () => {},
  };
  const result2 = await runLeasedTextJob(
    new TextProcessingJobs(delayed, f.policy, f.checks, () => c.now),
    w,
    "delayed",
    2,
    {
      readSource: async () => ({
        source: input.source,
        sha256: hashBytes(bytes),
        bytes,
        scanStatus: "clean",
        processingAllowed: true,
      }),
      storePrivateText: async () => ({ ...outcome, sha256: hashBytes(bytes) }),
    },
    () => c.now,
    { signal: signal.signal },
  );
  assert.equal(result2.status, "completed");
});
test("expired window, regressed live clock and external cancellation stop later stages", async (t) => {
  const f = await fixture(t);
  let time = c.now;
  const r = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    {
      ...f.services,
      readSource: async (...args) => {
        const v = await f.services.readSource(...args);
        time = "2026-01-01T00:02:00Z";
        return v;
      },
    },
    () => time,
  );
  assert.equal(r.status, "reconciliation-required");
  assert.equal(f.counts().stores, 0);
  const aborted = new AbortController();
  aborted.abort();
  await f.jobs.submit(
    c,
    {
      ...input,
      id: "not-started",
      sourceSha256: hashBytes(bytes),
      sourceBytes: 5,
    },
    "not-started",
  );
  await f.jobs.lease(w, "not-started", 1);
  await runLeasedTextJob(f.jobs, w, "not-started", 2, f.services, () => c.now, {
    signal: aborted.signal,
  });
  assert.equal((await f.jobs.read(w, "not-started")).job.status, "leased");
  await f.jobs.submit(
    c,
    {
      ...input,
      id: "regressed",
      sourceSha256: hashBytes(bytes),
      sourceBytes: 5,
    },
    "regressed",
  );
  await f.jobs.lease(w, "regressed", 1);
  await runLeasedTextJob(
    f.jobs,
    w,
    "regressed",
    2,
    f.services,
    () => "2025-12-31T23:59:59Z",
  );
  assert.equal((await f.jobs.read(w, "regressed")).job.status, "leased");
});
test("distinct jobs have distinct completion command identities and captured callback data cannot alter output binding", async (t) => {
  const f = await fixture(t);
  for (const id of ["second", "third"]) {
    await f.jobs.submit(
      c,
      { ...input, id, sourceSha256: hashBytes(bytes), sourceBytes: 5 },
      id,
    );
    await f.jobs.lease(w, id, 1);
    let captured = false;
    const result = await runLeasedTextJob(
      f.jobs,
      w,
      id,
      2,
      {
        readSource: async (job, ctx) => {
          (job.job.source as { id: string }).id = "mutated";
          (ctx.actor as { id: string }).id = "mutated";
          return {
            source: input.source,
            sha256: hashBytes(bytes),
            bytes,
            scanStatus: "clean",
            processingAllowed: true,
          };
        },
        storePrivateText: async (_j, d) => {
          captured = true;
          d.text = "changed by store";
          return { ...outcome, sha256: hashBytes(bytes) };
        },
      },
      () => c.now,
    );
    assert.equal(result.status, "completed");
    assert.equal(captured, true);
    assert.equal((await f.jobs.read(w, id)).job.source.id, input.source.id);
  }
});
