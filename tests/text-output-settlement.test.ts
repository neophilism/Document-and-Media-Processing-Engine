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
