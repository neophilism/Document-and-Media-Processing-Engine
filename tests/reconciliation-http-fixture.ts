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
export async function fixture(t: test.TestContext, pending = true) {
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
  const jobPolicy = {
    ...f.policy,
    verificationTimeoutMs: 1000,
    managerIds: ["owner", "second"],
  };
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

export { c, w } from "./text-job-fixture.ts";
