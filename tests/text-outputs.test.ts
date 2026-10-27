import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { PrivateObjectStore } from "@accountability/evidence/objects";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import { PrivateTextOutputs } from "../src/text-outputs.ts";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import { runLeasedTextJob } from "../src/text-worker.ts";
import { extractUtf8 } from "../src/index.ts";
import { hashBytes, digest } from "../src/contracts.ts";
import { setup, c, w, input } from "./text-job-fixture.ts";
const bytes = Buffer.from("hello");
const signal = () => new AbortController().signal;
async function fixture(t: test.TestContext) {
  const f = await setup(t);
  const root = await mkdtemp(join(tmpdir(), "processing-private-output-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const objects = new PrivateObjectStore(root, randomBytes(32), "fixture-v1");
  let allowed = true;
  const source = async () => ({
    source: input.source,
    sha256: hashBytes(bytes),
    bytes,
    scanStatus: "clean" as const,
    processingAllowed: true,
  });
  const options = {
    ...f.policy,
    verificationTimeoutMs: 1000,
    authorize: async () => allowed,
    readSource: source,
  };
  const outputs = new PrivateTextOutputs(
    f.db,
    objects,
    options,
    () => f.time.now,
  );
  const jobs = new TextProcessingJobs(
    f.db,
    { ...f.policy, verificationTimeoutMs: 2000 },
    {
      authorizeStart: async () => true,
      verifyOutcome: outputs.verifyOutcome.bind(outputs),
    },
    () => f.time.now,
  );
  await jobs.submit(
    c,
    {
      ...input,
      id: "actual",
      sourceSha256: hashBytes(bytes),
      sourceBytes: bytes.length,
    },
    "actual",
  );
  await jobs.lease(w, "actual", 1);
  const derivative = extractUtf8(c, await source());
  return {
    ...f,
    jobs,
    outputs,
    objects,
    options,
    source,
    derivative,
    root,
    deny: () => {
      allowed = false;
    },
  };
}
test("worker stores encrypted private text and exact immutable receipts verified from persisted bytes and fresh extraction", async (t) => {
  const f = await fixture(t);
  const result = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    {
      readSource: f.source,
      storePrivateText: f.outputs.storePrivateText.bind(f.outputs),
    },
    () => c.now,
  );
  assert.equal(result.status, "completed");
  if (result.status !== "completed") return;
  const out = result.snapshot.job.outcome!;
  assert.equal(out.derivative.kind, "processing-private-text");
  assert.equal(out.receipt.kind, "processing-output-receipt");
  assert.equal(out.derivative.revision, 1);
  assert.equal(
    await f.outputs.verifyOutcome(result.snapshot.job, out, w, signal()),
    true,
  );
  assert.deepEqual(
    await f.outputs.recoverOutcome(c, "actual", 3, signal()),
    out,
  );
  await scoped(f.db, c, async (tx) => {
    const art = await tx.get<{ object: { key: string }; metadata: unknown }>(
      out.derivative.kind,
      out.derivative.id,
    );
    assert.equal(art?.revision, 1);
    assert.equal(JSON.stringify(art!.value).includes("hello"), false);
    assert.equal(
      (await readFile(join(f.root, art!.value.object.key), "utf8")).includes(
        "hello",
      ),
      false,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
});
test("unconsumed, foreign and wrong-worker snapshots and forged output text cannot create a receipt", async (t) => {
  const f = await fixture(t);
  await assert.rejects(
    f.outputs.storePrivateText(
      await f.jobs.read(w, "actual"),
      f.derivative,
      w,
      signal(),
    ),
  );
  const consumed = await f.jobs.consume(w, "actual", 2);
  await assert.rejects(
    f.outputs.storePrivateText(
      consumed,
      f.derivative,
      { ...w, projectId: "foreign" },
      signal(),
    ),
  );
  await assert.rejects(
    f.outputs.storePrivateText(
      consumed,
      f.derivative,
      { ...w, actor: { ...w.actor, id: "other" } },
      signal(),
    ),
  );
  await assert.rejects(
    f.outputs.storePrivateText(
      consumed,
      {
        ...f.derivative,
        text: "forged",
        sha256: hashBytes(Buffer.from("forged")),
      },
      w,
      signal(),
    ),
  );
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-output-intent"), []);
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
  });
});
test("persisted object tampering and current authorization revocation fail verification without rewriting outcomes", async (t) => {
  const f = await fixture(t);
  const j = await f.jobs.consume(w, "actual", 2),
    out = await f.outputs.storePrivateText(j, f.derivative, w, signal());
  assert.equal(await f.outputs.verifyOutcome(j.job, out, w, signal()), true);
  f.deny();
  assert.equal(await f.outputs.verifyOutcome(j.job, out, w, signal()), false);
  const other = new PrivateTextOutputs(
    f.db,
    f.objects,
    { ...f.options, authorize: async () => true },
    () => c.now,
  );
  const object = await scoped(f.db, c, (tx) =>
    tx.get<{ object: { key: string } }>(out.derivative.kind, out.derivative.id),
  );
  const path = join(f.root, object!.value.object.key);
  await writeFile(path, "corrupted encrypted envelope");
  assert.equal(await other.verifyOutcome(j.job, out, w, signal()), false);
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});
test("repeated exact storage produces one output edition while conflicting metadata and receipt bindings fail closed", async (t) => {
  const f = await fixture(t);
  const j = await f.jobs.consume(w, "actual", 2),
    out = await f.outputs.storePrivateText(j, f.derivative, w, signal());
  assert.deepEqual(
    await f.outputs.storePrivateText(j, f.derivative, w, signal()),
    out,
  );
  await assert.rejects(
    f.outputs.storePrivateText(
      j,
      { ...f.derivative, createdAt: "2026-01-01T00:00:00.001Z" },
      w,
      signal(),
    ),
  );
  assert.equal(
    await f.outputs.verifyOutcome(
      j.job,
      { ...out, receipt: { ...out.receipt, revision: 2 } },
      w,
      signal(),
    ),
    false,
  );
  assert.equal(
    await f.outputs.verifyOutcome(
      j.job,
      { ...out, derivative: { ...out.derivative, projectId: "foreign" } },
      w,
      signal(),
    ),
    false,
  );
  await scoped(f.db, c, async (tx) => {
    assert.equal((await tx.list("processing-private-text")).length, 1);
    assert.equal((await tx.list("processing-output-receipt")).length, 1);
    assert.equal(
      (await tx.get(
        "processing-output-intent",
        digest({ jobId: "actual", consumedRevision: 3 }),
      ))!.revision,
      2,
    );
  });
});
