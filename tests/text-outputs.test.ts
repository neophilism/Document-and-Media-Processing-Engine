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

test("lost committed output acknowledgement recovers exact outcome without a second artifact or execution", async (t) => {
  const f = await fixture(t),
    j = await f.jobs.consume(w, "actual", 2);
  let lost = false;
  const db: typeof f.db = {
    transaction: async (fn) => {
      let publishing = false;
      const result = await f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const r = await q.query<T>(sql, v);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              v?.includes("processing.output.stored")
            )
              publishing = true;
            return r;
          },
        }),
      );
      if (publishing && !lost) {
        lost = true;
        throw Error("lost output ack with private diagnostics");
      }
      return result;
    },
    close: async () => {},
  };
  const uncertain = new PrivateTextOutputs(
    db,
    f.objects,
    f.options,
    () => c.now,
  );
  await assert.rejects(
    uncertain.storePrivateText(j, f.derivative, w, signal()),
    (e) => {
      assert.equal(String(e).includes("private diagnostics"), false);
      return true;
    },
  );
  assert.equal(lost, true);
  f.time.now = "2026-01-01T00:02:00Z";
  const fresh = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      authorize: async (_job, ctx) => {
        assert.equal(ctx.now, f.time.now);
        return true;
      },
      readSource: async (_job, ctx) => {
        assert.equal(ctx.now, f.time.now);
        return f.source();
      },
    },
    () => f.time.now,
  );
  const out = await fresh.recoverOutcome(c, "actual", 3, signal());
  assert.equal(await f.outputs.verifyOutcome(j.job, out, c, signal()), true);
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
  await scoped(f.db, c, async (tx) =>
    assert.equal((await tx.list("processing-output-receipt")).length, 1),
  );
});
test("post-outbox rollback leaves durable pending intent and encrypted object but never a usable receipt", async (t) => {
  const f = await fixture(t),
    j = await f.jobs.consume(w, "actual", 2);
  let injected = false;
  const db: typeof f.db = {
    transaction: (fn) =>
      f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, v?: unknown[]) => {
            const r = await q.query<T>(sql, v);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              v?.includes("processing.output.stored")
            ) {
              injected = true;
              throw Error("rollback output publication");
            }
            return r;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new PrivateTextOutputs(
      db,
      f.objects,
      f.options,
      () => c.now,
    ).storePrivateText(j, f.derivative, w, signal()),
  );
  assert.equal(injected, true);
  await assert.rejects(f.outputs.recoverOutcome(c, "actual", 3, signal()));
  await scoped(f.db, c, async (tx) => {
    assert.equal(
      (await tx.list<{ status: string }>("processing-output-intent"))[0]!.value
        .status,
      "pending",
    );
    assert.deepEqual(await tx.list("processing-private-text"), []);
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.ok(verifyAudit(await tx.audits()));
  });
  const key = `${digest({ tenantId: c.tenantId, projectId: c.projectId })}/${hashBytes(bytes)}`;
  assert.deepEqual(
    Buffer.from(
      await f.objects.get(c, {
        key,
        plaintextHash: hashBytes(bytes),
        size: 5,
        keyVersion: "fixture-v1",
      }),
    ),
    bytes,
  );
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});
test("bounded policy/source verification, cancellation and expired execution cannot create output records", async (t) => {
  const f = await fixture(t),
    j = await f.jobs.consume(w, "actual", 2);
  let finish!: (v: boolean) => void, seen: AbortSignal | undefined;
  const hanging = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      verificationTimeoutMs: 10,
      authorize: async (_j, _c, s) => {
        seen = s;
        return new Promise((r) => {
          finish = r;
        });
      },
    },
    () => c.now,
  );
  await assert.rejects(hanging.storePrivateText(j, f.derivative, w, signal()));
  assert.equal(seen?.aborted, true);
  finish(true);
  await new Promise((r) => setImmediate(r));
  const source = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      verificationTimeoutMs: 10,
      readSource: async () => new Promise(() => {}),
    },
    () => c.now,
  );
  await assert.rejects(source.storePrivateText(j, f.derivative, w, signal()));
  const aborted = new AbortController();
  aborted.abort();
  await assert.rejects(
    f.outputs.storePrivateText(j, f.derivative, w, aborted.signal),
  );
  f.time.now = "2026-01-01T00:02:00Z";
  await assert.rejects(
    f.outputs.storePrivateText(j, f.derivative, w, signal()),
  );
  await scoped(f.db, c, async (tx) =>
    assert.deepEqual(await tx.list("processing-output-intent"), []),
  );
});
test("captured derivative is stable across policy waits and current source revalidation can deny an existing receipt", async (t) => {
  const f = await fixture(t),
    j = await f.jobs.consume(w, "actual", 2);
  let entered!: () => void,
    finish!: (v: boolean) => void,
    calls = 0;
  const ready = new Promise<void>((r) => {
    entered = r;
  });
  const store = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      authorize: async () => {
        if (++calls === 1) {
          entered();
          return new Promise((r) => {
            finish = r;
          });
        }
        return true;
      },
    },
    () => c.now,
  );
  const d = structuredClone(f.derivative);
  const pending = store.storePrivateText(j, d, w, signal());
  await ready;
  d.text = "mutated";
  d.sha256 = hashBytes(Buffer.from("mutated"));
  finish(true);
  const out = await pending;
  assert.equal(out.sha256, hashBytes(bytes));
  const denied = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      readSource: async () => ({
        ...(await f.source()),
        processingAllowed: false,
      }),
    },
    () => c.now,
  );
  assert.equal(await denied.verifyOutcome(j.job, out, w, signal()), false);
  await assert.rejects(
    f.outputs.recoverOutcome(
      { ...c, purpose: "foreign" },
      "actual",
      3,
      signal(),
    ),
  );
});

async function pendingOutput(t: test.TestContext) {
  const f = await fixture(t);
  const consumed = await f.jobs.consume(w, "actual", 2);
  const db: typeof f.db = {
    transaction: (fn) =>
      f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              values?.includes("processing.output.stored")
            )
              throw Error("test-only rollback after output outbox");
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new PrivateTextOutputs(
      db,
      f.objects,
      f.options,
      () => f.time.now,
    ).storePrivateText(consumed, f.derivative, w, signal()),
  );
  return { ...f, consumed };
}

test("manager finalizes existing pending bytes after lease expiry without rewriting bytes or repeating processing", async (t) => {
  const f = await pendingOutput(t);
  const key = `${digest({ tenantId: c.tenantId, projectId: c.projectId })}/${hashBytes(bytes)}`;
  const before = await readFile(join(f.root, key));
  f.objects.put = async () => {
    throw Error("finalization must not write objects");
  };
  f.time.now = "2026-01-01T00:02:00Z";
  const out = await f.outputs.finalizePending(c, "actual", 3, signal());
  assert.deepEqual(await readFile(join(f.root, key)), before);
  assert.equal((await f.jobs.read(c, "actual")).job.status, "consumed");
  assert.equal(
    await f.outputs.verifyOutcome(f.consumed.job, out, c, signal()),
    true,
  );
  const done = await f.jobs.complete(
    c,
    "actual",
    3,
    out,
    "finalized-completion",
  );
  assert.equal(done.job.status, "completed");
  assert.deepEqual(
    await f.outputs.finalizePending(c, "actual", 3, signal()),
    out,
  );
  await scoped(f.db, c, async (tx) => {
    assert.equal((await tx.list("processing-private-text")).length, 1);
    assert.equal((await tx.list("processing-output-receipt")).length, 1);
    const intent = await tx.get<{ status: string }>(
      "processing-output-intent",
      out.receipt.id,
    );
    assert.equal(intent?.revision, 2);
    assert.equal(intent?.value.status, "stored");
    assert.ok(verifyAudit(await tx.audits()));
  });
});

test("pending finalization requires current listed manager, exact scope/purpose and consumed edition", async (t) => {
  const f = await pendingOutput(t);
  for (const ctx of [
    w,
    { ...c, actor: { ...c.actor, id: "unlisted" } },
    { ...c, actor: { ...c.actor, roles: [] } },
    { ...c, projectId: "foreign" },
    { ...c, purpose: "other" },
  ]) {
    await assert.rejects(
      f.outputs.finalizePending(ctx, "actual", 3, signal()),
      { code: "output_unavailable" },
    );
  }
  await assert.rejects(f.outputs.finalizePending(c, "actual", 2, signal()));
  await assert.rejects(f.outputs.finalizePending(c, "actual", 4, signal()));
  f.deny();
  await assert.rejects(f.outputs.finalizePending(c, "actual", 3, signal()));
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.equal(
      (
        await tx.get(
          "processing-output-intent",
          digest({ jobId: "actual", consumedRevision: 3 }),
        )
      )?.revision,
      1,
    );
  });
});

test("finalization denies absent intent, missing/corrupted ciphertext and unavailable key without creating or deleting bytes", async (t) => {
  const f = await pendingOutput(t);
  const key = `${digest({ tenantId: c.tenantId, projectId: c.projectId })}/${hashBytes(bytes)}`;
  const path = join(f.root, key),
    original = await readFile(path);
  await f.jobs.submit(
    c,
    {
      ...input,
      id: "no-intent",
      sourceSha256: hashBytes(bytes),
      sourceBytes: bytes.length,
    },
    "no-intent",
  );
  await f.jobs.lease(w, "no-intent", 1);
  await f.jobs.consume(w, "no-intent", 2);
  await assert.rejects(f.outputs.finalizePending(c, "no-intent", 3, signal()));
  await rm(path);
  await assert.rejects(f.outputs.finalizePending(c, "actual", 3, signal()));
  await assert.rejects(readFile(path));
  await writeFile(path, "corrupt private object");
  await assert.rejects(f.outputs.finalizePending(c, "actual", 3, signal()));
  assert.equal(await readFile(path, "utf8"), "corrupt private object");
  await writeFile(path, original);
  const rotated = new PrivateTextOutputs(
    f.db,
    new PrivateObjectStore(f.root, randomBytes(32), "different-key"),
    f.options,
    () => f.time.now,
  );
  await assert.rejects(rotated.finalizePending(c, "actual", 3, signal()));
  assert.deepEqual(await readFile(path), original);
  await scoped(f.db, c, async (tx) =>
    assert.deepEqual(await tx.list("processing-output-receipt"), []),
  );
  assert.equal((await f.jobs.read(c, "actual")).job.status, "consumed");
});

test("current source denial, late verification, cancellation and regressed clock leave pending work unresolved", async (t) => {
  const f = await pendingOutput(t);
  const denied = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      readSource: async () => ({
        ...(await f.source()),
        processingAllowed: false,
      }),
    },
    () => f.time.now,
  );
  await assert.rejects(denied.finalizePending(c, "actual", 3, signal()));
  let finish!: (input: Awaited<ReturnType<typeof f.source>>) => void;
  let seen: AbortSignal | undefined;
  const hung = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      verificationTimeoutMs: 10,
      readSource: async (_j, ctx, s) => {
        assert.equal(ctx.now, f.time.now);
        seen = s;
        return new Promise((r) => {
          finish = r;
        });
      },
    },
    () => f.time.now,
  );
  f.time.now = "2026-01-01T00:02:00Z";
  await assert.rejects(hung.finalizePending(c, "actual", 3, signal()));
  assert.equal(seen?.aborted, true);
  finish(await f.source());
  await new Promise((r) => setImmediate(r));
  const stopped = new AbortController();
  stopped.abort();
  await assert.rejects(
    f.outputs.finalizePending(c, "actual", 3, stopped.signal),
  );
  const regressed = new PrivateTextOutputs(
    f.db,
    f.objects,
    f.options,
    () => "2025-12-31T23:59:59Z",
  );
  await assert.rejects(regressed.finalizePending(c, "actual", 3, signal()));
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.equal(
      (
        await tx.get(
          "processing-output-intent",
          digest({ jobId: "actual", consumedRevision: 3 }),
        )
      )?.revision,
      1,
    );
  });
});

test("lost finalization acknowledgement replays the committed outcome after fresh current authority", async (t) => {
  const f = await pendingOutput(t);
  let lost = false;
  const db: typeof f.db = {
    transaction: async (fn) => {
      let publishing = false;
      const result = await f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              values?.includes("processing.output.stored")
            )
              publishing = true;
            return result;
          },
        }),
      );
      if (publishing && !lost) {
        lost = true;
        throw Error("lost finalization ack: secret fixture diagnostic");
      }
      return result;
    },
    close: async () => {},
  };
  const uncertain = new PrivateTextOutputs(
    db,
    f.objects,
    f.options,
    () => f.time.now,
  );
  await assert.rejects(
    uncertain.finalizePending(c, "actual", 3, signal()),
    (e) => {
      assert.equal(String(e).includes("secret fixture"), false);
      return true;
    },
  );
  assert.equal(lost, true);
  const out = await f.outputs.finalizePending(c, "actual", 3, signal());
  assert.deepEqual(
    await uncertain.finalizePending(c, "actual", 3, signal()),
    out,
  );
  f.deny();
  await assert.rejects(f.outputs.finalizePending(c, "actual", 3, signal()));
  await scoped(f.db, c, async (tx) => {
    assert.equal((await tx.list("processing-output-receipt")).length, 1);
    assert.equal(
      (await tx.get("processing-output-intent", out.receipt.id))?.revision,
      2,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
});

test("finalization rollback after outbox keeps only the pending intent and existing encrypted bytes", async (t) => {
  const f = await pendingOutput(t);
  let injected = false;
  const db: typeof f.db = {
    transaction: (fn) =>
      f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (
              sql.includes("INSERT INTO public.evidence_outbox") &&
              values?.includes("processing.output.stored")
            ) {
              injected = true;
              throw Error("rollback finalization after outbox");
            }
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  await assert.rejects(
    new PrivateTextOutputs(
      db,
      f.objects,
      f.options,
      () => f.time.now,
    ).finalizePending(c, "actual", 3, signal()),
  );
  assert.equal(injected, true);
  await assert.rejects(f.outputs.recoverOutcome(c, "actual", 3, signal()));
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-private-text"), []);
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.equal(
      (
        await tx.get(
          "processing-output-intent",
          digest({ jobId: "actual", consumedRevision: 3 }),
        )
      )?.revision,
      1,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
  // The same existing bytes can still be finalized after the transaction failure.
  assert.ok(await f.outputs.finalizePending(c, "actual", 3, signal()));
});

test("competing managers and an in-flight normal writer converge on one exact publication", async (t) => {
  const f = await pendingOutput(t);
  let count = 0,
    release!: () => void;
  const all = new Promise<void>((r) => {
    release = r;
  });
  const policy = {
    ...f.options,
    managerIds: ["owner", "second"],
    authorize: async () => {
      if (++count === 3) release();
      if (count <= 3) await all;
      return true;
    },
  };
  const outputs = new PrivateTextOutputs(
    f.db,
    f.objects,
    policy,
    () => f.time.now,
  );
  const values = await Promise.all([
    outputs.finalizePending(c, "actual", 3, signal()),
    outputs.finalizePending(
      { ...c, actor: { ...c.actor, id: "second" } },
      "actual",
      3,
      signal(),
    ),
    outputs.storePrivateText(f.consumed, f.derivative, w, signal()),
  ]);
  assert.deepEqual(values[0], values[1]);
  assert.deepEqual(values[0], values[2]);
  await scoped(f.db, c, async (tx) => {
    assert.equal((await tx.list("processing-private-text")).length, 1);
    assert.equal((await tx.list("processing-output-receipt")).length, 1);
    assert.equal(
      (await tx.get("processing-output-intent", values[0]!.receipt.id))
        ?.revision,
      2,
    );
    assert.ok(verifyAudit(await tx.audits()));
  });
});

test("completion during finalization verification fences stale publication and permits only fresh verified replay", async (t) => {
  const f = await pendingOutput(t);
  let entered!: () => void, release!: () => void;
  const ready = new Promise<void>((r) => {
      entered = r;
    }),
    wait = new Promise<void>((r) => {
      release = r;
    });
  const waiting = new PrivateTextOutputs(
    f.db,
    f.objects,
    {
      ...f.options,
      readSource: async () => {
        entered();
        await wait;
        return f.source();
      },
    },
    () => f.time.now,
  );
  const ctx = structuredClone(c);
  const operation = waiting.finalizePending(ctx, "actual", 3, signal());
  await ready;
  // Captured manager/scope/purpose must not drift while awaiting source verification.
  (ctx as { projectId: string }).projectId = "mutated";
  const out = await f.outputs.storePrivateText(
    f.consumed,
    f.derivative,
    w,
    signal(),
  );
  await f.jobs.complete(c, "actual", 3, out, "competing-completion");
  release();
  await assert.rejects(operation, { code: "output_unavailable" });
  assert.deepEqual(
    await waiting.finalizePending(c, "actual", 3, signal()),
    out,
  );
  await scoped(f.db, c, async (tx) =>
    assert.equal(
      (await tx.get("processing-output-intent", out.receipt.id))?.revision,
      2,
    ),
  );
});

test("cancellation while acquiring the publication lock fences finalization without clearing pending state", async (t) => {
  const f = await pendingOutput(t),
    controller = new AbortController();
  let observed = false;
  const db: typeof f.db = {
    transaction: (fn) =>
      f.db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (sql.includes("FOR UPDATE")) {
              observed = true;
              controller.abort();
            }
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  const outputs = new PrivateTextOutputs(
    db,
    f.objects,
    f.options,
    () => f.time.now,
  );
  await assert.rejects(
    outputs.finalizePending(c, "actual", 3, controller.signal),
  );
  assert.equal(observed, true);
  await scoped(f.db, c, async (tx) => {
    assert.deepEqual(await tx.list("processing-output-receipt"), []);
    assert.equal(
      (
        await tx.get(
          "processing-output-intent",
          digest({ jobId: "actual", consumedRevision: 3 }),
        )
      )?.revision,
      1,
    );
  });
});

test("finalization captures authenticated manager context before asynchronous current-policy verification", async (t) => {
  const f = await pendingOutput(t);
  let entered!: () => void,
    release!: () => void,
    calls = 0;
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
      authorize: async (_job, ctx) => {
        assert.equal(ctx.projectId, c.projectId);
        assert.equal(ctx.actor.id, c.actor.id);
        if (++calls === 1) {
          entered();
          await wait;
        }
        return true;
      },
    },
    () => f.time.now,
  );
  const ctx = structuredClone(c);
  const operation = outputs.finalizePending(ctx, "actual", 3, signal());
  await ready;
  (ctx as { projectId: string }).projectId = "mutated";
  (ctx.actor as { id: string }).id = "mutated";
  release();
  const out = await operation;
  assert.equal(out.derivative.projectId, c.projectId);
  assert.deepEqual(await outputs.recoverOutcome(c, "actual", 3, signal()), out);
});
