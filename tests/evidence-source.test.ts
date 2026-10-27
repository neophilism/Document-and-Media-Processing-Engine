import test from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { EvidenceService } from "@accountability/evidence-service-sdk/service";
import type { Identity } from "@accountability/evidence-service-sdk/service";
import type { Database } from "@accountability/evidence-service-sdk/database";
import { PrivateObjectStore } from "@accountability/evidence-service-sdk/objects";
import { createEvidenceTextSource } from "../src/evidence-source.ts";
import { runLeasedTextJob } from "../src/text-worker.ts";
import { hashBytes } from "../src/contracts.ts";
import { setup, c, w, outcome } from "./text-job-fixture.ts";
const bytes = new TextEncoder().encode("hello"),
  author = "fiction_author_credential_more_than32characters",
  scannerToken = "fiction_scanner_credential_more_than32characters";
async function fixture(t: test.TestContext) {
  const pg = new PGlite();
  t.after(() => pg.close());
  await pg.exec(
    await readFile(
      new URL(
        "../migrations/001_foundation.sql",
        import.meta.resolve("@accountability/evidence-service-sdk/database"),
      ),
      "utf8",
    ),
  );
  await pg.exec(
    "CREATE ROLE source_login LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION; GRANT evidence_runtime TO source_login; SET SESSION AUTHORIZATION source_login",
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
  const root = await mkdtemp(join(tmpdir(), "source-adapter-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const identities: Identity[] = [
    {
      tenantId: c.tenantId,
      projectId: c.projectId,
      actor: { id: "reader", kind: "service", roles: ["researcher"] },
      tokenHash: hashBytes(Buffer.from(author)),
      disabled: false,
      epoch: 1,
    },
    {
      tenantId: c.tenantId,
      projectId: c.projectId,
      actor: {
        id: "scanner",
        kind: "service",
        roles: ["operator", "evidence_scanner"],
      },
      tokenHash: hashBytes(Buffer.from(scannerToken)),
      disabled: false,
      epoch: 1,
    },
  ];
  const service = new EvidenceService({
    database: db,
    objects: new PrivateObjectStore(root, randomBytes(32), "fixture-key"),
    identities: async () => identities,
    now: () => c.now,
    scanner: {
      version: "synthetic-test-scanner",
      scan: async (i) => ({
        attemptId: i.attemptId,
        sha256: i.sha256,
        engineVersion: "synthetic-test-scanner",
        verdict: "clean",
      }),
    },
  });
  const request = {
    token: author,
    purpose: c.purpose,
    requestId: "source-request",
  };
  const acquired = await service.acquire(request, {
    id: "source",
    acquisitionId: "fixture",
    bytes,
    mediaType: "text/plain",
    capturedAt: c.now,
    originalLanguage: "und",
    supersedes: null,
    expectedRevision: 0,
    idempotencyKey: "source",
  });
  const f = await setup(t);
  await f.jobs.submit(
    c,
    {
      id: "actual",
      source: acquired.ref,
      sourceSha256: hashBytes(bytes),
      sourceBytes: 5,
      language: null,
    },
    "actual",
  );
  await f.jobs.lease(w, "actual", 1);
  const consume = () => f.jobs.consume(w, "actual", 2);
  return {
    ...f,
    service,
    identities,
    request,
    acquired,
    consume,
    scan: () => service.runNextScan({ ...request, token: scannerToken }),
    source: () =>
      createEvidenceTextSource(service, {
        credential: async () => author,
        authorizeProcessing: async () => true,
      }),
  };
}
test("source adapter joins actual authenticated Evidence encrypted storage and independent processing database", async (t) => {
  const f = await fixture(t);
  await f.scan();
  const read = f.source();
  const result = await runLeasedTextJob(
    f.jobs,
    w,
    "actual",
    2,
    {
      readSource: read,
      storePrivateText: async (_j, d) => ({ ...outcome, sha256: d.sha256 }),
    },
    () => c.now,
  );
  assert.equal(result.status, "completed");
  assert.equal((await f.jobs.read(w, "actual")).job.status, "completed");
});
test("real quarantine, processing denial and identity revocation cannot yield source bytes", async (t) => {
  const f = await fixture(t),
    job = await f.consume();
  await assert.rejects(f.source()(job, w, new AbortController().signal), {
    code: "source_unavailable",
  });
  await f.scan();
  await assert.rejects(
    createEvidenceTextSource(f.service, {
      credential: async () => author,
      authorizeProcessing: async () => false,
    })(job, w, new AbortController().signal),
  );
  f.identities[0]!.disabled = true;
  await assert.rejects(f.source()(job, w, new AbortController().signal));
  assert.equal((await f.jobs.read(w, "actual")).job.status, "consumed");
});
test("quarantine after actual read is rejected by post-read inspection and returned bytes never enter processing", async (t) => {
  const f = await fixture(t);
  await f.scan();
  const job = await f.consume();
  let reads = 0;
  const port = {
    inspect: f.service.inspect.bind(f.service),
    read: async (...args: Parameters<EvidenceService["read"]>) => {
      reads++;
      const b = await f.service.read(...args);
      const state = await f.service.inspect(f.request, f.acquired.ref);
      await f.service.quarantine(
        { ...f.request, token: scannerToken },
        f.acquired.ref,
        state.revision,
        "safety",
      );
      return b;
    },
  };
  const read = createEvidenceTextSource(port, {
    credential: async () => author,
    authorizeProcessing: async () => true,
  });
  await assert.rejects(read(job, w, new AbortController().signal), {
    code: "source_unavailable",
  });
  assert.equal(reads, 1);
  assert.equal(
    (await f.service.inspect(f.request, f.acquired.ref)).scan,
    "quarantined",
  );
});
test("bounded credential lookup, pre-abort and sanitized callback failures do not trigger later Evidence reads", async (t) => {
  const f = await fixture(t);
  await f.scan();
  const job = await f.consume();
  let calls = 0,
    finish!: (v: string) => void;
  const port = {
    inspect: async (...args: Parameters<EvidenceService["inspect"]>) => {
      calls++;
      return f.service.inspect(...args);
    },
    read: f.service.read.bind(f.service),
  };
  const read = createEvidenceTextSource(port, {
    credential: async () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
    authorizeProcessing: async () => true,
    timeoutMs: 10,
  });
  await assert.rejects(read(job, w, new AbortController().signal), {
    code: "source_unavailable",
  });
  finish(author);
  await new Promise((r) => setImmediate(r));
  assert.equal(calls, 0);
  const aborted = new AbortController();
  aborted.abort();
  await assert.rejects(f.source()(job, w, aborted.signal));
  await assert.rejects(
    createEvidenceTextSource(port, {
      credential: async () => {
        throw Error(author);
      },
      authorizeProcessing: async () => true,
    })(job, w, new AbortController().signal),
    (e) => {
      assert.equal(String(e).includes(author), false);
      return true;
    },
  );
  assert.equal(calls, 0);
});

test("exact metadata and returned-byte mismatches fail closed rather than trusting facade-shaped values", async (t) => {
  const f = await fixture(t);
  await f.scan();
  const job = await f.consume();
  const extra = structuredClone(job);
  Object.assign(extra.job.source, { credential: "must-not-propagate" });
  await assert.rejects(f.source()(extra, w, new AbortController().signal));
  for (const mutate of [
    (x: Awaited<ReturnType<EvidenceService["inspect"]>>) => {
      x.version = { ...x.version, sha256: "b".repeat(64) };
    },
    (x: Awaited<ReturnType<EvidenceService["inspect"]>>) => {
      x.version = { ...x.version, byteLength: 6 };
    },
    (x: Awaited<ReturnType<EvidenceService["inspect"]>>) => {
      x.version = { ...x.version, mediaType: "application/pdf" };
    },
    (x: Awaited<ReturnType<EvidenceService["inspect"]>>) => {
      x.version = { ...x.version, allowedPurposes: ["other"] };
    },
    (x: Awaited<ReturnType<EvidenceService["inspect"]>>) => {
      x.version = {
        ...x.version,
        ref: { ...x.version.ref, projectId: "foreign" },
      };
    },
  ]) {
    let reads = 0;
    const port = {
      inspect: async (...args: Parameters<EvidenceService["inspect"]>) => {
        const v = await f.service.inspect(...args);
        mutate(v);
        return v;
      },
      read: async (...args: Parameters<EvidenceService["read"]>) => {
        reads++;
        return f.service.read(...args);
      },
    };
    await assert.rejects(
      createEvidenceTextSource(port, {
        credential: async () => author,
        authorizeProcessing: async () => true,
      })(job, w, new AbortController().signal),
    );
    assert.equal(reads, 0);
  }
  for (const corrupt of [false, true]) {
    let inspections = 0;
    const port = {
      inspect: async (...args: Parameters<EvidenceService["inspect"]>) => {
        const v = await f.service.inspect(...args);
        if (++inspections === 2 && !corrupt) v.revision++;
        return v;
      },
      read: async (...args: Parameters<EvidenceService["read"]>) => {
        const b = await f.service.read(...args);
        if (corrupt) b[0] = 0;
        return b;
      },
    };
    await assert.rejects(
      createEvidenceTextSource(port, {
        credential: async () => author,
        authorizeProcessing: async () => true,
      })(job, w, new AbortController().signal),
    );
  }
});
test("captured source context is stable while current processing authority is rechecked after bytes are read", async (t) => {
  const f = await fixture(t);
  await f.scan();
  const job = await f.consume();
  let checks = 0;
  await assert.rejects(
    createEvidenceTextSource(f.service, {
      credential: async () => author,
      authorizeProcessing: async () => ++checks === 1,
    })(job, w, new AbortController().signal),
  );
  assert.equal(checks, 2);
  let entered!: () => void, finish!: (v: string) => void;
  const ready = new Promise<void>((r) => {
    entered = r;
  });
  const context = structuredClone(w),
    supplied = structuredClone(job);
  const source = createEvidenceTextSource(f.service, {
    timeoutMs: 2000,
    authorizeProcessing: async (j, ctx) => {
      (j.job.source as { id: string }).id = "callback-mutated";
      (ctx.actor as { id: string }).id = "callback-mutated";
      return true;
    },
    credential: async (ctx) => {
      assert.equal(ctx.actor.id, w.actor.id);
      entered();
      return new Promise((r) => {
        finish = r;
      });
    },
  });
  const pending = source(supplied, context, new AbortController().signal);
  await ready;
  (supplied.job.source as { id: string }).id = "caller-mutated";
  (context as { purpose: string }).purpose = "caller-mutated";
  finish(author);
  const value = await pending;
  assert.deepEqual(value.source, f.acquired.ref);
  assert.deepEqual(value.bytes, bytes);
  assert.equal(value.processingAllowed, true);
});
