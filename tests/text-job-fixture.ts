import test from "node:test";
import { readFile } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import type { Database } from "@accountability/evidence/database";
import { TextProcessingJobs } from "../src/text-jobs.ts";
import type { TextJobVerification } from "../src/text-jobs.ts";
import type { Context } from "../src/contracts.ts";
export const c: Context = {
  tenantId: "fiction",
  projectId: "processing",
  actor: { id: "owner", kind: "human", roles: ["owner"] },
  purpose: "review",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
export const w: Context = {
  ...c,
  actor: { id: "worker", kind: "service", roles: ["process"] },
};
export const input = {
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
export const outcome = {
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
export async function setup(
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
  const time = { now: c.now };
  const jobs = new TextProcessingJobs(db, policy, checks, () => time.now);
  await jobs.submit(c, input, "submit");
  return { db, jobs, policy, checks, time };
}
