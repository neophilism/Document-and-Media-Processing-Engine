import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createReconciliationHandler } from "../src/reconciliation-http.ts";
import { fixture, c, w } from "./reconciliation-http-fixture.ts";
const origin = "https://workspace.example",
  csrf = "s".repeat(40);
const action = {
  jobId: "actual",
  consumedRevision: 3,
  output: "finalize-existing",
};
function request(
  body: unknown = action,
  headers: Record<string, string> = {},
  signal?: AbortSignal,
) {
  return new Request(origin + "/processing/reconcile", {
    method: "POST",
    headers: {
      origin,
      "content-type": "application/json",
      "x-csrf-token": csrf,
      ...headers,
    },
    body: JSON.stringify(body),
    ...(signal ? { signal } : {}),
  });
}
async function setup(t: test.TestContext) {
  const f = await fixture(t);
  const policy = {
    tenantId: c.tenantId,
    projectId: c.projectId,
    origin,
    purpose: c.purpose,
    managerIds: [c.actor.id],
  };
  let calls = 0;
  const auth = {
    authenticate: async () => {
      calls++;
      return { context: c, csrfToken: csrf };
    },
  };
  const handler = createReconciliationHandler(
    f.jobs,
    f.outputs,
    auth,
    policy,
    () => f.time.now,
  );
  return { ...f, policy, auth, handler, calls: () => calls };
}
test("HTTP session and explicit finalization use actual restricted services without object writes", async (t) => {
  const f = await setup(t);
  const session = await f.handler(
    new Request(origin + "/processing/session", {
      headers: { "sec-fetch-site": "same-origin" },
    }),
  );
  assert.equal(session.status, 200);
  assert.equal((await session.json()).csrfToken, csrf);
  assert.equal(session.headers.get("cache-control"), "private, no-store");
  const response = await f.handler(request());
  assert.equal(response.status, 200);
  const value = await response.json();
  assert.equal(value.status, "completed");
  assert.equal(value.revision, 4);
  assert.deepEqual(await readFile(f.objectPath), f.ciphertext);
  assert.equal(JSON.stringify(value).includes("hello"), false);
  f.deny();
  const denied = await f.handler(
    request({ ...action, output: "recover-stored" }),
  );
  assert.equal(denied.status, 503);
  assert.equal((await denied.json()).phase, "output");
});
test("routes, methods, query, origin, CSRF and exact input cannot reach reconciliation", async (t) => {
  const f = await setup(t),
    reads = f.reads();
  for (const r of [
    request(action, { origin: "https://foreign.example" }),
    request(action, { "sec-fetch-site": "cross-site" }),
    request(action, { "x-csrf-token": "bad" }),
    request({ ...action, context: c }),
    request({ ...action, output: "execute" }),
    request({ ...action, consumedRevision: 2 }),
    new Request(origin + "/processing/reconcile?token=bad", { method: "POST" }),
    new Request(origin + "/processing/send", { method: "POST" }),
    new Request(origin + "/processing/reconcile", { headers: { origin } }),
  ])
    assert.ok((await f.handler(r)).status >= 400);
  assert.equal(f.reads(), reads);
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
});
test("fresh host human/listed/scope/purpose authority is mandatory and auth diagnostics are sanitized", async (t) => {
  const f = await setup(t),
    reads = f.reads();
  for (const context of [
    w,
    { ...c, projectId: "foreign" },
    { ...c, purpose: "other" },
    { ...c, actor: { ...c.actor, id: "unlisted" } },
    { ...c, actor: { ...c.actor, roles: [] } },
  ]) {
    const h = createReconciliationHandler(
      f.jobs,
      f.outputs,
      { authenticate: async () => ({ context, csrfToken: csrf }) },
      f.policy,
      () => f.time.now,
    );
    assert.equal((await h(request())).status, 403);
  }
  for (const value of [null, { context: c, csrfToken: "invalid" }]) {
    const h = createReconciliationHandler(
      f.jobs,
      f.outputs,
      { authenticate: async () => value },
      f.policy,
      () => f.time.now,
    );
    assert.ok((await h(request())).status >= 400);
  }
  const h = createReconciliationHandler(
    f.jobs,
    f.outputs,
    {
      authenticate: async () => {
        throw Error("private-credential-diagnostic");
      },
    },
    f.policy,
    () => f.time.now,
  );
  const response = await h(request());
  assert.equal(response.status, 503);
  assert.equal((await response.text()).includes("private-credential"), false);
  assert.equal(f.reads(), reads);
});
test("unknown recovery never auto-finalizes; lost completion acknowledgement needs explicit replay", async (t) => {
  const f = await setup(t);
  let r = await f.handler(request({ ...action, output: "recover-stored" }));
  assert.deepEqual(await r.json(), {
    status: "reconciliation-required",
    phase: "output",
  });
  const complete = f.jobs.complete.bind(f.jobs);
  let lost = true;
  f.jobs.complete = async (...args) => {
    const done = await complete(...args);
    if (lost) {
      lost = false;
      throw Error("private lost facade acknowledgement");
    }
    return done;
  };
  r = await f.handler(request());
  assert.equal(r.status, 503);
  assert.deepEqual(await r.json(), {
    status: "reconciliation-required",
    phase: "completion",
  });
  assert.equal((await f.jobs.read(c, "actual")).job.status, "completed");
  r = await f.handler(request({ ...action, output: "recover-stored" }));
  assert.equal(r.status, 200);
  assert.equal((await r.json()).status, "completed");
});
test("hung, blocked and pre-aborted authentication cannot admit late reconciliation; signals close", async (t) => {
  const f = await setup(t);
  let signal: AbortSignal | undefined;
  const make = (
    authenticate: Parameters<
      typeof createReconciliationHandler
    >[2]["authenticate"],
  ) =>
    createReconciliationHandler(
      f.jobs,
      f.outputs,
      { authenticate },
      { ...f.policy, authenticationTimeoutMs: 10 },
      () => f.time.now,
    );
  let h = make(async (_m, s) => {
    signal = s;
    return new Promise(() => {});
  });
  assert.equal((await h(request())).status, 503);
  assert.equal(signal?.aborted, true);
  h = make(async (_m, s) => {
    signal = s;
    const until = performance.now() + 50;
    while (performance.now() < until) {}
    return { context: c, csrfToken: csrf };
  });
  assert.equal((await h(request())).status, 503);
  assert.equal(signal?.aborted, true);
  h = make(async (_m, s) => {
    signal = s;
    return { context: c, csrfToken: csrf };
  });
  assert.equal(
    (
      await h(
        new Request(origin + "/processing/session", { headers: { origin } }),
      )
    ).status,
    200,
  );
  assert.equal(signal?.aborted, true);
  const stop = new AbortController();
  stop.abort();
  assert.equal((await f.handler(request(action, {}, stop.signal))).status, 408);
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
});
test("body byte/chunk/encoding/media budgets reject before authentication", async (t) => {
  const f = await setup(t);
  const native = (body: BodyInit, headers: Record<string, string> = {}) =>
    new Request(origin + "/processing/reconcile", {
      method: "POST",
      headers: { origin, "content-type": "application/json", ...headers },
      body,
      duplex: "half",
    } as RequestInit);
  assert.equal((await f.handler(native("x".repeat(16385)))).status, 413);
  assert.equal((await f.handler(native(new Uint8Array([255])))).status, 400);
  assert.equal(
    (await f.handler(native("{}", { "content-encoding": "gzip" }))).status,
    415,
  );
  let chunks = 0;
  const stream = new ReadableStream<Uint8Array>(
    {
      pull(q) {
        if (++chunks <= 4097) q.enqueue(new Uint8Array());
        else q.close();
      },
    },
    { highWaterMark: 0 },
  );
  assert.equal((await f.handler(native(stream))).status, 413);
  assert.equal(f.calls(), 0);
});
test("stream deadline cannot be bypassed by blocked read and authentication happens after bounded body", async (t) => {
  const f = await setup(t);
  const handler = createReconciliationHandler(
    f.jobs,
    f.outputs,
    f.auth,
    { ...f.policy, bodyTimeoutMs: 10 },
    () => f.time.now,
  );
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array>(
    {
      pull(q) {
        const until = performance.now() + 50;
        while (performance.now() < until) {}
        q.enqueue(new TextEncoder().encode(JSON.stringify(action)));
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  const r = new Request(origin + "/processing/reconcile", {
    method: "POST",
    headers: { origin, "content-type": "application/json" },
    body: stream,
    duplex: "half",
  } as RequestInit);
  assert.equal((await handler(r)).status, 408);
  assert.equal(cancelled, true);
  assert.equal(f.calls(), 0);
});
test("factory captures policy/auth receiver and rejects unsafe configuration", async (t) => {
  const f = await setup(t),
    config = structuredClone(f.policy),
    auth = { authenticate: async () => ({ context: c, csrfToken: csrf }) };
  const h = createReconciliationHandler(
    f.jobs,
    f.outputs,
    auth,
    config,
    () => f.time.now,
  );
  config.managerIds.length = 0;
  config.origin = "https://foreign.example";
  auth.authenticate = async () => {
    throw Error("replaced");
  };
  assert.equal((await h(request())).status, 200);
  for (const bad of [
    { ...f.policy, origin: "http://workspace.example" },
    { ...f.policy, managerIds: [] },
    { ...f.policy, operationTimeoutMs: 0 },
  ])
    assert.throws(() =>
      createReconciliationHandler(
        f.jobs,
        f.outputs,
        f.auth,
        bad,
        () => f.time.now,
      ),
    );
});
