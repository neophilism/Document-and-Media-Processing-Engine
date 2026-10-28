import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { scoped } from "@accountability/evidence/database";
import { createIdentityReconciliationHandler } from "../src/identity-reconciliation-http.ts";
import { fixture as processing, c } from "./reconciliation-http-fixture.ts";
import { fixture as identity } from "./identity-http-fixture.ts";
const origin = "https://workspace.example",
  csrf = "s".repeat(40);
const action = {
  jobId: "actual",
  consumedRevision: 3,
  output: "finalize-existing",
};
const request = (body = action) =>
  new Request(origin + "/processing/reconcile", {
    method: "POST",
    headers: {
      origin,
      "content-type": "application/json",
      "x-csrf-token": csrf,
    },
    body: JSON.stringify(body),
  });
async function setup(t: test.TestContext) {
  const f = await processing(t),
    i = await identity(t);
  const configuration = {
    tenantId: c.tenantId,
    projectId: c.projectId,
    purpose: c.purpose,
    origin,
    managerIds: ["owner"],
  };
  let calls = 0;
  const provider = {
    authenticate: async () => {
      calls++;
      return {
        actorId: "owner",
        requestId: "session-request",
        csrfToken: csrf,
      };
    },
  };
  const handler = createIdentityReconciliationHandler(
    f.jobs,
    f.outputs,
    i.registry,
    provider,
    configuration,
    () => f.time.now,
  );
  let revision = 1;
  const change = async (roles: ("owner" | "viewer")[]) => {
    if (revision === 1) {
      await i.registry.apply(
        c,
        revision++,
        { type: "set-member", id: "second", kind: "human", roles: ["owner"] },
        "second",
      );
    }
    await i.registry.apply(
      { ...c, actor: { ...c.actor, id: "second" } },
      revision++,
      { type: "set-member", id: "owner", kind: "human", roles },
      "change-" + revision,
    );
  };
  return {
    ...f,
    identity: i,
    configuration,
    provider,
    handler,
    calls: () => calls,
    change,
  };
}

test("current Identity session composes actual HTTP/coordinator/output flow while operational databases remain separate", async (t) => {
  const f = await setup(t);
  const session = await f.handler(
    new Request(origin + "/processing/session", { headers: { origin } }),
  );
  assert.equal(session.status, 200);
  assert.equal((await session.json()).csrfToken, csrf);
  const r = await f.handler(request());
  assert.equal(r.status, 200);
  const value = await r.json();
  assert.equal(value.status, "completed");
  assert.equal(value.revision, 4);
  assert.equal(JSON.stringify(value).includes("hello"), false);
  assert.deepEqual(await readFile(f.objectPath), f.ciphertext);
  await scoped(f.identity.db, c, async (tx) => {
    assert.equal((await tx.list("identity-membership")).length, 1);
    assert.equal((await tx.list("text-processing-job")).length, 0);
  });
  await scoped(f.db, c, async (tx) => {
    assert.equal((await tx.list("identity-membership")).length, 0);
    assert.equal((await tx.list("text-processing-job")).length, 2);
  });
});

test("stored demotion denies next POST and cached completed replay without output reads or state regression", async (t) => {
  const f = await setup(t);
  assert.equal(
    (
      await f.handler(
        new Request(origin + "/processing/session", { headers: { origin } }),
      )
    ).status,
    200,
  );
  await f.change(["viewer"]);
  const before = f.reads();
  const denied = await f.handler(request());
  assert.equal(denied.status, 503);
  assert.equal(f.reads(), before);
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
  await f.change(["owner"]);
  assert.equal((await f.handler(request())).status, 200);
  await f.change(["viewer"]);
  const after = f.reads();
  assert.equal((await f.handler(request())).status, 503);
  assert.equal(f.reads(), after);
  assert.equal((await f.jobs.read(c, "actual")).job.status, "completed");
});

test("body stalls cannot retain earlier session authority: stored demotion before body completion denies reconciliation", async (t) => {
  const f = await setup(t);
  let pulled!: () => void, release!: () => void;
  const ready = new Promise<void>((r) => {
      pulled = r;
    }),
    wait = new Promise<void>((r) => {
      release = r;
    });
  const bytes = new TextEncoder().encode(JSON.stringify(action));
  const body = new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        pulled();
        await wait;
        controller.enqueue(bytes);
        controller.close();
      },
    },
    { highWaterMark: 0 },
  );
  const pending = f.handler(
    new Request(origin + "/processing/reconcile", {
      method: "POST",
      headers: {
        origin,
        "content-type": "application/json",
        "x-csrf-token": csrf,
      },
      body,
      duplex: "half",
    } as RequestInit & { duplex: "half" }),
  );
  await ready;
  assert.equal(f.calls(), 0);
  await f.change(["viewer"]);
  const before = f.reads();
  release();
  assert.equal((await pending).status, 503);
  assert.equal(f.calls(), 1);
  assert.equal(f.reads(), before);
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
});

test("lost actual completion acknowledgement stays completed; fresh membership gates later exact reconciliation", async (t) => {
  const f = await setup(t);
  let lose = true;
  const complete = f.jobs.complete.bind(f.jobs);
  f.jobs.complete = async (...args) => {
    const value = await complete(...args);
    if (lose) {
      lose = false;
      throw Error("lost acknowledgement");
    }
    return value;
  };
  const r = await f.handler(request());
  assert.equal(r.status, 503);
  assert.equal((await f.jobs.read(c, "actual")).job.status, "completed");
  await f.change(["viewer"]);
  assert.equal((await f.handler(request())).status, 503);
  await f.change(["owner"]);
  assert.equal((await f.handler(request())).status, 200);
  assert.equal((await f.jobs.read(c, "actual")).revision, 4);
  assert.deepEqual(await readFile(f.objectPath), f.ciphertext);
});

test("provider cancellation and independent manager/CSRF/policy gates cannot reach output services", async (t) => {
  const f = await setup(t),
    before = f.reads();
  let started!: () => void;
  const ready = new Promise<void>((r) => {
    started = r;
  });
  let owned: AbortSignal | undefined;
  const hanging = createIdentityReconciliationHandler(
    f.jobs,
    f.outputs,
    f.identity.registry,
    {
      authenticate: async (_m, signal) => {
        owned = signal;
        started();
        return new Promise(() => {});
      },
    },
    f.configuration,
    () => f.time.now,
  );
  const controller = new AbortController();
  const pending = hanging(
    new Request(request(), { signal: controller.signal }),
  );
  await ready;
  controller.abort();
  assert.equal((await pending).status, 408);
  assert.equal(owned?.aborted, true);
  const unlisted = createIdentityReconciliationHandler(
    f.jobs,
    f.outputs,
    f.identity.registry,
    f.provider,
    { ...f.configuration, managerIds: ["second"] },
    () => f.time.now,
  );
  assert.equal((await unlisted(request())).status, 403);
  const wrong = new Request(request(), {
    headers: {
      origin,
      "content-type": "application/json",
      "x-csrf-token": "x".repeat(40),
    },
  });
  assert.equal((await f.handler(wrong)).status, 403);
  const configured = {
    ...f.configuration,
    managerIds: [...f.configuration.managerIds],
  };
  const fixed = createIdentityReconciliationHandler(
    f.jobs,
    f.outputs,
    f.identity.registry,
    f.provider,
    configured,
    () => f.time.now,
  );
  configured.managerIds[0] = "foreign";
  configured.purpose = "mutated";
  assert.equal(f.reads(), before);
  f.deny();
  assert.equal((await fixed(request())).status, 503);
  assert.ok(f.reads() >= before);
  assert.equal((await f.jobs.read(c, "actual")).revision, 3);
});
