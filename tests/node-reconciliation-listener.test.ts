import test from "node:test";
import assert from "node:assert/strict";
import { request as httpsRequest } from "node:https";
import { readFile } from "node:fs/promises";
import { createIdentityReconciliationHandler } from "../src/identity-reconciliation-http.ts";
import { fixture as processing, c } from "./reconciliation-http-fixture.ts";
import { fixture as identity } from "./identity-http-fixture.ts";
import { createNodeReconciliationListener } from "../src/node-reconciliation-listener.ts";
import { tlsFixture, json } from "./node-listener-fixture.ts";

const csrf = "s".repeat(40),
  action = JSON.stringify({
    jobId: "actual",
    consumedRevision: 3,
    output: "finalize-existing",
  });
test("actual verified loopback TLS bridges Identity and encrypted Processing reconciliation; demotion denies the next request", async (t) => {
  const p = await processing(t),
    i = await identity(t);
  const f = await tlsFixture(t, (origin) =>
    createIdentityReconciliationHandler(
      p.jobs,
      p.outputs,
      i.registry,
      {
        authenticate: async () => ({
          actorId: "owner",
          requestId: "tls-request",
          csrfToken: csrf,
        }),
      },
      {
        tenantId: c.tenantId,
        projectId: c.projectId,
        purpose: c.purpose,
        managerIds: ["owner"],
        origin,
      },
      () => p.time.now,
    ),
  );
  const session = await f.send("/processing/session");
  assert.equal(session.status, 200);
  assert.equal(JSON.parse(session.body).csrfToken, csrf);
  const result = await f.send("/processing/reconcile", action, {
    "x-csrf-token": csrf,
  });
  assert.equal(result.status, 200);
  assert.equal(JSON.parse(result.body).status, "completed");
  assert.equal(result.headers["cache-control"], "private, no-store");
  assert.equal(result.headers.connection, "close");
  assert.deepEqual(await readFile(p.objectPath), p.ciphertext);
  await i.registry.apply(
    c,
    1,
    { type: "set-member", id: "second", kind: "human", roles: ["owner"] },
    "second",
  );
  await i.registry.apply(
    { ...c, actor: { ...c.actor, id: "second" } },
    2,
    { type: "disable-member", id: "owner" },
    "disable",
  );
  const before = p.reads();
  assert.equal(
    (await f.send("/processing/reconcile", action, { "x-csrf-token": csrf }))
      .status,
    503,
  );
  assert.equal(p.reads(), before);
});

test("authority, forwarded metadata, unknown routes and declared body limits are rejected before handler invocation", async (t) => {
  let calls = 0;
  const f = await tlsFixture(t, () => async () => {
    calls++;
    return json({ ok: true });
  });
  for (const [path, body, headers] of [
    ["/processing/session", undefined, { host: "foreign.example" }],
    ["/processing/session", undefined, { "x-forwarded-host": "localhost" }],
    [
      "/processing/session",
      undefined,
      { forwarded: "host=localhost;proto=https" },
    ],
    ["/processing/execute", "{}", {}],
    ["/processing/reconcile", "{}", { "content-length": "16385" }],
  ] as [string, string | undefined, Record<string, string>][])
    assert.equal((await f.send(path, body, headers)).status, 400);
  assert.equal(calls, 0);
  assert.equal((await f.send("/processing/session")).status, 200);
  assert.equal(calls, 1);
});

test("streamed actual body bytes are bounded and normal body completion does not cancel a valid response", async (t) => {
  let received = 0;
  const f = await tlsFixture(t, () => async (req) => {
    const reader = req.body!.getReader();
    try {
      for (;;) {
        const r = await reader.read();
        if (r.done) break;
        received += r.value.length;
      }
    } finally {
      void reader.cancel().catch(() => {});
    }
    return json({ received });
  });
  assert.equal(
    (
      await f.send("/processing/reconcile", "x".repeat(16385), {
        "transfer-encoding": "chunked",
      })
    ).status,
    503,
  );
  assert.ok(received <= 16384);
  received = 0;
  const valid = await f.send("/processing/reconcile", "hello", {
    "transfer-encoding": "chunked",
  });
  assert.equal(valid.status, 200);
  assert.equal(JSON.parse(valid.body).received, 5);
});

test("deadline closes signals and preserves occupied concurrency until a noncooperative handler actually settles", async (t) => {
  let finish!: (r: Response) => void,
    owned: AbortSignal | undefined,
    calls = 0;
  const f = await tlsFixture(
    t,
    () => async (req) => {
      calls++;
      owned = req.signal;
      return new Promise((r) => {
        finish = r;
      });
    },
    { maxInFlight: 1, exchangeTimeoutMs: 30 },
  );
  assert.equal((await f.send("/processing/session")).status, 503);
  assert.equal(owned?.aborted, true);
  assert.equal((await f.send("/processing/session")).status, 503);
  assert.equal(calls, 1);
  finish(json({ late: true }));
  await new Promise((r) => setImmediate(r));
  assert.equal((await f.send("/processing/session")).status, 503);
  assert.equal(calls, 2);
  finish(json({ late: true }));
});

test("client disconnect cancels the Fetch request and late responses cannot write into a closed socket", async (t) => {
  let started!: () => void,
    finish!: (r: Response) => void,
    owned: AbortSignal | undefined;
  const ready = new Promise<void>((r) => {
    started = r;
  });
  const f = await tlsFixture(t, () => async (req) => {
    owned = req.signal;
    started();
    return new Promise((r) => {
      finish = r;
    });
  });
  const req = httpsRequest(f.origin + "/processing/session", {
    ca: f.cert,
    servername: "localhost",
    agent: false,
    family: 4,
    lookup: (_h, _o, cb) => cb(null, "127.0.0.1", 4),
    headers: { origin: f.origin },
  });
  req.on("error", () => {});
  req.end();
  await ready;
  req.destroy();
  const until = performance.now() + 3000;
  while (!owned?.aborted) {
    assert.ok(performance.now() < until);
    await new Promise((r) => setTimeout(r, 5));
  }
  finish(json({ late: true }));
  await new Promise((r) => setImmediate(r));
});

test("response byte/chunk limits and invalid configuration fail closed with sanitized private errors", async (t) => {
  for (const origin of [
    "http://localhost",
    "https://localhost/",
    "https://a:b@localhost",
  ])
    assert.throws(() =>
      createNodeReconciliationListener(async () => json({}), { origin }),
    );
  for (const exchangeTimeoutMs of [0, 60001, Infinity])
    assert.throws(() =>
      createNodeReconciliationListener(async () => json({}), {
        origin: "https://localhost",
        exchangeTimeoutMs,
      }),
    );
  for (const behavior of ["bytes", "chunks", "throw", "utf8", "json"]) {
    const f = await tlsFixture(t, () => async () => {
      if (behavior === "throw") throw Error("private credential diagnostic");
      if (behavior === "utf8" || behavior === "json")
        return new Response(
          behavior === "utf8" ? new Uint8Array([255]) : "not JSON",
          {
            headers: {
              "content-type": "application/json",
              "cache-control": "no-store",
            },
          },
        );
      if (behavior === "bytes") return json({ value: "x".repeat(262145) });
      let count = 0;
      return new Response(
        new ReadableStream({
          pull(controller) {
            controller.enqueue(new Uint8Array());
            if (++count > 4100) controller.close();
          },
        }),
        {
          headers: {
            "content-type": "application/json",
            "cache-control": "no-store",
          },
        },
      );
    });
    const r = await f.send("/processing/session");
    assert.equal(r.status, 503);
    assert.equal(r.body.includes("private credential"), false);
    assert.equal(r.headers["cache-control"], "private, no-store");
  }
});

test("loopback TLS verifies hostname and duplicate raw authority never reaches the handler", async (t) => {
  const { connect } = await import("node:tls");
  let calls = 0;
  const f = await tlsFixture(t, () => async () => {
    calls++;
    return json({});
  });
  const url = new URL(f.origin);
  await assert.rejects(
    new Promise<void>((resolve, reject) => {
      const socket = connect(
        {
          host: "127.0.0.1",
          port: Number(url.port),
          ca: f.cert,
          servername: "wrong.example",
        },
        () => {
          socket.destroy();
          resolve();
        },
      );
      socket.on("error", reject);
    }),
    { code: "ERR_TLS_CERT_ALTNAME_INVALID" },
  );
  const raw = await new Promise<string>((resolve, reject) => {
    const socket = connect(
      {
        host: "127.0.0.1",
        port: Number(url.port),
        ca: f.cert,
        servername: "localhost",
      },
      () => {
        socket.write(
          "GET /processing/session HTTP/1.1\r\nHost: " +
            url.host +
            "\r\nHost: " +
            url.host +
            "\r\nConnection: close\r\n\r\n",
        );
      },
    );
    let value = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      value += chunk;
    });
    socket.on("end", () => resolve(value));
    socket.on("error", reject);
  });
  assert.match(raw, /^HTTP\/1.1 400/);
  assert.equal(calls, 0);
  const { createServer, request } = await import("node:http");
  const listener = createNodeReconciliationListener(
    async () => {
      calls++;
      return json({});
    },
    { origin: f.origin },
  );
  const plain = createServer(listener);
  await new Promise<void>((resolve) => plain.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    plain.closeAllConnections();
    await new Promise<void>((r) => plain.close(() => r()));
  });
  const address = plain.address();
  if (!address || typeof address === "string") throw Error("fixture");
  const status = await new Promise<number>((resolve, reject) => {
    const req = request(
      {
        hostname: "127.0.0.1",
        port: address.port,
        path: "/processing/session",
        headers: { host: url.host },
      },
      (res) => {
        res.resume();
        res.on("end", () => resolve(res.statusCode!));
      },
    );
    req.on("error", reject);
    req.end();
  });
  assert.equal(status, 400);
  assert.equal(calls, 0);
});

test("blocked-JS handler overrun is unknown before delayed timer; late response bytes are cancelled", async (t) => {
  let owned: AbortSignal | undefined,
    cancelled = false;
  const f = await tlsFixture(
    t,
    () => async (req) => {
      owned = req.signal;
      const until = performance.now() + 50;
      while (performance.now() < until) {}
      return new Response(
        new ReadableStream({
          cancel() {
            cancelled = true;
          },
        }),
        {
          headers: {
            "content-type": "application/json",
            "cache-control": "no-store",
          },
        },
      );
    },
    { exchangeTimeoutMs: 10 },
  );
  assert.equal((await f.send("/processing/session")).status, 503);
  assert.equal(owned?.aborted, true);
  assert.equal(cancelled, true);
});
