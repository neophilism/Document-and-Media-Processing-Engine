import type test from "node:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { createServer, request } from "node:https";
import type { RequestListener } from "node:http";
import { createNodeReconciliationListener } from "../src/node-reconciliation-listener.ts";

export async function tlsFixture(
  t: test.TestContext,
  factory: (origin: string) => (r: Request) => Promise<Response>,
  limits: { maxInFlight?: number; exchangeTimeoutMs?: number } = {},
) {
  const root = await mkdtemp(join(tmpdir(), "private-node-tls-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const keyPath = join(root, "key.pem"),
    certPath = join(root, "cert.pem");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost",
    ],
    { stdio: "ignore", timeout: 10000 },
  );
  const key = await readFile(keyPath),
    cert = await readFile(certPath);
  let listener: RequestListener = (_req, res) => {
    res.writeHead(503);
    res.end();
  };
  const server = createServer(
    { key, cert, minVersion: "TLSv1.2", maxHeaderSize: 16384 },
    (req, res) => listener(req, res),
  );
  server.headersTimeout = 1000;
  server.requestTimeout = 5000;
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  const address = server.address();
  if (!address || typeof address === "string") throw Error("missing address");
  const origin = "https://localhost:" + address.port;
  listener = createNodeReconciliationListener(factory(origin), {
    origin,
    ...limits,
  });
  const send = (
    path: string,
    body?: string,
    extra: Record<string, string> = {},
  ) =>
    new Promise<{
      status: number;
      body: string;
      headers: Record<string, unknown>;
    }>((resolve, reject) => {
      const req = request(
        origin + path,
        {
          ca: cert,
          servername: "localhost",
          agent: false,
          family: 4,
          lookup: (_host, _options, cb) => cb(null, "127.0.0.1", 4),
          method: body === undefined ? "GET" : "POST",
          headers: {
            origin,
            ...(body === undefined
              ? {}
              : { "content-type": "application/json" }),
            ...extra,
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (b: Buffer) => chunks.push(b));
          res.on("end", () =>
            resolve({
              status: res.statusCode!,
              body: Buffer.concat(chunks).toString("utf8"),
              headers: res.headers,
            }),
          );
          res.on("error", reject);
        },
      );
      req.on("error", reject);
      req.setTimeout(5000, () => req.destroy(Error("fixture timeout")));
      req.end(body);
    });
  return { server, origin, cert, send };
}
export const json = (v: unknown) =>
  new Response(JSON.stringify(v), {
    headers: {
      "content-type": "application/json",
      "cache-control": "private, no-store",
    },
  });
