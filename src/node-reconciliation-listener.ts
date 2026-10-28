import type {
  IncomingMessage,
  ServerResponse,
  RequestListener,
} from "node:http";
import { TLSSocket } from "node:tls";
import { invariant } from "./contracts.ts";

export type NodeReconciliationPolicy = Readonly<{
  origin: string;
  maxInFlight?: number;
  exchangeTimeoutMs?: number;
}>;
const headers = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "private, no-store",
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "same-origin",
  connection: "close",
};

/** Direct TLS only. Host owns server header/handshake limits, certificates and session proofing. */
export function createNodeReconciliationListener(
  handler: (request: Request) => Promise<Response>,
  configuration: NodeReconciliationPolicy,
): RequestListener {
  const policy = structuredClone(configuration),
    origin = new URL(policy.origin),
    max = policy.maxInFlight ?? 16,
    timeout = policy.exchangeTimeoutMs ?? 30000;
  invariant(
    origin.protocol === "https:" &&
      origin.origin === policy.origin &&
      !origin.username &&
      !origin.password &&
      typeof handler === "function" &&
      Number.isSafeInteger(max) &&
      max >= 1 &&
      max <= 128 &&
      Number.isSafeInteger(timeout) &&
      timeout >= 10 &&
      timeout <= 60000,
    "invalid_policy",
    "Invalid direct TLS listener policy",
  );
  let active = 0;
  const reply = (res: ServerResponse, status: number, value: unknown) => {
    if (res.destroyed || res.writableEnded) return;
    res.writeHead(status, headers);
    res.end(JSON.stringify(value));
  };
  return (req, res) => {
    if (active >= max) {
      req.pause();
      reply(res, 503, { error: "listener_unavailable" });
      return;
    }
    active++;
    void handle(req, res)
      .catch(() => res.destroy())
      .finally(() => {
        active--;
      });
  };
  async function handle(req: IncomingMessage, res: ServerResponse) {
    const controller = new AbortController(),
      deadline = performance.now() + timeout;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let response: Response | undefined;
    let handlerStarted = false;
    let stopBody = () => {};
    const abort = () => controller.abort();
    const closed = () => {
      if (!res.writableFinished) abort();
    };
    req.on("aborted", abort);
    res.on("close", closed);
    res.on("error", abort);
    let interrupt = () => {};
    const interrupted = new Promise<never>((_, reject) => {
      interrupt = () => reject(Error("interrupted"));
      controller.signal.addEventListener("abort", interrupt, { once: true });
    });
    // Rejection is observed even if admission fails before its first race.
    void interrupted.catch(() => {});
    const gate = () => {
      if (controller.signal.aborted || performance.now() >= deadline)
        throw Error("interrupted");
    };
    timer = setTimeout(() => {
      abort();
      reply(res, 503, { error: "outcome_unknown" });
    }, timeout);
    try {
      gate();
      if (
        !(req.socket instanceof TLSSocket) ||
        !req.socket.encrypted ||
        !["TLSv1.2", "TLSv1.3"].includes(req.socket.getProtocol() ?? "") ||
        req.httpVersion !== "1.1"
      )
        throw Error("admission");
      const seen = new Set<string>();
      let bytes = 0;
      if (req.rawHeaders.length > 256) throw Error("admission");
      for (let i = 0; i < req.rawHeaders.length; i += 2) {
        const name = req.rawHeaders[i]!.toLowerCase(),
          value = req.rawHeaders[i + 1]!;
        bytes += Buffer.byteLength(name) + Buffer.byteLength(value);
        if (
          seen.has(name) ||
          name === "forwarded" ||
          name.startsWith("x-forwarded-")
        )
          throw Error("admission");
        seen.add(name);
      }
      if (
        bytes > 16384 ||
        req.headers.host !== origin.host ||
        req.headers.upgrade ||
        req.headers.expect ||
        !req.url?.startsWith("/") ||
        req.url.startsWith("//") ||
        /[\\#]/.test(req.url)
      )
        throw Error("admission");
      const url = new URL(req.url, origin);
      if (
        url.origin !== policy.origin ||
        !["/processing/session", "/processing/reconcile"].includes(
          url.pathname,
        ) ||
        !["GET", "POST"].includes(req.method ?? "")
      )
        throw Error("admission");
      if (
        req.method === "GET" &&
        ((req.headers["content-length"] !== undefined &&
          req.headers["content-length"] !== "0") ||
          req.headers["transfer-encoding"])
      )
        throw Error("admission");
      const length = req.headers["content-length"];
      if (
        length !== undefined &&
        (typeof length !== "string" ||
          !/^\d+$/.test(length) ||
          Number(length) > 16384)
      )
        throw Error("admission");
      const requestHeaders = new Headers();
      for (const [name, value] of Object.entries(req.headers)) {
        if (
          [
            "connection",
            "keep-alive",
            "upgrade",
            "transfer-encoding",
            "te",
            "trailer",
            "proxy-connection",
          ].includes(name)
        )
          continue;
        if (Array.isArray(value)) throw Error("admission");
        if (value !== undefined) requestHeaders.set(name, value);
      }
      let body: ReadableStream<Uint8Array> | undefined;
      if (req.method === "POST") {
        body = new ReadableStream<Uint8Array>(
          {
            start(stream) {
              let size = 0,
                count = 0,
                stopped = false;
              const clean = () => {
                req.pause();
                req.off("data", data);
                req.off("end", end);
                req.off("error", error);
              };
              stopBody = () => {
                if (stopped) return;
                stopped = true;
                clean();
              };
              const data = (chunk: Buffer) => {
                req.pause();
                if (stopped) return;
                size += chunk.byteLength;
                count++;
                if (size > 16384 || count > 4096) {
                  stopBody();
                  stream.error(Error("body_limit"));
                  return;
                }
                stream.enqueue(new Uint8Array(chunk));
              };
              const end = () => {
                if (stopped) return;
                stopBody();
                stream.close();
              };
              const error = () => {
                if (stopped) return;
                stopBody();
                stream.error(Error("body_unavailable"));
              };
              req.on("data", data);
              req.on("end", end);
              req.on("error", error);
              req.pause();
            },
            pull() {
              req.resume();
            },
            cancel() {
              stopBody();
            },
          },
          { highWaterMark: 0 },
        );
      }
      gate();
      const request = new Request(url, {
        method: req.method,
        headers: requestHeaders,
        signal: controller.signal,
        ...(body ? { body, duplex: "half" } : {}),
      } as RequestInit & { duplex?: "half" });
      // Keep the concurrency slot until the trusted handler settles, even after disconnect/deadline.
      handlerStarted = true;
      response = await handler(request);
      gate();
      if (
        !response.body ||
        response.status < 200 ||
        response.status > 599 ||
        !/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(
          response.headers.get("content-type") ?? "",
        ) ||
        !/\bno-store\b/i.test(response.headers.get("cache-control") ?? "")
      )
        throw Error("response");
      reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0,
        count = 0;
      for (;;) {
        gate();
        const part = await Promise.race([reader.read(), interrupted]);
        gate();
        if (part.done) break;
        if (!(part.value instanceof Uint8Array)) throw Error("response");
        size += part.value.byteLength;
        if (size > 262144 || ++count > 4096) throw Error("response");
        chunks.push(new Uint8Array(part.value));
      }
      gate();
      const output = Buffer.concat(chunks);
      JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(output));
      gate();
      if (!res.destroyed && !res.writableEnded) {
        res.writeHead(response.status, headers);
        res.end(output);
      }
    } catch {
      reply(
        res,
        handlerStarted ||
          controller.signal.aborted ||
          performance.now() >= deadline
          ? 503
          : 400,
        { error: "request_unavailable" },
      );
    } finally {
      if (timer) clearTimeout(timer);
      stopBody();
      req.pause();
      if (reader) void reader.cancel().catch(() => {});
      else if (response?.body) void response.body.cancel().catch(() => {});
      req.off("aborted", abort);
      res.off("close", closed);
      res.off("error", abort);
      req.once("error", () => {});
      res.once("error", () => {});
      controller.signal.removeEventListener("abort", interrupt);
      controller.abort();
    }
  }
}
