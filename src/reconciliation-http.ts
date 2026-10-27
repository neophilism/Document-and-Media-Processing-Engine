import { timingSafeEqual } from "node:crypto";
import { DomainError, invariant, nonempty, requireScope } from "./contracts.ts";
import type { Context, Scope } from "./contracts.ts";
import { validTime } from "./contracts.ts";
import { reconcileTextOutput } from "./text-reconciliation.ts";
import type { TextProcessingJobs } from "./text-jobs.ts";
import type { PrivateTextOutputs } from "./text-outputs.ts";

export type ReconciliationSession = Readonly<{
  context: Context;
  csrfToken: string;
}>;
export type ReconciliationAuthentication = Readonly<{
  authenticate(
    metadata: { url: string; method: string; headers: Headers },
    signal: AbortSignal,
  ): Promise<ReconciliationSession | null>;
}>;
export type ReconciliationHttpPolicy = Scope &
  Readonly<{
    origin: string;
    purpose: string;
    managerIds: readonly string[];
    operationTimeoutMs?: number;
    authenticationTimeoutMs?: number;
    bodyTimeoutMs?: number;
  }>;
const responseHeaders = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "private, no-store",
  pragma: "no-cache",
  vary: "Cookie, Authorization",
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "same-origin",
};
function reply(status: number, value: unknown): Response {
  const body = JSON.stringify(value);
  invariant(
    Buffer.byteLength(body) <= 262144,
    "response_too_large",
    "Response budget exceeded",
  );
  return new Response(body, { status, headers: responseHeaders });
}
function exact(
  value: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  invariant(
    value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) === Object.prototype &&
      Object.keys(value).length === keys.length &&
      keys.every((k) => Object.hasOwn(value, k)),
    "invalid_request",
    "Exact JSON fields required",
  );
  return value as Record<string, unknown>;
}
function revision(value: unknown): number {
  invariant(
    Number.isSafeInteger(value) && (value as number) > 0,
    "invalid_request",
    "Positive viewed revision required",
  );
  return value as number;
}
function text(value: unknown): string {
  nonempty(value, "Identifier", 128);
  return value;
}
const statusCodes: Readonly<Record<string, number>> = {
  authentication_required: 401,
  authentication_unavailable: 503,
  manager_required: 403,
  scope_denied: 403,
  purpose_denied: 403,
  origin_denied: 403,
  csrf_denied: 403,
  route_not_found: 404,
  method_denied: 405,
  request_aborted: 408,
  body_timeout: 408,
  body_too_large: 413,
  response_too_large: 413,
  unsupported_media: 415,
  invalid_request: 400,
  invalid_input: 400,
};
async function session(
  auth: ReconciliationAuthentication,
  request: Request,
  timeoutMs: number,
): Promise<ReconciliationSession> {
  const controller = new AbortController(),
    deadline = performance.now() + timeoutMs;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let interrupt = () => {};
  let timedOut = false;
  const abort = () => controller.abort();
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  try {
    invariant(
      !controller.signal.aborted && performance.now() < deadline,
      "request_aborted",
      "Request cancelled",
    );
    const result = await Promise.race([
      Promise.resolve()
        .then(() => {
          invariant(
            !controller.signal.aborted && performance.now() < deadline,
            "request_aborted",
            "Request cancelled",
          );
          const metadata = {
            url: request.url,
            method: request.method,
            headers: new Headers(request.headers),
          };
          invariant(
            !controller.signal.aborted && performance.now() < deadline,
            "authentication_unavailable",
            "Authentication deadline exceeded",
          );
          return auth.authenticate(metadata, controller.signal);
        })
        .then((value) => {
          invariant(
            !controller.signal.aborted && performance.now() < deadline,
            "authentication_unavailable",
            "Authentication deadline exceeded",
          );
          const captured = value === null ? null : structuredClone(value);
          invariant(
            performance.now() < deadline,
            "authentication_unavailable",
            "Authentication deadline exceeded",
          );
          return captured;
        })
        .catch(() => {
          throw new DomainError(
            "authentication_unavailable",
            "Authentication unavailable",
          );
        }),
      new Promise<never>((_, reject) => {
        interrupt = () =>
          reject(
            new DomainError(
              timedOut ? "authentication_unavailable" : "request_aborted",
              "Authentication interrupted",
            ),
          );
        controller.signal.addEventListener("abort", interrupt, { once: true });
        timer = setTimeout(() => {
          timedOut = true;
          controller.abort();
        }, timeoutMs);
      }),
    ]);
    invariant(
      !controller.signal.aborted && performance.now() < deadline,
      timedOut ? "authentication_unavailable" : "request_aborted",
      "Request cancelled",
    );
    invariant(result, "authentication_required", "Authentication required");
    try {
      const c = result.context;
      for (const field of [
        c.tenantId,
        c.projectId,
        c.actor.id,
        c.purpose,
        c.requestId,
      ])
        nonempty(field, "Authenticated context", 200);
      nonempty(c.now, "Authenticated time", 64);
      validTime(c.now);
      invariant(
        Array.isArray(c.actor.roles) &&
          c.actor.roles.length <= 128 &&
          c.actor.roles.every(
            (role) => typeof role === "string" && role.length <= 128,
          ),
        "authentication_unavailable",
        "Invalid roles",
      );
    } catch {
      throw new DomainError(
        "authentication_unavailable",
        "Invalid authenticated context",
      );
    }
    invariant(
      result.context.actor.kind === "human" &&
        result.context.actor.roles.includes("owner"),
      "manager_required",
      "Reconciliation required",
    );
    invariant(
      typeof result.csrfToken === "string" &&
        /^[A-Za-z0-9_-]{32,256}$/.test(result.csrfToken),
      "authentication_unavailable",
      "Invalid session binding",
    );
    invariant(
      !controller.signal.aborted && performance.now() < deadline,
      "authentication_unavailable",
      "Authentication deadline exceeded",
    );
    return result;
  } finally {
    if (timer) clearTimeout(timer);
    request.signal.removeEventListener("abort", abort);
    controller.signal.removeEventListener("abort", interrupt);
    controller.abort();
  }
}
async function jsonBody(request: Request, timeoutMs: number): Promise<unknown> {
  invariant(
    /^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(
      request.headers.get("content-type") ?? "",
    ) && [null, "identity"].includes(request.headers.get("content-encoding")),
    "unsupported_media",
    "Uncompressed JSON required",
  );
  const length = request.headers.get("content-length");
  invariant(
    length === null || (/^\d+$/.test(length) && Number(length) <= 16384),
    "body_too_large",
    "Request body exceeds budget",
  );
  invariant(request.body, "invalid_request", "Body required");
  const reader = request.body.getReader(),
    deadline = performance.now() + timeoutMs;
  function gate() {
    invariant(!request.signal.aborted, "request_aborted", "Request cancelled");
    invariant(
      performance.now() < deadline,
      "body_timeout",
      "Body deadline exceeded",
    );
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  let interrupt = () => {};
  const interrupted = new Promise<never>((_, reject) => {
    interrupt = () =>
      reject(new DomainError("request_aborted", "Request cancelled"));
    request.signal.addEventListener("abort", interrupt, { once: true });
    timer = setTimeout(
      () => reject(new DomainError("body_timeout", "Body deadline exceeded")),
      timeoutMs,
    );
  });
  try {
    invariant(!request.signal.aborted, "request_aborted", "Request cancelled");
    const chunks: Uint8Array[] = [];
    let bytes = 0,
      count = 0;
    while (true) {
      gate();
      const part = await Promise.race([reader.read(), interrupted]);
      gate();
      if (part.done) break;
      count++;
      bytes += part.value.byteLength;
      invariant(
        bytes <= 16384 && count <= 4096,
        "body_too_large",
        "Request body exceeds budget",
      );
      chunks.push(new Uint8Array(part.value));
    }
    let parsed: unknown;
    gate();
    try {
      parsed = JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)),
      );
    } catch {
      throw new DomainError("invalid_request", "UTF8 JSON required");
    }
    gate();
    return parsed;
  } finally {
    if (timer) clearTimeout(timer);
    request.signal.removeEventListener("abort", interrupt);
    // A hostile stream cancellation callback must not hold the handler open.
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** Private host-authenticated adapter only. No listening server, execution, cleanup or retry routes. */
export function createReconciliationHandler(
  jobs: TextProcessingJobs,
  outputs: PrivateTextOutputs,
  auth: ReconciliationAuthentication,
  configuration: ReconciliationHttpPolicy,
  clock: () => string,
): (request: Request) => Promise<Response> {
  const policy = structuredClone(configuration),
    authenticate = auth.authenticate.bind(auth);
  const origin = new URL(policy.origin);
  invariant(
    origin.protocol === "https:" &&
      origin.origin === policy.origin &&
      !origin.username &&
      !origin.password,
    "invalid_policy",
    "Exact HTTPS origin required",
  );
  for (const id of [policy.tenantId, policy.projectId, policy.purpose])
    nonempty(id, "Policy", 200);
  invariant(
    Array.isArray(policy.managerIds) &&
      policy.managerIds.length > 0 &&
      policy.managerIds.length <= 128 &&
      new Set(policy.managerIds).size === policy.managerIds.length &&
      typeof clock === "function",
    "invalid_policy",
    "Listed managers and clock required",
  );
  for (const id of policy.managerIds) nonempty(id, "Manager", 128);
  const authTimeout = policy.authenticationTimeoutMs ?? 5000,
    bodyTimeout = policy.bodyTimeoutMs ?? 5000,
    operationTimeout = policy.operationTimeoutMs ?? 10000;
  for (const timeout of [authTimeout, bodyTimeout, operationTimeout])
    invariant(
      Number.isSafeInteger(timeout) && timeout >= 10 && timeout <= 30000,
      "invalid_policy",
      "Finite bounded timeout required",
    );
  return async (request) => {
    try {
      const url = new URL(request.url),
        read = url.pathname === "/processing/session";
      invariant(
        url.origin === policy.origin && !url.username && !url.password,
        "origin_denied",
        "Origin denied",
      );
      invariant(
        read || url.pathname === "/processing/reconcile",
        "route_not_found",
        "Unknown route",
      );
      invariant(
        request.method === (read ? "GET" : "POST"),
        "method_denied",
        "Unsupported method",
      );
      invariant(
        !url.search && !url.hash,
        "invalid_request",
        "Query and fragment forbidden",
      );
      const origin = request.headers.get("origin"),
        site = request.headers.get("sec-fetch-site");
      invariant(
        (origin === null || origin === policy.origin) &&
          (site === null || site === "same-origin") &&
          (read
            ? origin === policy.origin || site === "same-origin"
            : origin === policy.origin),
        "origin_denied",
        "Same origin required",
      );
      // Bound and capture operator input BEFORE obtaining fresh host session authority.
      const parsed = read
        ? null
        : exact(await jsonBody(request, bodyTimeout), [
            "jobId",
            "consumedRevision",
            "output",
          ]);
      let action:
        | import("./text-reconciliation.ts").TextReconciliationRequest
        | null = null;
      if (parsed) {
        const consumedRevision = revision(parsed.consumedRevision);
        invariant(
          consumedRevision >= 3 &&
            ["recover-stored", "finalize-existing"].includes(
              parsed.output as string,
            ),
          "invalid_request",
          "Explicit consumed execution/action required",
        );
        action = {
          jobId: text(parsed.jobId),
          consumedRevision,
          output: parsed.output as "recover-stored" | "finalize-existing",
        };
      }
      const identity = await session({ authenticate }, request, authTimeout);
      requireScope(identity.context, policy);
      invariant(
        identity.context.purpose === policy.purpose,
        "purpose_denied",
        "Purpose denied",
      );
      invariant(
        policy.managerIds.includes(identity.context.actor.id),
        "manager_required",
        "Listed manager required",
      );
      invariant(
        !request.signal.aborted,
        "request_aborted",
        "Request cancelled",
      );
      if (read)
        return reply(200, {
          csrfToken: identity.csrfToken,
          actorId: identity.context.actor.id,
          tenantId: policy.tenantId,
          projectId: policy.projectId,
        });
      const csrf = request.headers.get("x-csrf-token") ?? "";
      invariant(
        /^[A-Za-z0-9_-]{32,256}$/.test(csrf) &&
          csrf.length === identity.csrfToken.length &&
          timingSafeEqual(Buffer.from(csrf), Buffer.from(identity.csrfToken)),
        "csrf_denied",
        "Session CSRF required",
      );
      const result = await reconcileTextOutput(
        jobs,
        outputs,
        identity.context,
        action!,
        clock,
        { timeoutMs: operationTimeout, signal: request.signal },
      );
      return reply(result.status === "completed" ? 200 : 503, result);
    } catch (error) {
      const code =
        error instanceof DomainError && Object.hasOwn(statusCodes, error.code)
          ? error.code
          : "operation_unavailable";
      return reply(statusCodes[code] ?? 503, { error: code });
    }
  };
}
