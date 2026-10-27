import type { EvidenceService } from "@accountability/evidence-service-sdk/service";
import type { EvidenceRef } from "@accountability/evidence-service-sdk";
import {
  DomainError,
  hashBytes,
  invariant,
  refKey,
  requireScope,
  validateRef,
} from "./contracts.ts";
import type { Context } from "./contracts.ts";
import type { TextJobSnapshot } from "./text-jobs.ts";
import type { TextWorkerServices } from "./text-worker.ts";
export type EvidenceSourceOptions = Readonly<{
  credential(context: Context, signal: AbortSignal): Promise<string>;
  authorizeProcessing(
    job: TextJobSnapshot,
    context: Context,
    signal: AbortSignal,
  ): Promise<boolean>;
  timeoutMs?: number;
}>;
/** Uses an authenticated Evidence facade; never accesses its database or object root. */
export function createEvidenceTextSource(
  service: Pick<EvidenceService, "inspect" | "read">,
  supplied: EvidenceSourceOptions,
): TextWorkerServices["readSource"] {
  const inspect = service.inspect.bind(service),
    read = service.read.bind(service);
  const credential = supplied.credential.bind(supplied),
    authorize = supplied.authorizeProcessing.bind(supplied);
  const timeout = supplied.timeoutMs ?? 5000;
  invariant(
    Number.isSafeInteger(timeout) && timeout >= 10 && timeout <= 30000,
    "invalid_source_policy",
    "Finite Evidence source timeout required",
  );
  return async (snapshot, context, external) => {
    const job = structuredClone(snapshot),
      c = structuredClone(context),
      controller = new AbortController(),
      deadline = performance.now() + timeout;
    const abort = () => controller.abort();
    let rejectStopped!: () => void;
    const stopped = new Promise<never>((_, reject) => {
      rejectStopped = () => reject(Error("Source stopped"));
    });
    void stopped.catch(() => {});
    controller.signal.addEventListener("abort", rejectStopped, { once: true });
    external.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(abort, timeout);
    if (external.aborted) abort();
    function active() {
      invariant(
        !controller.signal.aborted && performance.now() < deadline,
        "source_unavailable",
        "Evidence source unavailable",
      );
    }
    async function step<T>(fn: () => Promise<T>): Promise<T> {
      active();
      const value = await Promise.race([
        Promise.resolve().then(() => {
          active();
          return fn();
        }),
        stopped,
      ]);
      active();
      return value;
    }
    try {
      requireScope(c, job.job.source);
      validateRef(job.job.source);
      invariant(
        Object.keys(job.job.source).length === 5 &&
          Object.keys(job.job.source).every((k) =>
            ["tenantId", "projectId", "kind", "id", "revision"].includes(k),
          ),
        "source_unavailable",
        "Exact source reference required",
      );
      invariant(
        job.job.purpose === c.purpose &&
          job.job.source.kind === "evidence" &&
          job.job.profile === "utf8-text@1.0.0" &&
          job.job.status === "consumed" &&
          Number.isSafeInteger(job.revision) &&
          job.revision >= 3 &&
          Number.isSafeInteger(job.job.sourceBytes) &&
          job.job.sourceBytes >= 0 &&
          job.job.sourceBytes <= 5 * 1024 * 1024,
        "source_unavailable",
        "Exact consumed UTF8 evidence job required",
      );
      invariant(
        (await step(() =>
          authorize(
            structuredClone(job),
            structuredClone(c),
            controller.signal,
          ),
        )) === true,
        "source_unavailable",
        "Current processing authority required",
      );
      const token = await step(() =>
        credential(structuredClone(c), controller.signal),
      );
      invariant(
        typeof token === "string" && token.length >= 32 && token.length <= 512,
        "source_unavailable",
        "Managed credential unavailable",
      );
      const request = { token, purpose: c.purpose, requestId: c.requestId };
      const ref = structuredClone(job.job.source) as EvidenceRef;
      function admitted(
        value: Awaited<ReturnType<EvidenceService["inspect"]>>,
      ) {
        validateRef(value.version.ref);
        requireScope(c, value.version.ref);
        invariant(
          value.scan === "clean" &&
            refKey(value.version.ref) === refKey(ref) &&
            value.version.sha256 === job.job.sourceSha256 &&
            value.version.byteLength === job.job.sourceBytes &&
            value.version.mediaType === "text/plain" &&
            value.version.allowedPurposes.includes(c.purpose) &&
            Number.isSafeInteger(value.revision) &&
            value.revision > 0,
          "source_unavailable",
          "Exact current clean plaintext evidence required",
        );
      }
      const before = structuredClone(
        await step(() => inspect({ ...request }, structuredClone(ref))),
      );
      admitted(before);
      const raw = await step(() => read({ ...request }, structuredClone(ref)));
      invariant(
        raw instanceof Uint8Array &&
          raw.byteLength === job.job.sourceBytes &&
          raw.byteLength <= 5 * 1024 * 1024,
        "source_unavailable",
        "Exact source byte budget required",
      );
      const bytes = Uint8Array.from(raw);
      invariant(
        hashBytes(bytes) === job.job.sourceSha256,
        "source_unavailable",
        "Exact source digest required",
      );
      const after = structuredClone(
        await step(() => inspect({ ...request }, structuredClone(ref))),
      );
      admitted(after);
      invariant(
        after.revision === before.revision,
        "source_unavailable",
        "Evidence aggregate changed while reading",
      );
      invariant(
        (await step(() =>
          authorize(
            structuredClone(job),
            structuredClone(c),
            controller.signal,
          ),
        )) === true,
        "source_unavailable",
        "Current processing authority required",
      );
      active();
      return {
        source: structuredClone(job.job.source),
        sha256: job.job.sourceSha256,
        bytes,
        scanStatus: "clean",
        processingAllowed: true,
      };
    } catch {
      throw new DomainError(
        "source_unavailable",
        "Evidence source unavailable",
      );
    } finally {
      clearTimeout(timer);
      external.removeEventListener("abort", abort);
      controller.signal.removeEventListener("abort", rejectStopped);
      controller.abort();
    }
  };
}
