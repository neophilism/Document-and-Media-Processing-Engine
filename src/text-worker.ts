import { extractUtf8 } from "./index.ts";
import type { ProcessingInput, TextDerivative } from "./index.ts";
import {
  digest,
  invariant,
  refKey,
  requireScope,
  validTime,
} from "./contracts.ts";
import type { Context } from "./contracts.ts";
import { TextProcessingJobs } from "./text-jobs.ts";
import type { TextJobOutcome, TextJobSnapshot } from "./text-jobs.ts";
export type TextWorkerServices = Readonly<{
  readSource(
    job: TextJobSnapshot,
    context: Context,
    signal: AbortSignal,
  ): Promise<ProcessingInput>;
  storePrivateText(
    job: TextJobSnapshot,
    derivative: TextDerivative,
    context: Context,
    signal: AbortSignal,
  ): Promise<TextJobOutcome>;
}>;
export type TextWorkerResult =
  | { status: "completed"; snapshot: TextJobSnapshot }
  | {
      status: "reconciliation-required";
      phase: "consumption" | "source" | "processing" | "storage" | "completion";
      consumedRevision: number | null;
    };
/** Runs once from a live lease. Read snapshots or uncertain acknowledgements never trigger I/O. */
export async function runLeasedTextJob(
  jobs: TextProcessingJobs,
  context: Context,
  id: string,
  leaseRevision: number,
  services: TextWorkerServices,
  clock: () => string,
  options: Readonly<{ timeoutMs?: number; signal?: AbortSignal }> = {},
): Promise<TextWorkerResult> {
  const base = structuredClone(context),
    timeout = options.timeoutMs ?? 10000,
    external = options.signal;
  invariant(
    typeof clock === "function" &&
      Number.isSafeInteger(timeout) &&
      timeout >= 10 &&
      timeout <= 30000,
    "invalid_worker",
    "Trusted clock and finite worker deadline required",
  );
  const read = services.readSource.bind(services),
    store = services.storePrivateText.bind(services);
  let sampled = validTime(base.now);
  const controller = new AbortController(),
    deadline = performance.now() + timeout;
  const abort = () => controller.abort();
  external?.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(abort, timeout);
  if (external?.aborted) abort();
  let stop!: () => void;
  const stopped = new Promise<never>((_, reject) => {
    stop = () => reject(Error("Worker stopped"));
  });
  void stopped.catch(() => {});
  controller.signal.addEventListener("abort", stop, { once: true });
  let consumed: TextJobSnapshot | null = null,
    phase: "consumption" | "source" | "processing" | "storage" | "completion" =
      "consumption";
  function current(): Context {
    invariant(
      !controller.signal.aborted && performance.now() < deadline,
      "worker_stopped",
      "Worker stopped",
    );
    const now = clock(),
      time = validTime(now);
    invariant(time >= sampled, "worker_stopped", "Trusted clock regressed");
    sampled = time;
    if (consumed)
      invariant(
        time < validTime(consumed.job.leaseExpiresAt!),
        "worker_stopped",
        "Execution lease window expired",
      );
    return { ...structuredClone(base), now };
  }
  async function wait<T>(p: Promise<T>): Promise<T> {
    const value = await Promise.race([p, stopped]);
    current();
    return value;
  }
  try {
    // Database operations are awaited, never raced: a timed-out acknowledgement may
    // have committed. Only this successful consume return can authorize source I/O.
    consumed = await jobs.consume(current(), id, leaseRevision);
    phase = "source";
    const sourceContext = current();
    const supplied = await wait(
      Promise.resolve().then(() => {
        current();
        return read(
          structuredClone(consumed!),
          sourceContext,
          controller.signal,
        );
      }),
    );
    phase = "processing";
    current();
    invariant(
      supplied.bytes instanceof Uint8Array &&
        supplied.bytes.byteLength === consumed.job.sourceBytes &&
        supplied.bytes.byteLength <= 5 * 1024 * 1024,
      "invalid_source",
      "Exact source byte budget required",
    );
    requireScope(base, supplied.source);
    invariant(
      refKey(supplied.source) === refKey(consumed.job.source) &&
        supplied.sha256 === consumed.job.sourceSha256 &&
        supplied.processingAllowed === true &&
        supplied.scanStatus === "clean",
      "invalid_source",
      "Current clean authorized exact source required",
    );
    const input: ProcessingInput = {
      source: structuredClone(consumed.job.source),
      sha256: supplied.sha256,
      bytes: Uint8Array.from(supplied.bytes),
      scanStatus: "clean",
      processingAllowed: true,
    };
    const derivative = extractUtf8(current(), input, consumed.job.language);
    phase = "storage";
    const storeContext = current();
    const outcome = structuredClone(
      await wait(
        Promise.resolve().then(() => {
          current();
          return store(
            structuredClone(consumed!),
            structuredClone(derivative),
            storeContext,
            controller.signal,
          );
        }),
      ),
    );
    invariant(
      outcome.sha256 === derivative.sha256,
      "invalid_outcome",
      "Private UTF8 output hash must match extracted text",
    );
    phase = "completion";
    const result = await jobs.complete(
      current(),
      id,
      consumed.revision,
      outcome,
      `text-worker-${digest({ id, revision: consumed.revision })}`,
    );
    // Acknowledged completion is durable even if the deadline elapsed during SQL.
    return { status: "completed", snapshot: result };
  } catch {
    return {
      status: "reconciliation-required",
      phase,
      consumedRevision: consumed?.revision ?? null,
    };
  } finally {
    clearTimeout(timer);
    external?.removeEventListener("abort", abort);
    controller.signal.removeEventListener("abort", stop);
    controller.abort();
  }
}
