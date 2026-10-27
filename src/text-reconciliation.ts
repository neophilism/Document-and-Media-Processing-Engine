import {
  canonical,
  digest,
  invariant,
  nonempty,
  validTime,
} from "./contracts.ts";
import type { Context } from "./contracts.ts";
import { TextProcessingJobs } from "./text-jobs.ts";
import type { TextJobOutcome, TextJobSnapshot } from "./text-jobs.ts";
import { PrivateTextOutputs } from "./text-outputs.ts";

export type TextReconciliationRequest = Readonly<{
  jobId: string;
  consumedRevision: number;
  output: "recover-stored" | "finalize-existing";
}>;
export type TextReconciliationResult =
  | {
      status: "completed";
      jobId: string;
      revision: number;
      outcome: TextJobOutcome;
    }
  | {
      status: "reconciliation-required";
      phase: "inspection" | "output" | "completion";
    };

/** Explicit trusted-host manager operation. Never consumes, executes, writes bytes or retries a job. */
export async function reconcileTextOutput(
  jobs: TextProcessingJobs,
  outputs: PrivateTextOutputs,
  context: Context,
  supplied: TextReconciliationRequest,
  clock: () => string,
  options: Readonly<{ timeoutMs?: number; signal?: AbortSignal }> = {},
): Promise<TextReconciliationResult> {
  const base = structuredClone(context),
    request = structuredClone(supplied),
    timeout = options.timeoutMs ?? 10000,
    external = options.signal;
  invariant(
    Object.keys(request).length === 3 &&
      ["jobId", "consumedRevision", "output"].every((k) =>
        Object.hasOwn(request, k),
      ) &&
      Number.isSafeInteger(request.consumedRevision) &&
      request.consumedRevision >= 3 &&
      ["recover-stored", "finalize-existing"].includes(request.output),
    "invalid_reconciliation",
    "Exact execution and explicit output action required",
  );
  nonempty(request.jobId, "Job id", 128);
  invariant(
    base.actor.kind === "human" &&
      base.actor.roles.includes("owner") &&
      typeof clock === "function" &&
      Number.isSafeInteger(timeout) &&
      timeout >= 10 &&
      timeout <= 30000,
    "invalid_reconciliation",
    "Trusted manager context, clock and finite deadline required",
  );
  // Capture receiver-bound methods before any asynchronous host work.
  const read = jobs.read.bind(jobs),
    complete = jobs.complete.bind(jobs),
    recover = outputs.recoverOutcome.bind(outputs),
    finalize = outputs.finalizePending.bind(outputs),
    verify = outputs.verifyOutcome.bind(outputs);
  let sampled = validTime(base.now);
  const controller = new AbortController(),
    deadline = performance.now() + timeout;
  const abort = () => controller.abort();
  external?.addEventListener("abort", abort, { once: true });
  if (external?.aborted) abort();
  const timer = setTimeout(abort, timeout);
  let phase: "inspection" | "output" | "completion" = "inspection";
  function current(): Context {
    invariant(
      !controller.signal.aborted && performance.now() < deadline,
      "reconciliation_stopped",
      "Reconciliation stopped",
    );
    const now = clock(),
      time = validTime(now);
    invariant(
      time >= sampled,
      "reconciliation_stopped",
      "Trusted clock regressed",
    );
    sampled = time;
    return { ...structuredClone(base), now };
  }
  function exact(snapshot: TextJobSnapshot) {
    invariant(
      snapshot.job.id === request.jobId &&
        (snapshot.job.status === "consumed" ||
          snapshot.job.status === "completed") &&
        snapshot.revision ===
          request.consumedRevision +
            (snapshot.job.status === "completed" ? 1 : 0),
      "reconciliation_changed",
      "Exact consumed execution required",
    );
  }
  try {
    // Reads perform owning-ledger listed-manager/scope/purpose admission.
    const before = await read(current(), request.jobId);
    exact(before);
    phase = "output";
    const outcome = await (
      request.output === "recover-stored" ? recover : finalize
    )(current(), request.jobId, request.consumedRevision, controller.signal);
    current();
    // Bind the output service's actual execution to this ledger's snapshot too.
    // Similar job IDs in two host-configured databases are not the same execution.
    invariant(
      (await verify(before.job, outcome, current(), controller.signal)) ===
        true,
      "reconciliation_denied",
      "Exact current private output proof required",
    );
    phase = "completion";
    const latest = await read(current(), request.jobId);
    exact(latest);
    if (latest.job.status === "completed") {
      invariant(
        canonical(latest.job.outcome) === canonical(outcome),
        "reconciliation_changed",
        "Completed outcome differs",
      );
      current();
      return {
        status: "completed",
        jobId: request.jobId,
        revision: latest.revision,
        outcome,
      };
    }
    invariant(
      canonical(latest) === canonical(before),
      "reconciliation_changed",
      "Execution changed while output was verified",
    );
    // Begun SQL is awaited, never raced. An error may hide a committed completion.
    const done = await complete(
      current(),
      request.jobId,
      request.consumedRevision,
      outcome,
      `text-reconciliation-${digest({ id: request.jobId, consumedRevision: request.consumedRevision })}`,
    );
    // An acknowledged commit stays completed even if cancellation arrived during SQL.
    return {
      status: "completed",
      jobId: request.jobId,
      revision: done.revision,
      outcome: done.job.outcome!,
    };
  } catch {
    // Phase is diagnostic only: it never certifies rollback, absent output or liveness.
    return { status: "reconciliation-required", phase };
  } finally {
    clearTimeout(timer);
    external?.removeEventListener("abort", abort);
    controller.abort();
  }
}
