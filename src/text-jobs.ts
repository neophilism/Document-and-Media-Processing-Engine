import { scoped } from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import {
  DomainError,
  canonical,
  digest,
  invariant,
  nonempty,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Json, Ref, Scope } from "./contracts.ts";
export type TextJobInput = Readonly<{
  id: string;
  source: Ref;
  sourceSha256: string;
  sourceBytes: number;
  language: string | null;
}>;
export type TextJobOutcome = Readonly<{
  derivative: Ref;
  sha256: string;
  receipt: Ref;
}>;
export type TextJob = TextJobInput &
  Readonly<{
    purpose: string;
    profile: "utf8-text@1.0.0";
    status: "queued" | "leased" | "consumed" | "completed" | "cancelled";
    workerId: string | null;
    workerKind: string | null;
    leaseExpiresAt: string | null;
    outcome: TextJobOutcome | null;
    submittedAt: string;
    submittedBy: string;
    consumedAt: string | null;
  }>;
export type TextJobSnapshot = Readonly<{ job: TextJob; revision: number }>;
export type TextJobPolicy = Scope &
  Readonly<{
    managerIds: readonly string[];
    workerIds: readonly string[];
    verificationTimeoutMs?: number;
  }>;
export type TextJobVerification = Readonly<{
  authorizeStart(
    job: TextJob,
    context: Context,
    signal: AbortSignal,
  ): Promise<boolean>;
  verifyOutcome(
    job: TextJob,
    outcome: TextJobOutcome,
    context: Context,
    signal: AbortSignal,
  ): Promise<boolean>;
}>;
const json = (value: unknown): Json => JSON.parse(canonical(value)) as Json;
const key = (id: string) => {
  nonempty(id, "Job id", 128);
  return digest({ id });
};
function sha(value: string) {
  invariant(
    typeof value === "string" && /^[a-f0-9]{64}$/.test(value),
    "invalid_digest",
    "Exact SHA256 required",
  );
}
async function verify(
  timeout: number,
  callback: (signal: AbortSignal) => Promise<boolean>,
) {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      Promise.resolve()
        .then(() => callback(controller.signal))
        .catch(() => {
          throw new DomainError(
            "verification_unavailable",
            "Trusted verification failed",
          );
        }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(
            new DomainError(
              "verification_timeout",
              "Trusted verification timed out",
            ),
          );
        }, timeout);
      }),
    ]);
    invariant(
      result === true,
      "verification_denied",
      "Trusted verification did not authorize this exact operation",
    );
  } finally {
    if (timer) clearTimeout(timer);
  }
}
/** A dedicated processing database is required. This ledger does not read bytes or run a processor. */
export class TextProcessingJobs {
  readonly #db: Database;
  readonly #policy: TextJobPolicy;
  readonly #checks: TextJobVerification;
  readonly #timeout: number;
  readonly #clock: () => string;
  constructor(
    db: Database,
    policy: TextJobPolicy,
    verification: TextJobVerification,
    clock: () => string,
  ) {
    invariant(
      typeof clock === "function",
      "invalid_policy",
      "A trusted live clock is required",
    );
    this.#clock = clock;
    this.#db = db;
    this.#policy = structuredClone(policy);
    this.#checks = {
      authorizeStart: verification.authorizeStart.bind(verification),
      verifyOutcome: verification.verifyOutcome.bind(verification),
    };
    this.#timeout = policy.verificationTimeoutMs ?? 5000;
    invariant(
      Number.isSafeInteger(this.#timeout) &&
        this.#timeout >= 10 &&
        this.#timeout <= 30000,
      "invalid_policy",
      "Finite verification timeout required",
    );
  }
  #manager(c: Context) {
    requireScope(c, this.#policy);
    invariant(
      c.actor.kind === "human" &&
        c.actor.roles.includes("owner") &&
        this.#policy.managerIds.includes(c.actor.id),
      "manager_required",
      "Listed human manager required",
    );
    validTime(c.now);
  }
  #worker(c: Context) {
    requireScope(c, this.#policy);
    invariant(
      ["agent", "service"].includes(c.actor.kind) &&
        c.actor.roles.includes("process") &&
        this.#policy.workerIds.includes(c.actor.id),
      "worker_required",
      "Listed processing worker required",
    );
    validTime(c.now);
  }
  #owned(c: Context, j: TextJob) {
    invariant(
      j.purpose === c.purpose &&
        j.workerId === c.actor.id &&
        j.workerKind === c.actor.kind,
      "lease_denied",
      "Worker and purpose must match the lease",
    );
  }
  async read(context: Context, id: string): Promise<TextJobSnapshot> {
    const c = structuredClone(context);
    requireScope(c, this.#policy);
    if (c.actor.kind === "human") this.#manager(c);
    else this.#worker(c);
    return scoped(this.#db, c, async (tx) => {
      const row = await tx.get<TextJob>("text-processing-job", key(id));
      invariant(row, "not_found", "Job unavailable");
      invariant(
        row.value.purpose === c.purpose,
        "purpose_denied",
        "Job purpose differs",
      );
      return { job: row.value, revision: row.revision };
    });
  }
  async submit(
    context: Context,
    supplied: TextJobInput,
    commandId: string,
  ): Promise<TextJobSnapshot> {
    const c = structuredClone(context),
      input = structuredClone(supplied);
    this.#manager(c);
    key(input.id);
    validateRef(input.source);
    requireScope(c, input.source);
    sha(input.sourceSha256);
    nonempty(c.purpose, "Purpose", 200);
    invariant(
      Object.keys(input).length === 5 &&
        ["id", "source", "sourceSha256", "sourceBytes", "language"].every((k) =>
          Object.hasOwn(input, k),
        ),
      "invalid_job",
      "Exact job fields required",
    );
    invariant(
      Number.isSafeInteger(input.sourceBytes) &&
        input.sourceBytes >= 0 &&
        input.sourceBytes <= 5 * 1024 * 1024,
      "invalid_job",
      "Text bytes must fit the accepted processor budget",
    );
    if (input.language !== null) nonempty(input.language, "Language", 64);
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        json({ operation: "submit-text", purpose: c.purpose, input }),
        async () => {
          const job: TextJob = {
            ...input,
            purpose: c.purpose,
            profile: "utf8-text@1.0.0",
            status: "queued",
            workerId: null,
            workerKind: null,
            leaseExpiresAt: null,
            outcome: null,
            submittedAt: c.now,
            submittedBy: c.actor.id,
            consumedAt: null,
          };
          const row = await tx.put(
            "text-processing-job",
            key(input.id),
            0,
            job,
          );
          await tx.enqueue(
            "processing.queued",
            json({ jobId: input.id, revision: row.revision }),
            digest({ id: input.id, event: "queued" }),
          );
          return json({ job, revision: row.revision });
        },
      ),
    ) as Promise<unknown> as Promise<TextJobSnapshot>;
  }
  async lease(
    context: Context,
    id: string,
    expectedRevision: number,
    leaseMs = 60000,
  ): Promise<TextJobSnapshot> {
    const c = structuredClone(context);
    this.#worker(c);
    invariant(
      Number.isSafeInteger(leaseMs) && leaseMs >= 1000 && leaseMs <= 300000,
      "invalid_lease",
      "Lease must be 1–300 seconds",
    );
    return scoped(this.#db, c, async (tx) => {
      const row = await tx.get<TextJob>("text-processing-job", key(id));
      invariant(
        row && row.revision === expectedRevision,
        "revision_conflict",
        "Job changed",
      );
      const j = row.value;
      invariant(
        j.purpose === c.purpose,
        "purpose_denied",
        "Job purpose differs",
      );
      invariant(
        j.status === "queued" ||
          (j.status === "leased" &&
            validTime(j.leaseExpiresAt!) <= validTime(c.now)),
        "job_unavailable",
        "Only queued or expired unconsumed work can be leased",
      );
      const job: TextJob = {
        ...j,
        status: "leased",
        workerId: c.actor.id,
        workerKind: c.actor.kind,
        leaseExpiresAt: new Date(validTime(c.now) + leaseMs).toISOString(),
      };
      const saved = await tx.put(
        "text-processing-job",
        key(id),
        expectedRevision,
        job,
      );
      return { job, revision: saved.revision };
    });
  }
  async consume(
    context: Context,
    id: string,
    expectedRevision: number,
  ): Promise<TextJobSnapshot> {
    const c = structuredClone(context);
    this.#worker(c);
    const before = await this.read(c, id);
    this.#owned(c, before.job);
    invariant(
      before.revision === expectedRevision &&
        before.job.status === "leased" &&
        validTime(before.job.leaseExpiresAt!) > validTime(c.now),
      "lease_denied",
      "Current unexpired lease required",
    );
    await verify(this.#timeout, (signal) =>
      this.#checks.authorizeStart(
        structuredClone(before.job),
        structuredClone(c),
        signal,
      ),
    );
    return scoped(this.#db, c, async (tx) => {
      // audit() acquires the SDK project lock; sample time only after storage serialization.
      // The attempt audit rolls back with any rejected consumption.
      await tx.audit("processing.consumption.checked", {
        jobId: id,
        expectedRevision,
      });
      const row = await tx.get<TextJob>("text-processing-job", key(id));
      invariant(
        row &&
          row.revision === expectedRevision &&
          row.value.status === "leased",
        "revision_conflict",
        "Job changed during authorization",
      );
      this.#owned(c, row.value);
      const consumedAt = this.#clock();
      invariant(
        validTime(consumedAt) >= validTime(c.now) &&
          validTime(row.value.leaseExpiresAt!) > validTime(consumedAt),
        "lease_denied",
        "Lease expired while authorization or storage was pending",
      );
      const job: TextJob = {
        ...row.value,
        status: "consumed",
        consumedAt,
      };
      const saved = await tx.put(
        "text-processing-job",
        key(id),
        expectedRevision,
        job,
      );
      await tx.enqueue(
        "processing.consumed",
        json({ jobId: id, revision: saved.revision }),
        digest({ id, event: "consumed" }),
      );
      return { job, revision: saved.revision };
    });
  }
  async complete(
    context: Context,
    id: string,
    expectedRevision: number,
    supplied: TextJobOutcome,
    commandId: string,
  ): Promise<TextJobSnapshot> {
    const c = structuredClone(context),
      outcome = structuredClone(supplied);
    requireScope(c, this.#policy);
    if (c.actor.kind === "human") this.#manager(c);
    else this.#worker(c);
    invariant(
      Object.keys(outcome).length === 3 &&
        ["derivative", "sha256", "receipt"].every((k) =>
          Object.hasOwn(outcome, k),
        ),
      "invalid_outcome",
      "Exact outcome fields required",
    );
    for (const ref of [outcome.derivative, outcome.receipt]) {
      validateRef(ref);
      requireScope(c, ref);
    }
    sha(outcome.sha256);
    const before = await this.read(c, id);
    if (c.actor.kind !== "human") this.#owned(c, before.job);
    invariant(
      before.job.status === "consumed" || before.job.status === "completed",
      "job_unavailable",
      "Consumed job required",
    );
    await verify(this.#timeout, (signal) =>
      this.#checks.verifyOutcome(
        structuredClone(before.job),
        structuredClone(outcome),
        structuredClone(c),
        signal,
      ),
    );
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        json({
          operation: "complete-text",
          id,
          expectedRevision,
          outcome,
          purpose: c.purpose,
          actorKind: c.actor.kind,
        }),
        async () => {
          const row = await tx.get<TextJob>("text-processing-job", key(id));
          invariant(
            row &&
              row.revision === expectedRevision &&
              row.value.status === "consumed",
            "revision_conflict",
            "Consumed job changed",
          );
          if (c.actor.kind !== "human") this.#owned(c, row.value);
          const job: TextJob = { ...row.value, status: "completed", outcome };
          const saved = await tx.put(
            "text-processing-job",
            key(id),
            expectedRevision,
            job,
          );
          await tx.enqueue(
            "processing.completed",
            json({ jobId: id, revision: saved.revision, outcome }),
            digest({ id, event: "completed" }),
          );
          return json({ job, revision: saved.revision });
        },
      ),
    ) as Promise<unknown> as Promise<TextJobSnapshot>;
  }
  async cancel(
    context: Context,
    id: string,
    expectedRevision: number,
  ): Promise<TextJobSnapshot> {
    const c = structuredClone(context);
    this.#manager(c);
    return scoped(this.#db, c, async (tx) => {
      const row = await tx.get<TextJob>("text-processing-job", key(id));
      invariant(
        row && row.revision === expectedRevision,
        "revision_conflict",
        "Job changed",
      );
      invariant(
        row.value.purpose === c.purpose,
        "purpose_denied",
        "Job purpose differs",
      );
      invariant(
        ["queued", "leased"].includes(row.value.status),
        "job_unavailable",
        "Consumed work cannot be recalled",
      );
      const job: TextJob = { ...row.value, status: "cancelled" };
      const saved = await tx.put(
        "text-processing-job",
        key(id),
        expectedRevision,
        job,
      );
      return { job, revision: saved.revision };
    });
  }
}
