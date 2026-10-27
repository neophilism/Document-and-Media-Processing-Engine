import { scoped } from "@accountability/evidence/database";
import type { Database, UnitOfWork } from "@accountability/evidence/database";
import { PrivateObjectStore } from "@accountability/evidence/objects";
import type { ObjectReceipt } from "@accountability/evidence/objects";
import {
  DomainError,
  canonical,
  digest,
  hashBytes,
  invariant,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Json, Ref } from "./contracts.ts";
import { extractUtf8 } from "./index.ts";
import type { TextDerivative } from "./index.ts";
import type { TextWorkerServices } from "./text-worker.ts";
import type {
  TextJob,
  TextJobSnapshot,
  TextJobOutcome,
  TextJobPolicy,
} from "./text-jobs.ts";
import {
  INTENT,
  ARTIFACT,
  RECEIPT,
  textOutputId as idFor,
} from "./text-output-records.ts";
const json = (v: unknown): Json => JSON.parse(canonical(v));
function binding(j: TextJob) {
  return {
    id: j.id,
    source: j.source,
    sourceSha256: j.sourceSha256,
    sourceBytes: j.sourceBytes,
    language: j.language,
    purpose: j.purpose,
    profile: j.profile,
    workerId: j.workerId,
    workerKind: j.workerKind,
    consumedAt: j.consumedAt,
    leaseExpiresAt: j.leaseExpiresAt,
  };
}
type Metadata = Omit<TextDerivative, "text"> & {
  visibility: "private";
  byteLength: number;
};
type Intent = {
  jobId: string;
  consumedRevision: number;
  execution: string;
  metadata: Metadata;
  status: "pending" | "stored";
};
type Artifact = {
  visibility: "private";
  execution: string;
  metadata: Metadata;
  object: ObjectReceipt;
};
type Receipt = {
  visibility: "private";
  execution: string;
  jobId: string;
  consumedRevision: number;
  derivative: Ref;
  sha256: string;
  metadataDigest: string;
};
export type PrivateTextOutputOptions = TextJobPolicy &
  Readonly<{
    readSource: TextWorkerServices["readSource"];
    authorize(
      job: TextJob,
      context: Context,
      signal: AbortSignal,
    ): Promise<boolean>;
  }>;
/** Processing-owned database and private object root only. Context/clock/policy are trusted host dependencies. */
export class PrivateTextOutputs {
  readonly #db: Database;
  readonly #objects: PrivateObjectStore;
  readonly #policy: TextJobPolicy;
  readonly #source: TextWorkerServices["readSource"];
  readonly #authorize: PrivateTextOutputOptions["authorize"];
  readonly #clock: () => string;
  constructor(
    database: Database,
    objects: PrivateObjectStore,
    options: PrivateTextOutputOptions,
    clock: () => string,
  ) {
    invariant(
      typeof clock === "function" &&
        typeof options.authorize === "function" &&
        typeof options.readSource === "function",
      "invalid_policy",
      "Trusted output policy and clock required",
    );
    this.#db = database;
    this.#objects = objects;
    this.#authorize = options.authorize.bind(options);
    this.#clock = clock;
    this.#source = options.readSource.bind(options);
    this.#policy = {
      ...structuredClone({
        tenantId: options.tenantId,
        projectId: options.projectId,
        workerIds: options.workerIds,
        managerIds: options.managerIds,
        verificationTimeoutMs: options.verificationTimeoutMs ?? 5000,
      }),
    };
    const t = this.#policy.verificationTimeoutMs!;
    invariant(
      Number.isSafeInteger(t) && t >= 10 && t <= 30000,
      "invalid_policy",
      "Bounded output policy timeout required",
    );
  }
  #access(c: Context, j: TextJob, write = false) {
    requireScope(c, this.#policy);
    requireScope(c, j.source);
    validTime(c.now);
    invariant(
      c.purpose === j.purpose,
      "output_denied",
      "Output purpose differs",
    );
    const manager =
      c.actor.kind === "human" &&
      c.actor.roles.includes("owner") &&
      this.#policy.managerIds.includes(c.actor.id);
    const worker =
      ["service", "agent"].includes(c.actor.kind) &&
      c.actor.roles.includes("process") &&
      this.#policy.workerIds.includes(c.actor.id) &&
      c.actor.id === j.workerId &&
      c.actor.kind === j.workerKind;
    invariant(
      worker || (!write && manager),
      "output_denied",
      "Current listed output authority required",
    );
  }
  #live(c: Context, j: TextJob, s: AbortSignal) {
    invariant(!s.aborted, "output_denied", "Output operation stopped");
    const now = validTime(this.#clock());
    invariant(
      now >= validTime(c.now) && now < validTime(j.leaseExpiresAt!),
      "output_denied",
      "Output execution window expired",
    );
  }
  async #bounded<T>(
    signal: AbortSignal,
    callback: (s: AbortSignal) => Promise<T>,
  ): Promise<T> {
    invariant(!signal.aborted, "output_denied", "Output operation stopped");
    const controller = new AbortController(),
      deadline = performance.now() + this.#policy.verificationTimeoutMs!;
    const abort = () => controller.abort();
    signal.addEventListener("abort", abort, { once: true });
    let stop!: () => void;
    const stopped = new Promise<never>((_, reject) => {
      stop = () => reject(Error("Output verification stopped"));
    });
    void stopped.catch(() => {});
    controller.signal.addEventListener("abort", stop, { once: true });
    const timer = setTimeout(abort, this.#policy.verificationTimeoutMs!);
    try {
      const value = await Promise.race([
        Promise.resolve().then(() => {
          invariant(
            !controller.signal.aborted,
            "output_denied",
            "Output stopped",
          );
          return callback(controller.signal);
        }),
        stopped,
      ]);
      invariant(
        !controller.signal.aborted && performance.now() < deadline,
        "output_denied",
        "Output verification stopped",
      );
      return value;
    } finally {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      controller.signal.removeEventListener("abort", stop);
      controller.abort();
    }
  }
  #fresh(c: Context): Context {
    const now = this.#clock();
    invariant(
      validTime(now) >= validTime(c.now),
      "output_denied",
      "Trusted clock regressed",
    );
    return { ...structuredClone(c), now };
  }
  async #permit(c: Context, j: TextJob, signal: AbortSignal) {
    invariant(
      (await this.#bounded(signal, (s) =>
        this.#authorize(structuredClone(j), this.#fresh(c), s),
      )) === true,
      "output_denied",
      "Current output policy denied",
    );
  }
  async #content(
    c: Context,
    id: string,
    revision: number,
    metadata: Metadata,
    signal: AbortSignal,
  ) {
    const historical = await scoped(this.#db, c, (tx) =>
      tx.get<TextJob>("text-processing-job", digest({ id }), revision),
    );
    invariant(
      historical?.value.status === "consumed",
      "output_denied",
      "Exact consumed history required",
    );
    const j = historical.value;
    const input = await this.#bounded(signal, (s) =>
      this.#source({ job: structuredClone(j), revision }, this.#fresh(c), s),
    );
    invariant(
      input.bytes instanceof Uint8Array &&
        input.bytes.byteLength === j.sourceBytes &&
        input.bytes.byteLength <= 5 * 1024 * 1024 &&
        input.processingAllowed === true &&
        input.scanStatus === "clean" &&
        canonical(input.source) === canonical(j.source) &&
        input.sha256 === j.sourceSha256,
      "output_denied",
      "Current exact source verification required",
    );
    const derived = extractUtf8(
      this.#fresh(c),
      {
        ...input,
        source: structuredClone(j.source),
        bytes: Uint8Array.from(input.bytes),
      },
      j.language,
    );
    invariant(
      derived.sha256 === metadata.sha256 &&
        Buffer.byteLength(derived.text, "utf8") === metadata.byteLength &&
        derived.sourceSha256 === metadata.sourceSha256 &&
        canonical(derived.source) === canonical(metadata.source) &&
        derived.language === metadata.language &&
        derived.processor === metadata.processor &&
        derived.processorVersion === metadata.processorVersion,
      "output_denied",
      "Output differs from independent source extraction",
    );
  }
  async #job(c: Context, id: string, revision: number, write = false) {
    return scoped(this.#db, c, async (tx) => {
      const row = await tx.get<TextJob>("text-processing-job", digest({ id }));
      invariant(
        row &&
          row.value.id === id &&
          Number.isSafeInteger(revision) &&
          revision >= 3 &&
          row.revision ===
            revision + (row.value.status === "completed" ? 1 : 0) &&
          ["consumed", ...(write ? [] : ["completed"])].includes(
            row.value.status,
          ),
        "output_denied",
        "Exact consumed execution required",
      );
      this.#access(c, row.value, write);
      return row.value;
    });
  }
  #outcome(c: Context, id: string, sha256: string): TextJobOutcome {
    const ref = (kind: string) => ({
      tenantId: c.tenantId,
      projectId: c.projectId,
      kind,
      id,
      revision: 1,
    });
    return { derivative: ref(ARTIFACT), sha256, receipt: ref(RECEIPT) };
  }
  async storePrivateText(
    supplied: TextJobSnapshot,
    derived: TextDerivative,
    context: Context,
    signal: AbortSignal,
  ): Promise<TextJobOutcome> {
    const snap = structuredClone(supplied),
      c = structuredClone(context);
    invariant(
      typeof derived.text === "string" &&
        Buffer.byteLength(derived.text, "utf8") <= 5 * 1024 * 1024,
      "invalid_output",
      "Bounded text required",
    );
    const captured = structuredClone(derived);
    try {
      const j = await this.#job(c, snap.job.id, snap.revision, true);
      invariant(
        canonical(j) === canonical(snap.job),
        "output_denied",
        "Execution snapshot differs",
      );
      this.#live(c, j, signal);
      await this.#permit(c, j, signal);
      invariant(
        typeof captured.text === "string" &&
          Buffer.byteLength(captured.text, "utf8") <= 5 * 1024 * 1024,
        "invalid_output",
        "Bounded text required",
      );
      const d = captured,
        bytes = Buffer.from(d.text, "utf8");
      invariant(
        bytes.toString("utf8") === d.text &&
          !d.text.includes("\0") &&
          Object.keys(d).length === 9 &&
          [
            "source",
            "sourceSha256",
            "sha256",
            "text",
            "language",
            "processor",
            "processorVersion",
            "createdAt",
            "warnings",
          ].every((k) => Object.hasOwn(d, k)),
        "invalid_output",
        "Exact well-formed derivative required",
      );
      requireScope(c, d.source);
      validateRef(d.source);
      validTime(d.createdAt);
      invariant(
        validTime(d.createdAt) >= validTime(j.consumedAt!) &&
          validTime(d.createdAt) <= validTime(this.#clock()),
        "invalid_output",
        "Derivative creation time outside execution window",
      );
      invariant(
        canonical(d.source) === canonical(j.source) &&
          d.sourceSha256 === j.sourceSha256 &&
          d.language === j.language &&
          d.processor === "utf8-text" &&
          d.processorVersion === "1.0.0" &&
          hashBytes(bytes) === d.sha256 &&
          Array.isArray(d.warnings) &&
          d.warnings.length === 0,
        "invalid_output",
        "Derivative does not bind the accepted exact processor",
      );
      const { text: _, ...rest } = d;
      const metadata: Metadata = {
        ...rest,
        visibility: "private",
        byteLength: bytes.byteLength,
      };
      await this.#content(c, j.id, snap.revision, metadata, signal);
      const id = idFor(j.id, snap.revision),
        execution = digest(binding(j)),
        pending: Intent = {
          jobId: j.id,
          consumedRevision: snap.revision,
          execution,
          metadata,
          status: "pending",
        };
      await scoped(this.#db, c, async (tx) => {
        await tx.audit("processing.output.intent.checked", { id });
        const current = await tx.get<TextJob>(
          "text-processing-job",
          digest({ id: j.id }),
        );
        invariant(
          current?.revision === snap.revision &&
            canonical(current.value) === canonical(j),
          "output_denied",
          "Execution changed",
        );
        this.#live(c, current.value, signal);
        const old = await tx.get<Intent>(INTENT, id);
        if (old)
          invariant(
            canonical({ ...old.value, status: "pending" }) ===
              canonical(pending),
            "output_conflict",
            "Output intent differs",
          );
        else await tx.put(INTENT, id, 0, pending);
      });
      this.#live(c, j, signal);
      const object = await this.#objects.put(c, bytes);
      const persisted = await this.#objects.get(c, object);
      invariant(
        object.plaintextHash === d.sha256 &&
          object.size === bytes.byteLength &&
          hashBytes(persisted) === d.sha256,
        "output_denied",
        "Persisted private bytes differ",
      );
      this.#live(c, j, signal);
      await this.#permit(c, j, signal);
      return await scoped(this.#db, c, async (tx) => {
        await tx.audit("processing.output.commit.checked", { id });
        const current = await tx.get<TextJob>(
          "text-processing-job",
          digest({ id: j.id }),
        );
        invariant(
          current?.revision === snap.revision &&
            canonical(current.value) === canonical(j),
          "output_denied",
          "Execution changed",
        );
        this.#live(c, current.value, signal);
        return this.#publish(tx, c, pending, object);
      });
    } catch {
      throw new DomainError(
        "output_unavailable",
        "Private text output requires reconciliation",
      );
    }
  }
  async #publish(
    tx: UnitOfWork,
    c: Context,
    pending: Intent,
    object: ObjectReceipt,
  ): Promise<TextJobOutcome> {
    const { jobId, consumedRevision, execution, metadata } = pending;
    const id = idFor(jobId, consumedRevision);
    const intent = await tx.get<Intent>(INTENT, id);
    invariant(
      intent &&
        canonical({ ...intent.value, status: "pending" }) ===
          canonical(pending),
      "output_conflict",
      "Output intent changed",
    );
    const result = this.#outcome(c, id, metadata.sha256);
    if (intent.value.status === "stored") return result;
    await tx.put<Artifact>(ARTIFACT, id, 0, {
      visibility: "private",
      execution,
      metadata,
      object,
    });
    await tx.put<Receipt>(RECEIPT, id, 0, {
      visibility: "private",
      execution,
      jobId,
      consumedRevision,
      derivative: result.derivative,
      sha256: metadata.sha256,
      metadataDigest: digest(metadata),
    });
    await tx.put(INTENT, id, intent.revision, {
      ...intent.value,
      status: "stored",
    });
    await tx.enqueue(
      "processing.output.stored",
      json({
        jobId,
        consumedRevision,
        outcome: result,
      }),
      id,
    );
    return result;
  }
  /** Publish only already-existing verified bytes. This does not execute a processor or clear consumption. */
  async finalizePending(
    context: Context,
    jobId: string,
    consumedRevision: number,
    signal: AbortSignal,
  ): Promise<TextJobOutcome> {
    const c = structuredClone(context);
    try {
      const j = await this.#job(c, jobId, consumedRevision);
      invariant(
        c.actor.kind === "human" &&
          c.actor.roles.includes("owner") &&
          this.#policy.managerIds.includes(c.actor.id),
        "output_denied",
        "Current listed manager required",
      );
      await this.#permit(c, j, signal);
      const id = idFor(jobId, consumedRevision);
      const row = await scoped(this.#db, c, (tx) => tx.get<Intent>(INTENT, id));
      invariant(row, "output_denied", "Existing intent required");
      if (row.value.status === "stored")
        return await this.recoverOutcome(c, jobId, consumedRevision, signal);
      const pending = row.value;
      invariant(
        row.revision === 1 &&
          pending.status === "pending" &&
          pending.jobId === jobId &&
          pending.consumedRevision === consumedRevision &&
          pending.execution === digest(binding(j)) &&
          j.status === "consumed",
        "output_denied",
        "Exact pending execution required",
      );
      await this.#content(c, jobId, consumedRevision, pending.metadata, signal);
      // This candidate is not a receipt/proof until get authenticates the existing
      // envelope with the configured Processing root/key and exact expected bytes.
      const object: ObjectReceipt = {
        key: `${digest({ tenantId: c.tenantId, projectId: c.projectId })}/${pending.metadata.sha256}`,
        plaintextHash: pending.metadata.sha256,
        size: pending.metadata.byteLength,
        keyVersion: this.#objects.keyVersion,
      };
      const bytes = await this.#objects.get(c, object);
      invariant(
        bytes.byteLength === object.size &&
          hashBytes(bytes) === object.plaintextHash,
        "output_denied",
        "Existing private bytes differ",
      );
      await this.#permit(c, j, signal);
      return await scoped(this.#db, c, async (tx) => {
        // Acquire the same project lock as normal publication/completion before CAS.
        await tx.audit("processing.output.finalization.checked", { id });
        const current = await tx.get<TextJob>(
          "text-processing-job",
          digest({ id: jobId }),
        );
        invariant(
          current?.revision === consumedRevision &&
            canonical(current.value) === canonical(j),
          "output_denied",
          "Execution changed",
        );
        this.#access(c, current.value);
        this.#fresh(c);
        invariant(!signal.aborted, "output_denied", "Finalization stopped");
        return this.#publish(tx, c, pending, object);
      });
    } catch {
      throw new DomainError(
        "output_unavailable",
        "Pending private text output requires reconciliation",
      );
    }
  }
  async recoverOutcome(
    context: Context,
    jobId: string,
    consumedRevision: number,
    signal: AbortSignal,
  ): Promise<TextJobOutcome> {
    const c = structuredClone(context);
    try {
      const j = await this.#job(c, jobId, consumedRevision);
      await this.#permit(c, j, signal);
      const id = idFor(jobId, consumedRevision),
        execution = digest(binding(j));
      const rows = await scoped(this.#db, c, async (tx) => ({
        intent: await tx.get<Intent>(INTENT, id),
        artifact: await tx.get<Artifact>(ARTIFACT, id),
        receipt: await tx.get<Receipt>(RECEIPT, id),
      }));
      const a = rows.artifact,
        r = rows.receipt,
        i = rows.intent;
      invariant(
        a?.revision === 1 &&
          r?.revision === 1 &&
          i?.value.status === "stored" &&
          a.value.visibility === "private" &&
          r.value.visibility === "private" &&
          a.value.execution === execution &&
          r.value.execution === execution &&
          i.value.execution === execution &&
          r.value.jobId === jobId &&
          r.value.consumedRevision === consumedRevision &&
          digest(a.value.metadata) === r.value.metadataDigest &&
          canonical(i.value.metadata) === canonical(a.value.metadata),
        "output_denied",
        "Exact committed receipt required",
      );
      const result = this.#outcome(c, id, a.value.metadata.sha256);
      invariant(
        canonical(r.value.derivative) === canonical(result.derivative) &&
          r.value.sha256 === result.sha256,
        "output_denied",
        "Receipt edition differs",
      );
      await this.#content(c, jobId, consumedRevision, a.value.metadata, signal);
      const bytes = await this.#objects.get(c, a.value.object);
      invariant(
        bytes.byteLength === a.value.metadata.byteLength &&
          hashBytes(bytes) === result.sha256 &&
          a.value.object.plaintextHash === result.sha256,
        "output_denied",
        "Persisted output failed verification",
      );
      invariant(
        !signal.aborted,
        "output_denied",
        "Output verification stopped",
      );
      await this.#permit(c, j, signal);
      const current = await this.#job(c, jobId, consumedRevision);
      invariant(
        digest(binding(current)) === execution,
        "output_denied",
        "Execution changed",
      );
      return result;
    } catch {
      throw new DomainError(
        "output_unavailable",
        "Private text output unavailable",
      );
    }
  }
  async verifyOutcome(
    job: TextJob,
    outcome: TextJobOutcome,
    context: Context,
    signal: AbortSignal,
  ): Promise<boolean> {
    const j = structuredClone(job),
      o = structuredClone(outcome),
      c = structuredClone(context);
    try {
      requireScope(c, o.derivative);
      requireScope(c, o.receipt);
      const current = await scoped(this.#db, c, (tx) =>
        tx.get<TextJob>("text-processing-job", digest({ id: j.id })),
      );
      invariant(
        current && digest(binding(current.value)) === digest(binding(j)),
        "output_denied",
        "Execution differs",
      );
      const revision =
        current.revision - (current.value.status === "completed" ? 1 : 0);
      return (
        canonical(await this.recoverOutcome(c, j.id, revision, signal)) ===
        canonical(o)
      );
    } catch {
      return false;
    }
  }
}
