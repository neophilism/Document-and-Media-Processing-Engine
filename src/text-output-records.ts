import type { UnitOfWork } from "@accountability/evidence/database";
import { digest, invariant } from "./contracts.ts";

export const INTENT = "processing-output-intent";
export const ARTIFACT = "processing-private-text";
export const RECEIPT = "processing-output-receipt";
export const textOutputId = (jobId: string, consumedRevision: number) =>
  digest({ jobId, consumedRevision });

/** Caller holds the project lock. Presence denies; absence is never a liveness proof. */
export async function requireNoOwnedTextOutput(
  tx: UnitOfWork,
  jobId: string,
  consumedRevision: number,
) {
  const id = textOutputId(jobId, consumedRevision);
  for (const kind of [INTENT, ARTIFACT, RECEIPT]) {
    invariant(
      !(await tx.get(kind, id)),
      "output_present",
      "Known private output requires reconciliation, not no-output settlement or retry",
    );
  }
}
