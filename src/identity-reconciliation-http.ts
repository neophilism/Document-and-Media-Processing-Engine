import type { MembershipRegistry } from "@accountability/identity-policy/membership";
import { createMembershipSessionAuthentication } from "@accountability/identity-policy/membership-session";
import type { HumanSessionProvider } from "@accountability/identity-policy/membership-session";
import { createReconciliationHandler } from "./reconciliation-http.ts";
import type { ReconciliationHttpPolicy } from "./reconciliation-http.ts";
import type { TextProcessingJobs } from "./text-jobs.ts";
import type { PrivateTextOutputs } from "./text-outputs.ts";

/** Opt-in composition; Identity and Processing retain separate owning databases. */
export function createIdentityReconciliationHandler(
  jobs: TextProcessingJobs,
  outputs: PrivateTextOutputs,
  membership: MembershipRegistry,
  sessions: HumanSessionProvider,
  configuration: ReconciliationHttpPolicy,
  clock: () => string,
): (request: Request) => Promise<Response> {
  const policy = structuredClone(configuration);
  const authentication = createMembershipSessionAuthentication(
    membership,
    sessions,
    {
      tenantId: policy.tenantId,
      projectId: policy.projectId,
      origin: policy.origin,
      purpose: policy.purpose,
      action: "admin",
      ...(policy.authenticationTimeoutMs === undefined
        ? {}
        : { timeoutMs: policy.authenticationTimeoutMs }),
    },
    clock,
  );
  return createReconciliationHandler(
    jobs,
    outputs,
    authentication,
    policy,
    clock,
  );
}
