# Phase 3 Slice 5 controlled execution plan — v3

Status: local review artifact only. It does not authorize a production action.
This amendment supersedes the v2 execution ordering where they conflict; the
original v2 evidence and artifacts remain immutable historical records.

## Corrected authority model

The historical manifest-v1 generation and its staged rows are immutable. A
future manifest-v2 generation is the only deletion-authoritative candidate.
Promotion may change the historical activation only through its expected
atomic retirement fields; it does not rewrite the historical generation.

Stage 0 is completed read-only evidence. It proves only the state at its
timestamp. It neither reserves work nor establishes an exclusive operator
window. Inactivity, an empty trigger inventory, and operator intent do not prove
that unknown external automation is absent.

The public, authenticated organization-admin enqueue endpoint creates the
controlled job together with one `connector_sync_control_reservations` row. An
authenticated organization admin supplies a fresh reservation UUID and a
capability generated in memory by a cryptographically secure random source
equivalent to Python `secrets.token_bytes(32)`, encoded as canonical unpadded
base64url,
a 300–7,200 second expiry, and the exact intended source-key hash, blob,
repository revision, and profile. The API stores only the capability hash and
returns no reservation secret. This route only enqueues durable control-plane
state; it does not execute the planner, processor, or any provider operation.

## Live GitHub identity gate

Before deleting anything, a separately authorized read-only provider step must
resolve and retain:

1. the live default-branch name and parent commit;
2. the retained file's canonical repository path, blob ID, and source-key hash;
3. the file selected for deletion's canonical path, blob ID, and source-key hash;
4. the expected post-deletion tree identity.

The mutation must be conditional on the recorded parent commit. Immediately
after mutation, the new branch head/tree must be re-read and match the expected
child. Any branch movement, identity mismatch, ambiguous response, or unrelated
tree change blocks enqueue/publication; it must not be treated as the intended
manifest.

## Ordered controlled workflow

1. Refresh read-only platform, queue, retrieval, and protected-data fingerprints.
2. Establish the operator window limitations and ensure all old binaries that
   can claim GitHub jobs/items are upgraded or held idle.
3. Create a fresh recovery backup under its own authorization.
4. Establish the live GitHub identities above, then conditionally delete only
   the reviewed file under a separate provider authorization.
5. Enqueue exactly one job with its reservation in the same database transaction.
6. Execute the exact planner with all four target IDs plus the reservation ID
   and owner capability. It may recover/claim only that job and has no global
   fallback.
7. Require atomic discovery handoff: a complete manifest with exactly one
   reserved eligible item, matching source/blob/revision/profile, one bound
   generation/work item, job success, and reservation state `work_item`.
8. Immediately before processing, repeat the global claimable and recoverable-
   expired checks. Execute the exact processor with all five target IDs and the
   same reservation capability. It may recover/claim only that item, bypasses
   global fairness/recovery, and stops after one outcome.
9. Verify all-success materialization and unchanged legacy retrieval before
   promotion.
10. Promote once in a caller-owned atomic transaction. Require exact activated
    retrieval for the new staged identities and explicit exclusion of legacy
    chunks for that scope; raw chunk inventory is not a retrieval substitute.
11. Reconcile the manifest-v2 generation in bounded transactions, then verify
    only the deleted membership/source/document lifecycle changed. Shared
    active membership and immutable history must remain intact.
12. Replay the idempotent operations and perform independent read-only final
    verification. Disable all transient settings, drop application references
    to the capability, and terminate the ephemeral controller process. Python
    cannot prove byte-for-byte erasure of an immutable string from process
    memory, so this is best-effort disposal rather than a memory-erasure claim.

## Reservation and transaction guarantees

Compatible generic job and work-item claim/recovery queries exclude every live
reservation. Planner acquisition binds its lease to `planner_lease_id` in the
same transaction as the exact job claim. Final discovery validates the complete
one-item projection and atomically changes `job` to `work_item`, binds the
generation/item, clears planner ownership, and completes the synchronization
job. There is no check-to-claim handoff gap.

Processor acquisition locks and validates the exact live reservation and work
item in one short transaction. Expired exact recovery and re-claim are atomic;
there is no committed mutation followed by a different fair/global claim.
Heartbeat, materialization, retry, completion, cancellation, and failure retain
the existing lease UUID and fencing predicates plus reservation ownership.
Retries retain the reservation while clearing the processor lease; terminal
paths release it. Database time controls expiry. Expiry alone never breaks a
valid work lease, while stale capability completion fails closed. Reservation
expiry ends exclusive reservation protection. A controller that observes lost
ownership must stop; it cannot assume indefinite exclusivity or silently
reacquire the target.

## Capability transport gate

Database hashing protects the durable reservation row, not transport. A raw
capability in a Cloud Run command, argument, literal environment override,
flags file, shell history, process listing, audit event, or execution metadata
may be retained outside PostgreSQL. Therefore no future production execution
may pass this bearer capability through a literal Cloud Run override or CLI
argument until a separately reviewed secret-reference or equivalent
non-persistent transport proves that templates, executions, audit logs, local
artifacts, and operator output do not expose it. Documentation intentionally
contains no usable token or command example.

## Rollout and residual limits

Deploy only the transition-compatible API first. On predecessor revision
`20260905_000024`, API startup, health, retrieval, ordinary enqueue, and
ordinary queued cancellation remain supported; reserved enqueue fails before
job mutation. The new generic job/work consumers already reference the new
reservation table and therefore must not execute on the predecessor schema.
Keep every ordinary worker, dedicated planner, and dedicated processor on its
previous image or idle, migrate to `20260911_000025`, then update the API,
legacy connector worker, dedicated planner, and dedicated processor to the
same reviewed compatible release. Verify their immutable images, normalized
templates, disabled defaults, and zero active incompatible executions. Only
then may the first reserved job be created. Old binaries do not query the
reservation table and can still claim protected work after migration.

Rollback to an older worker/API binary is prohibited once any live reservation
or reserved job/item exists. Downgrade drops every reservation row and is safe
only after all consumers are quiescent, no reservation is live or needed, and
the disposition of every reserved job/item has been independently verified.

The reservation prevents compatible generic consumers from taking the intended
job/item and prevents the controlled execution from touching unrelated work.
It cannot prove that external systems will not mutate GitHub, deploy old code,
change configuration, or invoke unrelated platform operations. Those remain
bounded-window operational gates, not database guarantees.

Existing activated retrieval continues to use generation activations,
generations, materializations, and staged chunks. Its SQL gains no dependency
on `connector_sync_control_reservations` (and continues not to acquire work from
`connector_sync_file_work_items`). Promotion, reconciliation, lifecycle
history, tenant authorization, and supported unreserved API behavior are
otherwise unchanged. This plan adds no scheduler, trigger, provider call,
automatic promotion/reconciliation, or production enablement.
