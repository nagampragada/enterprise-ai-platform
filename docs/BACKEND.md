# Connector Synchronization Worker

Run the API, scheduler, and connector synchronization worker as separate processes. Start the connector worker with `python -m infrastructure.workers.connector_sync_worker_host`; add `--once` for one bounded claim attempt. Production requires database, OpenAI embedding, GitHub App, and Google Secret Manager configuration. Worker identity, lease, heartbeat, polling, shutdown, and expired-recovery bounds use the `CONNECTOR_WORKER_*` settings documented in `GITHUB_CONNECTOR.md`. An entry point does not by itself mean the worker is deployed or monitored.

`GITHUB_SYNC_LEDGER_PLANNING_ENABLED` is an optional worker-only rollout flag.
It defaults to `false` and accepts only exact lowercase `true` or `false`.
Enabled mode shadows bounded GitHub discovery metadata into the durable work
ledger while the established GitHub path remains the only processor and
indexer. It does not enable ledger claims, embeddings, promotion, or retrieval.

`GITHUB_SYNC_LEDGER_PROCESSING_ENABLED` is a separate optional worker-only
rollout flag. It also defaults to `false` and accepts only exact lowercase
`true` or `false`. When enabled, the worker considers one completed-discovery
GitHub file-work item only after the legacy synchronization queue is empty. It
uses the recorded commit/blob/path/profile, existing extraction/chunking/
embedding pipeline, file-work lease and fence, and an atomic generation-scoped
staging plus completion transaction. Staged text/vectors are stored outside the
legacy source/document/version/chunk graph and are not retrieval-visible. It
does not promote or reconcile a generation.
The API, scheduler, migration, and bootstrap processes do not consume the flag.

Phase 3 Slice 2 adds a dedicated ledger-only host at
`python -m infrastructure.workers.github_sync_ledger_worker_host`. It is
independent of the legacy synchronization queue: it never claims a legacy job,
performs discovery, promotes a generation, reconciles deletion, or changes
retrieval-visible state. The host checks the processing gate before composing
ledger/provider services and before every claim. Defaults are 25 items, a
20-minute drain deadline, a 12-minute minimum claim runway, a 15-minute lease,
a 60-second heartbeat, one empty poll with a 5-second interval, a 5-minute
heartbeat shutdown bound, and recovery of at most 10 expired items. All bounds
are positive, hard-capped, and may be overridden only by the corresponding
validated `GITHUB_LEDGER_WORKER_*` environment values or explicit command-line
options. A runtime drain deadline never interrupts a claimed item. After a
signal, the active item receives the configured graceful window; an indivisible
provider request remains governed by its provider timeout, and an expired grace
window schedules a fixed-code durable retry at the next safe progress boundary.
The dedicated OpenAI client has a 10-minute per-call timeout and no SDK retry;
the claim runway and lease must each cover that timeout plus two heartbeat
margins. No new item is then claimed.

The dedicated host emits fixed structured per-item and terminal summary fields
only. Process status is `0` for disabled, empty, bounded/partial drain, durable
retry scheduling, quarantine, cancellation, and safely completed signal stop.
Their distinct meaning is retained in `run_status`, `stop_reason`,
`graceful_shutdown`, and `partial_drain`. Process status is `1` only for invalid
configuration/composition, unhandled or contradictory results, an undurable
terminal transition, or lease/fence correctness loss. A retryable failure stops
the execution only after database backoff is durable; `next_attempt_at` remains
the claim authority and the future Cloud Run Job must use task retries `0` so a
platform retry cannot compete with application backoff. Quarantined and
cancelled items are terminal and allow the drain to continue.

Phase 3 Slice 3 makes organization fairness intrinsic to the dedicated host;
there is no bypass flag or weight configuration. Never-served eligible
organizations precede served organizations, then the least committed claim
sequence and organization UUID determine selection. Indexed correlated probes
avoid grouping the full claimable backlog. The never-served organization row or
served schedule row, item lease/fence, and durable scheduling-state advancement
share one database transaction. Retry-wait and ineligible work receive no turn;
rollback leaves no durable turn but may leave an expected PostgreSQL sequence
gap, and expired-lease recovery never rewinds the original turn. This is equal
committed scheduling-turn order, not a wall-clock SLA or per-organization active
processing cap: later committed claims for the same organization may overlap in
processing. Safe item
telemetry adds only organization UUID and committed fairness sequence;
summaries add the distinct organizations whose committed claims were processed.
No tenant name, content, provider payload, URL,
credential, or vector is logged.

## Connector synchronization operations

Authenticated active `organization_admin` users can enqueue, list, inspect, and cancel synchronization jobs for a tenant-owned Local Folder or selected GitHub repository scope:

```http
POST /api/v1/connectors/{connector_id}/sync-jobs
GET  /api/v1/connectors/{connector_id}/sync-jobs?page=1&page_size=50
GET  /api/v1/connectors/{connector_id}/sync-jobs/{job_id}
POST /api/v1/connectors/{connector_id}/sync-jobs/{job_id}/cancel
```

Create accepts only `connector_scope_id`; cancel needs no body and accepts no fields. Connector type, organization, knowledge space, provider authorization, trigger, retry policy, priority, and worker controls are server-owned. Manual and scheduled enqueue share the database-enforced one-nonterminal-job-per-scope invariant, so a repeated or concurrent request returns the existing safe job without resetting attempts or backoff.

Listing uses newest-first `(created_at,id)` ordering with `page` limited to 1–1,000 and `page_size` limited to 1–100. Detail includes at most 20 newest run summaries. Responses are explicit DTOs and omit tenant identity, worker/lease/fence/heartbeat data, cursors, provider metadata, credentials, secrets, source content, vectors, raw exceptions, and arbitrary JSON.

Cancellation is database-only and cooperative. Queued and retry-waiting jobs become terminal immediately and cannot be claimed; running jobs retain their fenced lease with a durable cancellation request for the worker to acknowledge. Succeeded, failed, and already-cancelled jobs return their unchanged terminal representation. These API transactions never call a connector provider, Secret Manager, extraction, chunking, embeddings, or worker code.

# Production packaging and process commands

`backend/Dockerfile` builds one Python 3.12 image for all backend operations. The final stage contains runtime dependencies, installed application packages, and Alembic files; it runs as UID/GID `10001`, writes temporary extraction data only beneath `/tmp`, and does not contain tests, scripts, `.env`, Git data, virtual environments, caches, or credentials. Migrations are deliberately not part of API startup.

```text
python -m app.server
python -m infrastructure.workers.connector_sync_worker_host --once
python -m infrastructure.workers.github_sync_ledger_worker_host
python -m infrastructure.workers.connector_sync_scheduler_host --once
python -m alembic -c alembic.ini upgrade head
python -m infrastructure.bootstrap.sandbox
```

The API launcher runs one Uvicorn process on `0.0.0.0` with a validated `PORT`; reload and debug behavior are absent. `APP_ENVIRONMENT` accepts only `development`, `test`, `sandbox`, and `production`. Its deliberate missing-value default is `development`, never production. Sandbox and production require a non-development PostgreSQL URL plus distinct strong JWT and refresh-token secrets. Validation is process-specific so provider credentials are not required by operations that do not consume them.

`GET /health` is dependency-free liveness. `GET /api/v1/health` is readiness: it returns 200 only when configuration is valid, bounded database checks succeed, the schema is in the exact transition allowlist, and required GitHub/Secret Manager composition is available in a strict runtime. The Phase 3 Slice 3 application explicitly accepts only `20260831_000021` and `20260902_000022`; it reports compatibility, currentness, and migration requirement separately. Revision `20260831_000021` remains ready but is not represented as current. Unknown, missing, malformed, newer, older, or multiple heads fail closed. The endpoint uses only fixed values and never retrieves provider secrets.

The image and runbook are implemented and statically tested, but no image or cloud resource has been built or deployed. See `GCP_GITHUB_SANDBOX_RUNBOOK.md`.
