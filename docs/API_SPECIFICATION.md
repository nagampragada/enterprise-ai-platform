# Connector synchronization operations

All routes below require an authenticated user with an active organization membership and the `organization_admin` role. Organization and user identity come only from the authenticated context. Cross-tenant and wrong-connector identifiers use the existing concealed `404 Resource not found` response.

## Create or coalesce a manual synchronization job

```http
POST /api/v1/connectors/{connector_id}/sync-jobs
Content-Type: application/json

{"connector_scope_id":"<uuid>"}
```

Returns `202`. The body accepts exactly `connector_scope_id`. The server validates the persisted connector type and active tenant-bound connector, scope, and knowledge space. GitHub additionally requires a canonical active repository scope and matching active connected App-installation authorization. A current queued, running, or retry-waiting job for the scope is returned with `coalesced=true`; otherwise a new manual job is returned with `coalesced=false`.

## List connector synchronization jobs

```http
GET /api/v1/connectors/{connector_id}/sync-jobs?page=1&page_size=50&status=queued
```

`page` defaults to 1 and is limited to 1–1,000. `page_size` defaults to 50 and is limited to 1–100. Optional `status` is one of `queued`, `running`, `retry_wait`, `succeeded`, `failed`, or `cancelled`. Results are newest-first with an ID tie-breaker and contain only jobs for the authenticated tenant and requested connector.

## Inspect one synchronization job

```http
GET /api/v1/connectors/{connector_id}/sync-jobs/{job_id}
```

Returns the safe job plus at most 20 newest execution-attempt summaries. Aggregate counters are nonnegative and run history is never loaded without a bound.

## Request cancellation

```http
POST /api/v1/connectors/{connector_id}/sync-jobs/{job_id}/cancel
```

No request body is needed; arbitrary fields are rejected. Queued and retry-waiting jobs become cancelled immediately. Running jobs persist a cooperative cancellation request. Succeeded, failed, and already-cancelled jobs return their unchanged terminal representation.

## Public response boundary

Job responses contain platform job, connector, and scope IDs; deliberately mapped mode, trigger, and status; attempt counts; next-attempt, completion, and creation timestamps; cancellation state; and bounded safe error category/code. Run summaries contain run ID, deliberate status/trigger values, attempt number, start/completion/cancellation timestamps, and selected aggregate counters.

The operations never expose organization ID, requester identity, priority, worker owner, lease UUID, fence, heartbeat or expiry, cursor data, provider metadata, installation or credential IDs, secret references, tokens, raw exceptions or summaries, source content, chunks, embeddings, vectors, SQL, or ORM/database details. They perform database work only and never call GitHub, Secret Manager, extraction, chunking, embeddings, OpenAI, or worker threads.

## Runtime health contracts

`GET /health` is public process liveness and returns the fixed payload `{"status":"healthy"}` while the process can serve requests. It performs no database, GitHub, OpenAI, Secret Manager, or other provider operation.

`GET /api/v1/health` is public strict readiness. Success is HTTP 200 only when runtime configuration, bounded database connectivity, an explicitly compatible Alembic revision, and strict-runtime GitHub/Secret Manager composition are ready. During the Slice 5 expand-migration transition, the exact compatible revisions are `20260904_000023` and `20260905_000024`; no other older, newer, malformed, missing, or branched revision is accepted. The fixed response distinguishes Boolean `schema_compatible`, `schema_current`, and `migration_required` values while retaining the `schema` ready/not-ready check. Revision `20260904_000023` is compatible but not current and requires migration; `20260905_000024` is compatible and current. Any failed check returns HTTP 503. The contract never returns an exception, database URL, SQL, project identifier, secret reference, credential, or provider detail, and never retrieves secret values or contacts GitHub/OpenAI.

Slices 4 and 5 add no public promotion, reconciliation, query, search, chat, or
administration route. Generation promotion and deletion reconciliation are
independent internal default-off application contracts. A future
API/UI must add authenticated administration, transaction ownership, conflict
mapping, and audit policy without accepting tenant/repository attribution from
an untrusted client.
