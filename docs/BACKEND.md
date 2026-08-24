# Connector Synchronization Worker

Run the API, scheduler, and connector synchronization worker as separate processes. Start the connector worker with `python -m infrastructure.workers.connector_sync_worker_host`; add `--once` for one bounded claim attempt. Production requires database, OpenAI embedding, GitHub App, and Google Secret Manager configuration. Worker identity, lease, heartbeat, polling, shutdown, and expired-recovery bounds use the `CONNECTOR_WORKER_*` settings documented in `GITHUB_CONNECTOR.md`. An entry point does not by itself mean the worker is deployed or monitored.

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
python -m infrastructure.workers.connector_sync_scheduler_host --once
python -m alembic -c alembic.ini upgrade head
python -m infrastructure.bootstrap.sandbox
```

The API launcher runs one Uvicorn process on `0.0.0.0` with a validated `PORT`; reload and debug behavior are absent. `APP_ENVIRONMENT` accepts only `development`, `test`, `sandbox`, and `production`. Its deliberate missing-value default is `development`, never production. Sandbox and production require a non-development PostgreSQL URL plus distinct strong JWT and refresh-token secrets. Validation is process-specific so provider credentials are not required by operations that do not consume them.

`GET /health` is dependency-free liveness. `GET /api/v1/health` is readiness: it returns 200 only when configuration is valid, bounded database checks succeed, the schema is at `20260828_000019`, and required GitHub/Secret Manager composition is available in a strict runtime. It uses only fixed values and never retrieves provider secrets.

The image and runbook are implemented and statically tested, but no image or cloud resource has been built or deployed. See `GCP_GITHUB_SANDBOX_RUNBOOK.md`.
