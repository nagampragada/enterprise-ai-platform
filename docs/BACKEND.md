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
