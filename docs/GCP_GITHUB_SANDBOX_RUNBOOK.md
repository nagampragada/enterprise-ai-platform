# GCP GitHub connector sandbox runbook

This is an operator procedure for a controlled backend-only test. It uses placeholders and has not been executed by repository work. Every resource incurs cost; set budgets/alerts first, use a dedicated sandbox project, and obtain approval before creating or later deleting resources.

## 1. Prerequisites and placeholders

Install authenticated `gcloud`, Docker or Cloud Build access, `curl`, and PostgreSQL administration tooling. Use attached service accounts and Application Default Credentials (ADC); never create or upload a service-account JSON key.

Set only nonsecret operator variables. Do not put secret payloads in these variables:

```powershell
$PROJECT_ID = '<sandbox-project-id>'
$REGION = '<gcp-region>'
$REPOSITORY = '<artifact-repository>'
$IMAGE = "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/backend:<immutable-image-tag>"
$API_SERVICE = '<api-service-name>'
$WORKER_JOB = '<worker-job-name>'
$SCHEDULER_JOB = '<scheduler-job-name>'
$MIGRATION_JOB = '<migration-job-name>'
$BOOTSTRAP_JOB = '<bootstrap-job-name>'
```

Do not use a real organization name, user email, repository name, or GitHub account in shared documentation, tickets, or command transcripts. Commands below intentionally never contain passwords, access tokens, private keys, OAuth client secrets, OpenAI keys, or a complete database URL.

## 2. Enable required APIs

After approval, enable only the services used by the chosen design:

```powershell
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com sqladmin.googleapis.com iam.googleapis.com logging.googleapis.com monitoring.googleapis.com --project=$PROJECT_ID
```

If an approved PostgreSQL service other than Cloud SQL is used, omit `sqladmin.googleapis.com`. Do not broadly enable unrelated APIs.

## 3. Create PostgreSQL and pgvector capability

Provision one private, sandbox-only PostgreSQL 16/pgvector-compatible database. For Cloud SQL, require encrypted connections, automated backups appropriate to the test, no public authorized-network wildcard, and a dedicated database/user. Prefer private IP or the Cloud SQL connector/Unix socket from Cloud Run. Confirm the database user separation:

- API, worker, scheduler, and bootstrap identities receive only application DML/connect privileges needed by their process.
- The migration identity receives schema migration privileges and no provider permissions.
- The bootstrap identity receives temporary application-table insert/read privileges and no provider permissions.

Enable `vector` using the approved database administration path. Store the complete `DATABASE_URL` as a Secret Manager secret and bind it at runtime; never type it into `--set-env-vars`, logs, shell history, or this document.

## 4. Create separate runtime identities

Create five user-managed service accounts, without JSON keys:

```powershell
gcloud iam service-accounts create <api-service-account> --project=$PROJECT_ID
gcloud iam service-accounts create <worker-service-account> --project=$PROJECT_ID
gcloud iam service-accounts create <scheduler-service-account> --project=$PROJECT_ID
gcloud iam service-accounts create <migration-service-account> --project=$PROJECT_ID
gcloud iam service-accounts create <bootstrap-service-account> --project=$PROJECT_ID
```

Grant only these IAM categories, scoped to exact resources wherever supported:

- API: Cloud SQL Client/connectivity; access to the exact database, JWT, refresh, GitHub client-secret, and GitHub private-key secret versions; the narrow custom Secret Manager permissions documented in `GCP_SECRET_MANAGER.md` for application-created ephemeral PKCE containers.
- Worker: Cloud SQL Client/connectivity and access to only the exact database, OpenAI, and GitHub private-key secret versions. Do not grant GitHub client-secret access.
- Scheduler: Cloud SQL Client/connectivity and exact database secret access only. No GitHub, OpenAI, or Secret Manager application-container role.
- Migration: Cloud SQL Client/connectivity and exact database secret access only.
- Bootstrap: Cloud SQL Client/connectivity plus exact database and temporary bootstrap-password secret access only.

Do not grant Owner, Editor, Secret Manager Admin, project-wide wildcard secret access, service-account key administration, or a role to an entire default-compute identity. Restrict who may deploy revisions as each service account and retain audit logging.

## 5. Provision version-pinned secrets safely

Create separate random non-identifying secret containers for the GitHub private key and OAuth client secret. Their IDs must match `<prefix>-sm-<32-lowercase-hex>` because runtime references are canonical and version pinned. Create metadata on the command line, then provide each value through protected stdin:

```powershell
gcloud secrets create <private-key-secret-id> --project=$PROJECT_ID --replication-policy=automatic --labels=environment=sandbox,purpose=github-app-private-key
gcloud secrets versions add <private-key-secret-id> --project=$PROJECT_ID --data-file=-
gcloud secrets create <client-secret-secret-id> --project=$PROJECT_ID --replication-policy=automatic --labels=environment=sandbox,purpose=github-app-client-secret
gcloud secrets versions add <client-secret-secret-id> --project=$PROJECT_ID --data-file=-
```

Do not echo or retrieve payloads to verify them. Record only numeric versions and construct references of this form:

```text
gcp-secret-manager://projects/<sandbox-project-id>/secrets/<prefix>-sm-<32-lowercase-hex>/versions/<numeric-version>
```

Provision separate exact secrets for `DATABASE_URL`, `JWT_SECRET_KEY`, `REFRESH_TOKEN_HASH_SECRET`, `OPENAI_API_KEY`, and the temporary bootstrap password. JWT and refresh values must be distinct strong random values. Prefer `--set-secrets` bindings. Direct environment values are visible to users who can inspect Cloud Run revisions; command-line values can also remain in history.

Application-created PKCE verifier secrets are ephemeral adapter-managed containers. They are different from the operator-provisioned long-lived, version-pinned GitHub secrets and must not be manually relabeled as adapter owned.

Secret Manager may return resource names whose project segment is the numeric project number even when the request used `$PROJECT_ID`. The adapter accepts that canonicalization only for responses to its own exact-resource RPCs; it continues to send requests and persist version-pinned references with `$PROJECT_ID`, requires the exact generated secret ID and version, and does not require an additional project-lookup IAM permission.

## 6. Build and push the common image

From the repository root, build only the backend context and use an immutable tag:

```powershell
gcloud artifacts repositories create $REPOSITORY --repository-format=docker --location=$REGION --project=$PROJECT_ID
gcloud builds submit backend --tag=$IMAGE --project=$PROJECT_ID
```

Before deployment, inspect the image metadata, scan it under the organization policy, and confirm it runs as UID/GID `10001`. The image default is `python -m app.server`; jobs override it explicitly. No migration runs during API startup.

## 7. Create the API service and establish its stable URL

The API must be internet reachable because GitHub cannot present a Cloud Run IAM identity token to setup/callback routes. Use ingress `all` and allow unauthenticated transport to the service, then rely on application JWT/role authorization for administrator APIs. GitHub setup/callback routes are protected by hashed state, PKCE, expiry, single use, and provider verification. Do not expose a worker or scheduler service.

On the initial API revision, use syntactically valid reserved placeholder setup/callback URLs and do not configure or exercise the GitHub App yet. This step exists only to obtain the service's stable generated URL; immediately replace the placeholders before traffic testing:

```powershell
gcloud run deploy $API_SERVICE --image=$IMAGE --region=$REGION --project=$PROJECT_ID --service-account=<api-service-account-email> --ingress=all --allow-unauthenticated --port=8080 --timeout=60 --min=0 --max=<small-sandbox-maximum> --set-env-vars=APP_ENVIRONMENT=sandbox,PORT=8080,GCP_SECRET_MANAGER_PROJECT_ID=$PROJECT_ID,GCP_SECRET_MANAGER_SECRET_PREFIX=<prefix>,GCP_SECRET_MANAGER_ENVIRONMENT=sandbox,GITHUB_APP_ID=<github-app-id>,GITHUB_APP_SLUG=<github-app-slug>,GITHUB_APP_CLIENT_ID=<github-client-id>,GITHUB_APP_CALLBACK_URL=https://pending.invalid/api/v1/connectors/github/callback,GITHUB_APP_SETUP_URL=https://pending.invalid/api/v1/connectors/github/setup,GITHUB_APP_CLIENT_SECRET_REFERENCE=<version-pinned-client-secret-reference>,GITHUB_APP_PRIVATE_KEY_REFERENCE=<version-pinned-private-key-reference> --set-secrets=DATABASE_URL=<database-url-secret>:<version>,JWT_SECRET_KEY=<jwt-secret>:<version>,REFRESH_TOKEN_HASH_SECRET=<refresh-secret>:<version>
```

Read the generated HTTPS URL from `gcloud run services describe` output without shell command substitution. A custom domain is not required. Form the exact URLs:

```text
https://<generated-service-host>/api/v1/connectors/github/setup
https://<generated-service-host>/api/v1/connectors/github/callback
```

Update the Cloud Run service with those exact nonsecret values, wait for the revision to become ready, and then configure the same URLs in the GitHub App. Do not enable automatic OAuth-on-install. Minimum instances may remain zero for sandbox unless measured callback cold-start behavior requires one. Keep request timeout bounded.

## 8. Create private one-shot jobs

Create Cloud Run Jobs with no ingress or public endpoint and explicit commands:

```powershell
gcloud run jobs create $WORKER_JOB --image=$IMAGE --region=$REGION --project=$PROJECT_ID --service-account=<worker-service-account-email> --command=python --args=-m,infrastructure.workers.connector_sync_worker_host,--once --max-retries=<bounded-retry-count> --task-timeout=<bounded-worker-timeout> --set-env-vars=APP_ENVIRONMENT=sandbox,GCP_SECRET_MANAGER_PROJECT_ID=$PROJECT_ID,GCP_SECRET_MANAGER_SECRET_PREFIX=<prefix>,GCP_SECRET_MANAGER_ENVIRONMENT=sandbox,GITHUB_APP_ID=<github-app-id>,GITHUB_APP_CLIENT_ID=<github-client-id>,GITHUB_APP_PRIVATE_KEY_REFERENCE=<version-pinned-private-key-reference> --set-secrets=DATABASE_URL=<database-url-secret>:<version>,OPENAI_API_KEY=<openai-secret>:<version>
gcloud run jobs create $SCHEDULER_JOB --image=$IMAGE --region=$REGION --project=$PROJECT_ID --service-account=<scheduler-service-account-email> --command=python --args=-m,infrastructure.workers.connector_sync_scheduler_host,--once --max-retries=<bounded-retry-count> --task-timeout=<bounded-scheduler-timeout> --set-env-vars=APP_ENVIRONMENT=sandbox --set-secrets=DATABASE_URL=<database-url-secret>:<version>
gcloud run jobs create $MIGRATION_JOB --image=$IMAGE --region=$REGION --project=$PROJECT_ID --service-account=<migration-service-account-email> --command=python --args=-m,alembic,-c,alembic.ini,upgrade,head --max-retries=0 --task-timeout=<bounded-migration-timeout> --set-env-vars=APP_ENVIRONMENT=sandbox --set-secrets=DATABASE_URL=<database-url-secret>:<version>
```

The worker receives no OAuth client secret. Scheduler and migration receive no GitHub, GCP application-secret, or OpenAI settings. Successful work and legitimate no-work both exit zero; fatal startup/database/processing failures remain nonzero.

The dedicated Phase 3 ledger host is not deployed by this runbook. Its private
Cloud Run Job command is
`python -m infrastructure.workers.github_sync_ledger_worker_host`; it requires
the same database, OpenAI, GitHub private-key reference, and nonsecret GitHub/
Secret Manager settings as the connector worker, plus an explicitly authorized
`GITHUB_SYNC_LEDGER_PROCESSING_ENABLED=true`. Keep it absent or exactly
lowercase `false` until a separate controlled rollout. Do not give this job the
GitHub OAuth client secret. Its task timeout must exceed the validated drain and
graceful shutdown bounds, and set Cloud Run task retries to `0`. Disabled,
empty, bounded drain, safe signal shutdown, and a durably database-scheduled
retry all exit `0`; inspect `run_status` and `stop_reason` rather than treating
those expected outcomes as task failures. The database `next_attempt_at` gate,
not Cloud Run retry timing, controls another claim. The host does not discover,
promote, reconcile, or activate retrieval. Slice 3 organization fairness is an
intrinsic claim property with no additional environment flag; deploy it only
after migration `20260902_000022` and a separate controlled authorization.

The dedicated planner-only entry point is
`python -m infrastructure.workers.github_sync_ledger_planner_host --once`.
It uses the existing strict `GITHUB_SYNC_LEDGER_PLANNING_ENABLED` gate and
requires the database plus GitHub App/private-key configuration, but it must not
receive `OPENAI_API_KEY` or the GitHub OAuth client secret. It creates no legacy
source/document/indexing state and completes a synchronization job only after
the pinned manifest-schema-v2 traversal is exhausted. A stopped or partial run
retains its durable cursor and incomplete generation for fenced lease recovery.
One execution defaults to 5,000 committed batches and 20 minutes; configure
`GITHUB_LEDGER_PLANNER_MAX_BATCHES` and `GITHUB_LEDGER_PLANNER_MAX_SECONDS`
within their 100,000-batch and 3,600-second hard caps. A budget stop is a truthful
resumable outcome, not completion. Before executing the planner, set
`GITHUB_SYNC_LEDGER_PLANNING_ENABLED=true` on both the planner and legacy worker
templates so the legacy worker becomes Local-Folder-only, or keep the legacy
worker idle. Do not run a legacy template still set to `false` concurrently;
Cloud Run templates do not share environment state. Verify both normalized
templates and zero active executions as a rollout gate.
No Cloud Run Job, scheduler, or trigger for this host is provisioned by the
current implementation.

Slice 4 adds `GITHUB_SYNC_LEDGER_PROMOTION_ENABLED`, defaulting to lowercase
`false`. Do not add or enable it on an existing worker: no deployed host consumes
the promotion contract yet. A later controlled promotion operator must use the
same immutable application image, schema `20260904_000023`, one caller-owned
transaction, and read-only before/after retrieval verification. Promotion makes
no provider call; its citation projection and cutover must share the same single
caller-owned commit. Slice 5 separately addresses deletion reconciliation; Slice 6
retains staging retention, automation, and rollback operations.

Slice 5 adds `GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED`, also strict and
defaulting to lowercase `false`. No existing Cloud Run host consumes it. Do not
add or enable it on the connector or dedicated worker. A future authorized
operator must use schema `20260905_000024`, a promoted manifest-schema-v2
generation, bounded caller-owned transactions, and read-only retrieval checks.
Reconciliation makes no provider call and physically deletes no history.
Its 500 limit counts selected absent memberships, not every related row update.
The caller commits each batch and its progress together. A schema downgrade
removes observation/progress metadata but cannot reverse lifecycle retirements;
do not downgrade after reconciliation without a separately validated recovery
plan.

Create the bootstrap job with no command-line bootstrap values:

```powershell
gcloud run jobs create $BOOTSTRAP_JOB --image=$IMAGE --region=$REGION --project=$PROJECT_ID --service-account=<bootstrap-service-account-email> --command=python --args=-m,infrastructure.bootstrap.sandbox --max-retries=0 --task-timeout=<bounded-bootstrap-timeout> --set-env-vars=APP_ENVIRONMENT=sandbox,ALLOW_SANDBOX_BOOTSTRAP=true,SANDBOX_BOOTSTRAP_ORGANIZATION_NAME=<sandbox-organization-name>,SANDBOX_BOOTSTRAP_ORGANIZATION_SLUG=<sandbox-organization-slug>,SANDBOX_BOOTSTRAP_ADMIN_EMAIL=<sandbox-admin-email>,SANDBOX_BOOTSTRAP_ADMIN_FIRST_NAME=<first-name>,SANDBOX_BOOTSTRAP_ADMIN_LAST_NAME=<last-name>,SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_NAME=<space-name>,SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_SLUG=<space-slug> --set-secrets=DATABASE_URL=<database-url-secret>:<version>,SANDBOX_BOOTSTRAP_ADMIN_PASSWORD=<bootstrap-password-secret>:<version>
```

## 9. Execute migrations, bootstrap, and readiness verification

For the Phase 3 expand-migration transition, deploy in this order and inspect
each operation's nonsecret status before continuing:

1. deploy the transition-compatible API image before changing the database;
2. require readiness HTTP 200 with `schema_compatible=true`,
   `schema_current=false`, and `migration_required=true` at revision
   `20260904_000023`;
3. update only the established migration job to the same immutable image;
4. execute the migration job exactly once and verify the image's expected
   revision (Slice 5: `20260905_000024`);
5. require readiness HTTP 200 with `schema_compatible=true`,
   `schema_current=true`, and `migration_required=false`;
6. update worker images while keeping planning/processing false and leaving
   promotion/reconciliation absent or explicitly false;
7. continue only with the separately authorized controlled Phase 3 verification.

The predecessor compatibility entry is deliberate temporary expand-migration
policy. Remove it in a later cleanup only after all environments are confirmed
at `20260905_000024`. Do not broaden it into revision ordering or a range.

For initial provisioning after the compatible API is deployed:

```powershell
gcloud run jobs execute $MIGRATION_JOB --region=$REGION --project=$PROJECT_ID --wait
gcloud run jobs execute $BOOTSTRAP_JOB --region=$REGION --project=$PROJECT_ID --wait
curl.exe --fail-with-body https://<generated-service-host>/health
curl.exe --fail-with-body https://<generated-service-host>/api/v1/health
```

Readiness must return HTTP 200 only after database connectivity, an exact
compatible revision, configuration, and local GitHub/Secret Manager composition
are ready. It makes no GitHub, OpenAI, or secret-value request.

Immediately after bootstrap success:

1. remove the `SANDBOX_BOOTSTRAP_ADMIN_PASSWORD` secret binding and `ALLOW_SANDBOX_BOOTSTRAP` opt-in from the job;
2. disable or delete the bootstrap job after explicit approval;
3. retain only normal login capability;
4. rotate the initial password through a supported secure lifecycle when one is available.

Never preserve the bootstrap command as scheduled automation.

## 10. Run the backend-only GitHub test

Using the authenticated administrator and documented API contracts:

1. create the GitHub connector;
2. begin installation, follow the returned GitHub URL, and complete setup/callback state correlation;
3. request one bounded repository discovery page;
4. explicitly select one authorized repository into the bootstrapped knowledge space;
5. enqueue one synchronization job;
6. invoke the one-shot worker and poll the provider-neutral job endpoint until terminal;
7. create/resume an interval schedule, invoke the one-shot scheduler, then invoke the worker for the enqueued job;
8. perform read-only database/indexing verification using approved queries;
9. test bounded update, path rename, deletion reconciliation, and cooperative cancellation scenarios;
10. pause all schedules when testing ends.

```powershell
gcloud run jobs execute $WORKER_JOB --region=$REGION --project=$PROJECT_ID --wait
gcloud run jobs execute $SCHEDULER_JOB --region=$REGION --project=$PROJECT_ID --wait
```

Do not log bearer tokens or place them directly in shared command history. Public retrieval/search, answer generation, citations, webhooks, and ACL synchronization are outside this test.

## 11. Closeout

After explicit operator approval, scale down or remove the API, jobs, database, repository, and project resources in the approved dependency order. Do not run broad or wildcard deletion commands. Before removal, pause schedules, capture only approved nonsecret evidence, and confirm no job is running.

Rotate or revoke the test administrator credential, OpenAI key, JWT/refresh values, GitHub OAuth client secret, and GitHub App private key. Remove the GitHub App installation or App only through the approved GitHub administrator process. Review audit logs and billing. Resource cleanup is an operator action; this repository implementation performs none of it.

## 12. Rollback procedure

Rollback is an explicit operator decision, not an automatic container behavior. Stop new work first: pause connector schedules, do not invoke worker/scheduler jobs, and prevent additional administrator changes. Preserve approved nonsecret execution and revision identifiers for diagnosis.

- API regression: route 100 percent of traffic to an already validated prior Cloud Run revision using an exact `<prior-api-revision>` placeholder. Do not roll back to a revision with incompatible schema or configuration.
- Worker or scheduler regression: stop executions and update the private job to an already validated immutable prior image digest before manually retrying.
- Migration failure before commit: Alembic/PostgreSQL transactional DDL should roll back the failed revision; inspect the exact recorded revision before retrying. Do not run an improvised downgrade.
- Migration failure after a committed incompatible change: keep application traffic stopped and follow the approved database restore/recovery plan. Restore to a separately verified target; never overwrite the only recovery copy.
- Bootstrap failure: the command rolls back its caller-owned transaction. Inspect only fixed-safe job status and resolve the conflict; do not manually patch a partial identity state or rerun with weaker authorization.
- Credential concern: disable affected job invocations, rotate the exact version-pinned secret, update only the intended identity binding, and revoke the old version after validation.

Example traffic rollback (placeholder only):

```powershell
gcloud run services update-traffic $API_SERVICE --region=$REGION --project=$PROJECT_ID --to-revisions=<prior-api-revision>=100
```

After rollback, recheck liveness, strict readiness, schema revision, job status, and audit logs before resuming schedules. Resource deletion or database restoration requires separate explicit approval.

## Implemented versus remaining

Implemented and tested in code: common image definition, API launcher, process gates, liveness/readiness, one-shot exit semantics, and sandbox bootstrap. Not yet performed: real Docker image build, artifact push, Cloud SQL/GCP/IAM/Secret Manager provisioning, Cloud Run deployment, GitHub App configuration, live provider traffic, monitoring validation, backup/restore testing, and credential rotation. Frontend, search APIs, answer generation, webhooks, and ACL synchronization remain future work.
