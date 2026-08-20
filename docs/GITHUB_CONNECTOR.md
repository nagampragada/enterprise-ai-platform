# GitHub connector

The GitHub connector implements the secure GitHub App installation lifecycle, live read-only repository discovery, and explicit tenant-safe repository selection. Selection persists only a desired synchronization boundary; content synchronization, webhooks, ACL synchronization, document/index creation, and retrieval remain unimplemented.

## Repository discovery contract

An authenticated `organization_admin` can call:

```http
GET /api/v1/connectors/{connector_id}/github/repositories?page=1&page_size=50
```

`page` is limited to 1–1,000. `page_size` is limited to 1–100. Each platform request fetches exactly one provider page and preserves GitHub's order. The endpoint accepts no browser-supplied organization, owner, App, installation, token, URL, API-version, filter, sort, GraphQL, or unbounded-enumeration control.

The response is a platform-owned page containing only validated repository ID/name/owner metadata, privacy/visibility, archived/disabled flags, default branch, exact-host HTTPS GitHub URL, optional provider update time, and bounded page metadata. It never returns provider permission objects, credentials, installation or App IDs, rate-limit headers, raw provider errors, or arbitrary provider fields.

## Authorization and tenant boundary

Tenant identity comes only from the authenticated administrator. Connector, credential, and verified installation lookups all include that organization ID; a cross-tenant connector ID is concealed as not found. Discovery requires an active GitHub connector and a connected, organization-owned installation whose App ID, installation ID, credential relationship, external subject, account ID/login/type, and granted read scopes agree with the persisted verified binding.

A short database read transaction loads and validates those rows and copies only primitive installation/account values into an immutable request context. The transaction is rolled back before Secret Manager or GitHub I/O. Discovery does not lock or mutate rows and does not persist repository results.

## Repository selection contract

Authenticated `organization_admin` users can create, list, and locally remove persisted selections:

```http
POST /api/v1/connectors/{connector_id}/github/repository-scopes
GET /api/v1/connectors/{connector_id}/github/repository-scopes?limit=20&cursor=...
DELETE /api/v1/connectors/{connector_id}/github/repository-scopes/{scope_id}
```

The POST body accepts exactly a positive `repository_id` and a tenant-owned active `knowledge_space_id`. Owner/name, installation/App/account identifiers, URLs, permissions, status, creator, tenant, and configuration are server-owned. The response exposes only the platform scope ID, connector/space IDs, validated repository administration metadata, lifecycle status, and platform timestamps. It never exposes raw `safe_config` or credentials.

Selection first validates the tenant connector, active credential, connected organization installation, and active knowledge space in a short read transaction. After that transaction ends, it creates one installation token restricted to exactly the candidate repository ID and `Metadata: read`. GitHub's token response must report `repository_selection=selected` and exactly that repository. The restricted token then calls `GET /installation/repositories?per_page=1&page=1`; the page must contain exactly the same repository, total one, with no next page. Both proofs must agree with the verified installation account ID/login. Public repository visibility is never accepted as authorization.

A new short write transaction re-locks the connector, credential, installation binding, and knowledge space, then compares all copied security-boundary identities before persistence. The canonical immutable identity is `github:repository:{repository_id}`. The existing unique `(organization_id, connector_id, external_scope_key)` constraint prevents duplicate or different-space selections, so no migration was required. Connector locking serializes create/reactivate/deselect races. Exact duplicates return one scope; a removed same-space scope is reactivated; a different-space assignment conflicts and is never moved implicitly.

GET is a provider-free bounded `(created_at,id)` keyset page of persisted selections, including locally removed history. DELETE is provider-free and idempotently changes the exact tenant/connector-owned scope to `removed`; it does not revoke GitHub access, hard-delete history, or delete content. Neither operation requires GitHub configuration. No selection route enqueues a job, creates a schedule, retrieves content, or writes source/document/index rows. Existing synchronization APIs remain limited to Local Folder connectors.

## Request-scoped credentials and provider bounds

The adapter retrieves the App private key through the configured `SecretStore`, creates an App JWT in memory, and makes one non-retried `POST /app/installations/{stored_id}/access_tokens` request for `Metadata: read` only. It validates token text, requested permissions, and an aware expiry between 30 seconds and 65 minutes from issuance. The installation token is passed only in memory to the immediately following repository request and is then discarded; it is never cached, persisted, returned, or rendered in object representations.

Repository discovery uses one `GET /installation/repositories` request with the trusted configured GitHub API origin, GitHub JSON Accept header, and pinned API version `2022-11-28`. A GET receives at most three total attempts (the initial call plus at most two retries), all within the configured 0.1–60 second total deadline. Only transient transport, 502, 503, 504, and sufficiently short documented rate-limit waits are retryable. Backoff is bounded exponential jitter. Authentication, authorization, validation, and token-creation failures are not retried. `Retry-After` or rate-limit reset waits above 30 seconds, or waits that do not fit the remaining deadline, fail immediately with a safe retry-later error.

Provider responses are untrusted. Every repository must have a positive unique ID, bounded safe names, exact owner-ID ownership of the verified organization, case-insensitive login agreement, consistent full name, real booleans, a supported visibility, a safe branch, a parseable aware timestamp, and an exact-host credential-free HTTPS URL. A malformed entry fails the entire page. Link headers may only describe the trusted repository endpoint with canonical bounded `page` and `per_page` values; they are never followed.

## Operations and remaining roadmap

The GitHub App registration still needs repository `Metadata: read` and `Contents: read` for the installed App because later read-only content synchronization will require Contents access. Discovery narrows its generated token to metadata only. Production operation requires a real organization-installed GitHub App, exact HTTPS setup/callback URLs, production Google Cloud Secret Manager resources and versions, ADC/IAM for the runtime identity, and the documented App settings. No public domain or real provider calls are needed for the deterministic implementation tests.

The next GitHub slice is read-only repository content identity and download design. Only after that should incremental synchronization be designed. Webhooks, ACLs, version history, deletion handling, indexing, search, and answers remain later work.
