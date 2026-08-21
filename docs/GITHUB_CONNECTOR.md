# GitHub connector

The GitHub connector implements the secure GitHub App installation lifecycle, live read-only repository discovery, explicit tenant-safe repository selection, and an internal read-only content-identity/bounded-download foundation. Selection persists only a desired synchronization boundary; content synchronization, webhooks, ACL synchronization, document/index creation, and retrieval remain unimplemented.

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

## Internal repository content reader

There is no public snapshot, tree, blob, or file-download route. A future synchronization worker can use the internal content reader only through a staged boundary: a short tenant-qualified database read validates the active GitHub connector, active repository scope, canonical `github:repository:{repository_id}` key, strict safe scope configuration, active knowledge space, active App-installation credential, connected verified organization installation, owner/account agreement, and recorded `Metadata: read` plus `Contents: read` grants. It copies immutable identifiers into a redacted frozen authorization object. The caller must end the database transaction before any SecretStore or GitHub call; the service rejects provider access while its injected session has an open transaction. Every later operation repeats this short authorization step. A future persistence stage must revalidate the connector/scope boundary before writing anything.

Each snapshot, tree-list, or blob-download operation retrieves the App key on demand, generates an in-memory App JWT, and performs one non-retried installation-token POST restricted to exactly the selected repository ID and exactly `Metadata: read` plus `Contents: read`. The token response must identify exactly that repository and its verified organization owner. Tokens and JWTs are request-scoped, redacted from representations, and never cached, returned, logged, or persisted. Public-repository visibility is never an authorization fallback.

Snapshot resolution accepts no caller-selected branch, tag, ref, or commit. It resolves only the provider-validated default branch stored on the scope. The reader validates canonical lowercase hexadecimal Git object IDs of the explicitly supported 40- and 64-character forms, resolves the commit and root tree, and rechecks the branch head. A branch movement retries the whole identity sequence once; repeated movement or commit/tree disagreement fails closed. Git object IDs remain provider identities and are never represented as platform content hashes.

Tree reads use `GET /repos/{owner}/{repo}/git/trees/{tree_sha}` with no `recursive` parameter and fetch exactly one tree per call. The existing 1 MiB raw JSON cap applies before parsing; the platform then rejects `truncated=true` and more than 1,000 entries. Provider order is preserved. Names and repository-relative paths are bounded UTF-8 strings with `/` separators and no leading slash, empty/dot/dot-dot segment, backslash, NUL, or control character. The reader preserves original Unicode without normalization, while rejecting distinct names that collide under NFC plus case-folding. Regular trees, regular blobs, and executable regular blobs are returned as immutable safe descriptors. Symlinks and submodules are omitted; unknown modes/types fail closed. Provider URLs are ignored.

Blob reads accept only a snapshot-bound regular-blob descriptor and supported pipeline extensions (`.pdf`, `.docx`, `.txt`, `.md`, `.markdown`). The adapter calls the official Git blob endpoint with GitHub's raw media type, refuses redirects, validates `Content-Length` when present, streams raw chunks, and stops as soon as actual bytes exceed 10 MiB. Actual bytes must exactly match the tree-declared size. SHA-256 is computed incrementally over the untouched bytes, which remain immutable and in memory; there are no temporary files and no decoding or extraction. Git LFS pointer content is rejected and never followed.

The planned stable source identity is `github:repository:{repository_id}:path:{canonical_repository_path}`. Repository ID—not mutable owner/name—is the security identity, so a repository rename does not create a new repository identity. Until rename reconciliation exists, a path rename may create a new source identity. A future immutable version will bind commit object ID, blob object ID, platform SHA-256, and exact byte count. No `SourceItem`, document, version, chunk, cursor, job, or provider payload is created by this foundation.

## Request-scoped credentials and provider bounds

The adapter retrieves the App private key through the configured `SecretStore`, creates an App JWT in memory, and makes one non-retried `POST /app/installations/{stored_id}/access_tokens` request for `Metadata: read` only. It validates token text, requested permissions, and an aware expiry between 30 seconds and 65 minutes from issuance. The installation token is passed only in memory to the immediately following repository request and is then discarded; it is never cached, persisted, returned, or rendered in object representations.

Repository discovery uses one `GET /installation/repositories` request with the trusted configured GitHub API origin, GitHub JSON Accept header, and pinned API version `2022-11-28`. A GET receives at most three total attempts (the initial call plus at most two retries), all within the configured 0.1–60 second total deadline. Only transient transport, 502, 503, 504, and sufficiently short documented rate-limit waits are retryable. Backoff is bounded exponential jitter. Authentication, authorization, validation, and token-creation failures are not retried. `Retry-After` or rate-limit reset waits above 30 seconds, or waits that do not fit the remaining deadline, fail immediately with a safe retry-later error.

Provider responses are untrusted. Every repository must have a positive unique ID, bounded safe names, exact owner-ID ownership of the verified organization, case-insensitive login agreement, consistent full name, real booleans, a supported visibility, a safe branch, a parseable aware timestamp, and an exact-host credential-free HTTPS URL. A malformed entry fails the entire page. Link headers may only describe the trusted repository endpoint with canonical bounded `page` and `per_page` values; they are never followed.

## Operations and remaining roadmap

The GitHub App registration needs repository `Metadata: read` and `Contents: read`. Discovery and selection narrow their generated tokens to metadata only; internal content operations request both permissions for exactly one repository. Production operation requires a real organization-installed GitHub App, exact HTTPS setup/callback URLs, production Google Cloud Secret Manager resources and versions, ADC/IAM for the runtime identity, and the documented App settings. No public domain or real provider calls are needed for the deterministic implementation tests.

The next GitHub slice is staged synchronization on the existing job/lease infrastructure, using this immutable snapshot and bounded-download foundation. Recursive whole-repository crawling is intentionally absent: future traversal must remain staged and bounded one tree at a time. Webhooks, ACLs, rename/deletion reconciliation, indexing integration, search, and answers remain later work.
