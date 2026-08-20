# GitHub connector

The GitHub connector currently implements the secure GitHub App installation lifecycle and live, read-only repository discovery. Repository selection, scope persistence, content synchronization, webhooks, ACL synchronization, document/index creation, and retrieval remain unimplemented.

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

## Request-scoped credentials and provider bounds

The adapter retrieves the App private key through the configured `SecretStore`, creates an App JWT in memory, and makes one non-retried `POST /app/installations/{stored_id}/access_tokens` request for `Metadata: read` only. It validates token text, requested permissions, and an aware expiry between 30 seconds and 65 minutes from issuance. The installation token is passed only in memory to the immediately following repository request and is then discarded; it is never cached, persisted, returned, or rendered in object representations.

Repository discovery uses one `GET /installation/repositories` request with the trusted configured GitHub API origin, GitHub JSON Accept header, and pinned API version `2022-11-28`. A GET receives at most three total attempts (the initial call plus at most two retries), all within the configured 0.1–60 second total deadline. Only transient transport, 502, 503, 504, and sufficiently short documented rate-limit waits are retryable. Backoff is bounded exponential jitter. Authentication, authorization, validation, and token-creation failures are not retried. `Retry-After` or rate-limit reset waits above 30 seconds, or waits that do not fit the remaining deadline, fail immediately with a safe retry-later error.

Provider responses are untrusted. Every repository must have a positive unique ID, bounded safe names, exact owner-ID ownership of the verified organization, case-insensitive login agreement, consistent full name, real booleans, a supported visibility, a safe branch, a parseable aware timestamp, and an exact-host credential-free HTTPS URL. A malformed entry fails the entire page. Link headers may only describe the trusted repository endpoint with canonical bounded `page` and `per_page` values; they are never followed.

## Operations and remaining roadmap

The GitHub App registration still needs repository `Metadata: read` and `Contents: read` for the installed App because later read-only content synchronization will require Contents access. Discovery narrows its generated token to metadata only. Production operation requires a real organization-installed GitHub App, exact HTTPS setup/callback URLs, production Google Cloud Secret Manager resources and versions, ADC/IAM for the runtime identity, and the documented App settings. No public domain or real provider calls are needed for the deterministic implementation tests.

The next GitHub slice is an explicit product-approved repository-selection contract and tenant-safe `ConnectorScope` persistence. Only after that should content identity/download and incremental synchronization be designed. Webhooks, ACLs, version history, deletion handling, indexing, search, and answers remain later work.
