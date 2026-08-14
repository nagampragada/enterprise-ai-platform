# Sprint 02: Secure Document Ingestion Foundation

## Sprint Goal

Establish the secure, tenant-scoped foundation for ingesting local documents into PostgreSQL and preparing them for retrieval. This sprint begins with an authorization review of the existing authentication flows and ends with a tested organization-scoped retrieval path.

## User Stories

- As an authenticated organization user, I need authorization checks to respect my organization context before I can access document data.
- As an organization, I need documents and chunks persisted with `organization_id` so records cannot cross tenant boundaries.
- As an organization user, I need local TXT, Markdown, PDF, and DOCX files converted into deterministic chunks suitable for embedding and retrieval.
- As a platform engineer, I need a provider-independent embedding interface and pgvector storage foundation so embedding providers can evolve without changing ingestion contracts.
- As an organization user, I need retrieval to return only documents belonging to my organization.

## Technical Tasks

- Review authentication authorization, tenant context propagation, logout permissions, and access-control assumptions for the existing authentication endpoints.
- Define and implement document and document-chunk persistence using UUID primary keys and `organization_id` as the tenant-security boundary.
- Enable pgvector in the approved PostgreSQL storage foundation and add the required vector model configuration without introducing unrelated schema.
- Implement PDF and DOCX extractors against the existing content-extraction contracts.
- Define and implement deterministic chunking with stable ordering and repeatable boundaries.
- Define an embedding-provider interface without coupling the domain to a specific provider.
- Implement local-folder ingestion that extracts supported files, persists documents and chunks, generates embeddings through the interface, and stores vectors in PostgreSQL.
- Add an organization-scoped retrieval test that proves one organization cannot retrieve another organization’s documents or chunks.

## Deliverables

- Authentication authorization review findings and any narrowly required authorization corrections.
- Tenant-aware document and document-chunk persistence.
- PostgreSQL pgvector enablement and vector storage foundation.
- PDF and DOCX extraction implementations with focused tests.
- Deterministic chunking implementation with focused tests.
- Embedding-provider interface and local ingestion integration.
- Organization-scoped retrieval behavior and isolation test coverage.

## Acceptance Criteria

- Existing authentication authorization behavior has been reviewed and tenant context is enforced for document operations.
- Documents and chunks are persisted with organization ownership that cannot be mismatched.
- PDF and DOCX files are extracted through the established extraction contracts.
- Identical input produces identical chunk content and ordering.
- Embeddings are requested through the provider interface and stored in pgvector-backed PostgreSQL records.
- Local-folder ingestion persists supported documents and their chunks for the correct organization.
- Retrieval always scopes results by `organization_id`; the organization-isolation test passes.
- Existing tests continue to pass, and new behavior has focused unit and integration coverage.

## Security Requirements

- Require authenticated access for document ingestion and retrieval.
- Derive or verify organization context from authenticated identity rather than trusting an arbitrary tenant identifier from the caller.
- Enforce authorization for the requested organization on every document, chunk, and retrieval operation.
- Preserve password hashing, token, and session-security practices already established in the repository.
- Do not log passwords, raw refresh tokens, document contents, or embedding payloads unnecessarily.
- Keep local-folder path and symlink protections intact.

## Tenant-Isolation Requirements

- `organization_id` is the tenant-security boundary for every customer-owned document and chunk record.
- Repository queries must include organization scope, including lookup, ingestion, deletion, and retrieval paths.
- Foreign keys and service-level validation must prevent a document, chunk, or embedding from being associated with a different organization.
- Tests must include a cross-organization access attempt and verify that no data is returned or modified.

## Testing Expectations

- Add focused tests for authorization and tenant-context enforcement.
- Add extractor tests for valid, blank, malformed, and unsupported PDF/DOCX input as appropriate to the established contracts.
- Add deterministic chunking tests covering stable boundaries, ordering, and edge cases.
- Add persistence and ingestion tests using the repository’s existing database-test conventions.
- Add a retrieval isolation test with at least two organizations and overlapping query conditions.
- Run the relevant backend test suite and report any environment-dependent integration prerequisites.

## Risks

- Authorization gaps could expose another organization’s documents even when authentication succeeds.
- Inconsistent tenant keys or repository filters could create cross-organization retrieval defects.
- PDF/DOCX parsing differences may produce unstable content or metadata.
- Chunking choices may reduce retrieval quality or make reprocessing difficult.
- Embedding-provider assumptions could make later provider changes expensive.
- pgvector availability may differ between local and test PostgreSQL environments.

## Definition of Done

- The existing authentication authorization surface has been reviewed and documented.
- Tenant-aware document and chunk persistence is implemented and tested.
- pgvector is enabled in the approved PostgreSQL foundation.
- PDF and DOCX extraction, deterministic chunking, and the embedding-provider interface are implemented with focused tests.
- Local-folder ingestion stores documents, chunks, and embeddings for the correct organization.
- Organization-scoped retrieval and its cross-tenant isolation test pass.
- No Google Drive, frontend, AI chat, agent router, workflow automation, Tracklytics, AI Advocate, Medical Bill Evaluator, Kubernetes, production deployment, billing, or multiple-LLM-provider work is included.
- Documentation, code, and tests remain within the accepted architecture and no unrelated scope is added.
