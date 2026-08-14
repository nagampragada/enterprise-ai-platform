# Current Task

Documentation alignment and preparation for the document-ingestion vertical slice.

## Why This Task Exists

The documentation was written during the repository scaffold and database-foundation phase. Implementation then progressed through authentication, admin bootstrap, connector contracts, a local-folder connector, and TXT/Markdown extraction, but the status documents were not updated with those changes. They therefore describe completed work as pending and incorrectly state that application code and authentication do not exist.

## Current Implementation

- PostgreSQL database infrastructure, SQLAlchemy models, and Alembic migrations exist for organizations and identity/authentication data.
- Password hashing, JWT access and refresh-token handling, authentication sessions, login, refresh, logout, logout-all, and authenticated `/me` behavior exist.
- An admin-user bootstrap script exists.
- Connector contracts, a local-folder connector, content-extraction contracts, and TXT/Markdown extractors exist with related tests.
- Document persistence, PDF/DOCX extraction, chunking, embeddings, pgvector storage, and tenant-scoped retrieval do not yet exist.

## Allowed Changes

- Update only `README.md`, `docs/PROJECT_CONTEXT.md`, `docs/CURRENT_TASK.md`, and `docs/SPRINT_02.md`.
- Correct status, phase, scope, sequencing, and acceptance language so it matches the repository.
- Preserve accepted architectural decisions and keep `organization_id` as the tenant-security boundary.

## Prohibited Changes

- Do not change Python files, tests, migrations, dependencies, frontend code, or infrastructure code.
- Do not implement document ingestion or any other application feature in this task.
- Do not copy Financial Analyst Copilot code into this repository.
- Do not rewrite the project into a different architecture or add unrelated formatting changes.
- Do not commit or push changes.

## Acceptance Criteria

- The four specified Markdown files accurately distinguish implemented work from planned work.
- The current phase is document-ingestion vertical-slice preparation.
- The next milestone and its sequence are explicit: authentication security review, document persistence, PDF/DOCX extraction, chunking, embedding generation, pgvector storage, and tenant-scoped retrieval.
- Sprint 02 is focused only on the secure document-ingestion foundation.
- The Version 1 boundary and long-term product vision remain consistent with the accepted decision log.
- `git diff --check` passes, only the four specified Markdown files are modified, and no commit or push is made.

## Exact Next Engineering Task

Authentication authorization review for the existing authentication endpoints and tenant context, followed by the secure, tenant-scoped document-ingestion foundation defined in Sprint 02.