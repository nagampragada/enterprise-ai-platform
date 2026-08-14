# Product Name Placeholder

## Product Vision

Build an enterprise AI platform that helps organizations securely search, reason over, and operationalize their internal knowledge and workflows across connected systems.

## Core Capabilities

- Federated search across enterprise data sources
- AI-assisted knowledge retrieval and summarization
- Workflow support for teams and administrators
- Organization-scoped tenant isolation and security boundaries
- Extensible connector and integration model

## Planned Technology Stack

- Backend: FastAPI, Python, Pydantic v2, SQLAlchemy 2.x
- Frontend: Next.js, TypeScript, React
- Database: PostgreSQL with pgvector
- Caching and queues: Redis
- Infrastructure: Docker, Kubernetes, GitHub Actions

## Repository Structure

```text
backend/         Backend service, domain, infrastructure, and tests
connectors/      Connector-specific implementations and experiments
deployment/      Environment-specific deployment definitions
docs/            Product, architecture, and delivery documentation
frontend/        Web application shell and UI structure
infra/           Shared infrastructure templates and automation
plugins/         Plugin contracts and registry foundation
scripts/         Repository and operational scripts
workers/         Background worker foundation
```

## Current Development Status

The repository has moved beyond the initial scaffold and database-foundation phase. The following foundation work is implemented:

- PostgreSQL engine, sessions, health checks, and Alembic integration
- Organization, role, user, and authentication-session models and migrations
- Argon2 password hashing and JWT access tokens
- Hashed, rotating refresh tokens with authentication-session management
- Login, refresh, logout, logout-all, and authenticated `/me` endpoints
- Admin-user bootstrap script
- Connector domain contracts and a security-conscious local-folder connector
- Content-extraction contracts with TXT and Markdown extractors
- Backend tests for the implemented foundation, authentication, connector, and extraction behavior

The project is entering the document-ingestion vertical-slice phase. The next milestone is secure, tenant-scoped document ingestion, including document persistence, PDF and DOCX extraction, deterministic chunking, embedding generation, pgvector storage, and organization-scoped retrieval.

The following capabilities are not implemented yet: document and document-chunk persistence, PDF and DOCX extraction, chunking, embeddings, pgvector indexing and retrieval, search, citations, AI chat, Google Drive synchronization, the read-only PostgreSQL intelligence connector, administration APIs, audit logging, and workflow automation.

The long-term product vision remains a multi-tenant AI knowledge and operations platform with connected enterprise sources, controlled retrieval and workflows, and future industry-specific modules. Version 1 remains intentionally focused on secure document retrieval, organization-scoped AI chat, a first Google Drive connector, and one read-only PostgreSQL intelligence workflow. Broader connectors, complex workflow automation, billing, Kubernetes, and other explicitly documented non-goals remain outside that boundary.