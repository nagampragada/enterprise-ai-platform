# Project Context

## Product Summary

A multi-tenant, Glean-like AI knowledge and operations platform for small and medium-sized businesses. Version 1 is intentionally focused on secure document retrieval, organization-scoped AI chat, a first Google Drive connector, and one read-only PostgreSQL intelligence workflow.

## Current Phase

Document-ingestion vertical-slice preparation.

## Current Sprint

Sprint 02: Secure Document Ingestion Foundation.

## Important Architecture Decisions

- Separate repository from the Financial Analyst Copilot
- PostgreSQL with pgvector for storage and vector search
- FastAPI for backend services
- Next.js and TypeScript for frontend development
- Tailwind CSS and shadcn/ui for frontend UI foundations
- Organization as the tenant-security boundary
- Industry treated as metadata rather than a tenant boundary
- Modular features first, dynamic plugin system later
- Focused MVP before broader platform expansion
- Version 1 remains limited to approved scope and excludes broad connector and workflow expansion

## Completed Work

- Top-level repository structure established
- Domain-oriented backend folder scaffold established
- Frontend, connectors, workers, infrastructure, and deployment folders established
- Foundation documentation and repository guidance files created
- Version 1 vision, roadmap, and sprint planning documentation prepared
- Database architecture reviewed and simplified into a minimum secure first-release schema
- PostgreSQL engine, session, health, and Alembic infrastructure implemented
- Organization, role, user, and authentication-session models implemented
- Migrations for industries, organizations, identity, roles, and authentication sessions created
- Password hashing, JWT access tokens, hashed refresh tokens, and session management implemented
- Login, refresh, logout, logout-all, and authenticated `/me` endpoints implemented
- Admin-user bootstrap script implemented
- Connector domain contracts and a local-folder connector implemented
- Content-extraction contracts plus TXT and Markdown extractors implemented
- Related backend tests added for the implemented foundation

## Current Task

Documentation alignment and preparation for the document-ingestion vertical slice.

## Next Milestone

Secure, tenant-scoped document ingestion.

Immediate sequence:

1. Authentication security review
2. Document persistence
3. PDF and DOCX extraction
4. Chunking
5. Embedding generation
6. pgvector storage
7. Tenant-scoped retrieval

## Implementation Status

The backend foundation and authentication flows are implemented. Connector contracts, a local-folder connector, content-extraction contracts, and TXT/Markdown extraction are also implemented. Document persistence, PDF/DOCX extraction, chunking, embedding generation, pgvector storage, tenant-scoped retrieval, chat, Google Drive synchronization, the PostgreSQL intelligence connector, administration APIs, audit logging, and workflow automation remain unimplemented.

## Known Risks

- Architecture may drift without strict adherence to repository guidance
- Early connector assumptions may affect future extensibility
- Tenant-boundary mistakes could create security rework later
- Scope growth beyond the approved Version 1 boundary could delay delivery

## Last Updated

2026-08-14