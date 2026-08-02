# Project Context

## Product Summary

A multi-tenant, Glean-like AI knowledge and operations platform for small and medium-sized businesses. Version 1 is intentionally focused on secure document retrieval, organization-scoped AI chat, a first Google Drive connector, and one read-only PostgreSQL intelligence workflow.

## Current Phase

Database infrastructure verification.

## Current Sprint

Sprint 01: Database foundation design.

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
- Database engine, session, and health infrastructure implemented
- First migration validated against the local PostgreSQL development container

## Current Task

Database infrastructure verification.

## Next Tasks

- Review the identity and organization schema slice next
- Confirm that database session usage patterns are ready for repository work
- Continue schema rollout with the first-release tenant and identity tables

## Implementation Status

Database infrastructure exists; no application endpoints, auth flows, or business features have been implemented.

## Known Risks

- Architecture may drift without strict adherence to repository guidance
- Early connector assumptions may affect future extensibility
- Tenant-boundary mistakes could create security rework later
- Scope growth beyond the approved Version 1 boundary could delay delivery

## Last Updated

2026-08-02