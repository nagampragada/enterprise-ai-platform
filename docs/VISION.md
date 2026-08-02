# Vision

## Problem Statement

Small and medium-sized businesses often store critical knowledge across documents, shared drives, and operational databases, but that information is difficult to search, trust, and act on from one secure interface. Teams lose time switching between systems, repeating questions, and manually validating answers that should be accessible through approved, organization-scoped AI workflows.

## Target Customer

Version 1 targets small and medium-sized businesses that need a practical, secure internal knowledge and operations platform without the cost or complexity of large-enterprise software. The initial focus is on organizations that rely on shared documents, Google Drive, and PostgreSQL-backed business data.

## Product Promise

Provide each organization with a secure, multi-tenant AI workspace that can retrieve approved knowledge, answer questions with citations, and expose controlled operational intelligence without compromising tenant isolation or expanding scope beyond a focused MVP.

## Version 1 Capabilities

### Core SaaS

- Multi-tenant organizations
- User authentication
- Organization admin and employee roles
- Tenant isolation using organization_id
- Basic audit logging

### Knowledge

- Manual upload for PDF, DOCX, TXT, and Markdown
- Document extraction
- Chunking
- Embedding generation
- pgvector storage
- Document search
- AI answers with source citations

### Chat

- Chat sessions
- Conversation history
- Basic document question answering
- Organization-scoped retrieval

### Administration

- Basic admin dashboard
- User management
- Document management
- Connector status placeholders
- Usage summary placeholders

### First Connector

- Google Drive
- Admin chooses approved folders
- Read-only access
- Initial synchronization
- Incremental synchronization
- No full file-level permission synchronization in Version 1

### Database Intelligence

- One read-only PostgreSQL connector
- Schema metadata extraction
- Natural-language SQL generation
- SELECT-only execution
- Query validation and row limits

### Technical Foundation

- PostgreSQL
- pgvector
- FastAPI
- SQLAlchemy 2.x
- Alembic
- Pydantic v2
- Next.js
- TypeScript
- Tailwind CSS
- shadcn/ui
- LangGraph
- Docker

## Long-Term Vision

Over time, the platform should evolve into a modular, multi-tenant AI knowledge and operations system that connects to more business systems, supports controlled business actions through APIs and workflows, and enables industry-specific modules without breaking the shared core architecture. Future expansion can include broader connector coverage, richer workflow orchestration, and more specialized vertical experiences once the Version 1 foundation is validated.

## Non-Goals

The following are explicitly out of scope for Version 1:

- SharePoint connector
- Salesforce connector
- SQL Server connector
- Billing and Stripe
- Mobile applications
- Employee GPS tracking
- Dynamic plugin marketplace
- Hotel booking
- Refund processing
- Payments
- Write access to customer databases
- Complex workflow automation
- Full Google Drive permission synchronization
- Kubernetes
- Multi-region deployment
- Hundreds of connectors
