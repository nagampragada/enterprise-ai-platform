# Roadmap

## Version 1 Boundary

Version 1 includes a focused SaaS foundation, document ingestion and retrieval, organization-scoped chat, a single Google Drive connector, one read-only PostgreSQL connector, and a basic administration surface. It does not include broad connector coverage, workflow automation, billing, mobile applications, Kubernetes, or multi-region deployment.

## Phase 0: Foundation

- Establish repository structure and documentation baseline
- Confirm product scope, architecture guardrails, and delivery sequencing
- Define current task tracking and decision logging

## Phase 1: Database and Backend Setup

- Design the initial relational model for organizations, users, documents, chats, connectors, and audit events
- Prepare backend project bootstrap plan using FastAPI, SQLAlchemy 2.x, Alembic, and Pydantic v2
- Define repository conventions, entity boundaries, and persistence patterns

## Phase 2: Authentication and Tenancy

- Implement user authentication
- Support organization admin and employee roles
- Enforce organization_id-based tenant isolation across customer-owned records
- Add initial audit logging for key user and administrative actions

## Phase 3: Document Ingestion and RAG

- Support manual upload for PDF, DOCX, TXT, and Markdown
- Implement extraction, chunking, embedding generation, and pgvector storage
- Provide organization-scoped document indexing and document search
- Return AI answers with source citations

## Phase 4: AI Chat

- Add chat sessions and conversation history
- Support basic document question answering
- Use organization-scoped retrieval for chat responses
- Establish the first stable user-facing AI interaction flow for Version 1

## Phase 5: Google Drive Connector

- Add a read-only Google Drive connector
- Allow admins to select approved folders
- Support initial synchronization and incremental synchronization
- Exclude full file-level permission synchronization from Version 1

## Phase 6: PostgreSQL Connector and SQL Agent

- Add one read-only PostgreSQL connector
- Extract schema metadata
- Generate SQL from natural-language prompts
- Restrict execution to validated SELECT queries with row limits

## Phase 7: Admin Dashboard

- Deliver a basic admin dashboard
- Add user management and document management views
- Show connector status placeholders
- Show usage summary placeholders

## Phase 8: Testing and Pilot Deployment

- Execute focused backend, frontend, and integration testing for Version 1 scope
- Prepare Docker-based local and pilot deployment workflows
- Validate tenant isolation, retrieval quality, and connector behavior with pilot customers
- Capture Version 1 feedback before expanding scope

## Future Phases Beyond Version 1

Future phases may add additional connectors such as SharePoint and SQL Server, controlled business actions through APIs and workflows, broader operational automation, industry-specific modules, and more advanced deployment options. These remain intentionally separate from Version 1 to preserve delivery focus.
