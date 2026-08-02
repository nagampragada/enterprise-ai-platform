# Decision Log

## DEC-001: Use a Separate Repository from the Financial Analyst Copilot

- Status: Accepted
- Context: The new platform has broader product goals, different architectural boundaries, and a separate lifecycle from the Financial Analyst Copilot.
- Decision: Create and maintain this platform in its own repository.
- Consequences: Isolation improves maintainability, release management, and governance, but shared patterns must be documented explicitly rather than assumed.

## DEC-002: Use PostgreSQL and pgvector

- Status: Accepted
- Context: The platform needs reliable transactional storage plus support for semantic retrieval use cases.
- Decision: Use PostgreSQL as the primary database and pgvector for vector search capabilities.
- Consequences: This provides a strong operational foundation and supports retrieval workloads, but it introduces database extension and hosting requirements.

## DEC-003: Use FastAPI for the Backend

- Status: Accepted
- Context: The backend requires a modern Python framework with strong typing, validation support, and async-friendly request handling.
- Decision: Use FastAPI for backend APIs and service composition.
- Consequences: Development speed and API ergonomics improve, but the team must stay disciplined about layered architecture and avoid framework leakage into domain code.

## DEC-004: Use Next.js and TypeScript for the Frontend

- Status: Accepted
- Context: The frontend needs a modern web application framework with structured routing and strong type safety.
- Decision: Use Next.js with TypeScript for the web application.
- Consequences: The stack supports scalable UI development, but it adds a build pipeline and requires clear frontend architecture standards.

## DEC-005: Organization Is the Tenant-Security Boundary

- Status: Accepted
- Context: The system must enforce a clear boundary for data ownership, access control, and isolation.
- Decision: Treat the organization as the core tenant and security boundary.
- Consequences: Data models and authorization rules become clearer, but every customer-owned record must consistently carry organization context.

## DEC-006: Industry Is Organization Metadata, Not a Tenant Boundary

- Status: Accepted
- Context: Industry classification is useful for configuration and analytics, but it does not define ownership or isolation.
- Decision: Model industry as organization metadata rather than a separate tenant dimension.
- Consequences: The tenancy model remains simpler, though vertical-specific behavior must be designed without compromising shared architecture.

## DEC-007: Start with Modular Features Before Building a Dynamic Plugin System

- Status: Accepted
- Context: The platform will likely need extensibility, but a plugin system adds complexity before core product boundaries are proven.
- Decision: Implement focused modular features first and defer a dynamic plugin system until the architecture needs it.
- Consequences: The MVP can move faster with less infrastructure complexity, but some future refactoring may be required when plugin abstractions are introduced.

## DEC-008: Start with a Focused MVP Rather than Cloning All of Glean

- Status: Accepted
- Context: Attempting to reproduce a large established platform upfront would create unnecessary scope and delivery risk.
- Decision: Define and build a focused MVP around the most important use cases first.
- Consequences: Delivery becomes more realistic and learnings arrive sooner, but some stakeholder expectations may need active scope management.