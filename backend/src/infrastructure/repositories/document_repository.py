"""Document repository implementation using SQLAlchemy 2.x."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from infrastructure.db.models import Document


MAX_DOCUMENT_LIST_LIMIT = 100


class DocumentRepository:
    """Repository for tenant-scoped document persistence operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, organization_id: UUID, document: Document) -> Document:
        """Add a document entity after validating its tenant context."""
        if document.organization_id != organization_id:
            raise ValueError("document organization does not match repository organization")

        self._session.add(document)
        return document

    def get_by_id(self, organization_id: UUID, document_id: UUID) -> Document | None:
        """Return a document by tenant and document identifier."""
        statement = select(Document).where(
            Document.organization_id == organization_id,
            Document.id == document_id,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def get_by_source_identity(
        self,
        organization_id: UUID,
        source_type: str,
        source_document_key: str,
    ) -> Document | None:
        """Return a document by complete tenant-scoped source identity."""
        statement = select(Document).where(
            Document.organization_id == organization_id,
            Document.source_type == source_type,
            Document.source_document_key == source_document_key,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def list_for_organization(
        self,
        organization_id: UUID,
        *,
        limit: int = MAX_DOCUMENT_LIST_LIMIT,
        status: str | None = None,
        include_deleted: bool = False,
    ) -> list[Document]:
        """List documents for one organization in deterministic order."""
        if limit <= 0:
            raise ValueError("limit must be greater than zero")

        statement = select(Document).where(Document.organization_id == organization_id)
        if not include_deleted:
            statement = statement.where(Document.deleted_at.is_(None))
        if status is not None:
            statement = statement.where(Document.status == status)

        statement = statement.order_by(Document.created_at.desc(), Document.id.asc()).limit(
            min(limit, MAX_DOCUMENT_LIST_LIMIT)
        )
        return list(self._session.execute(statement).scalars().all())

    def update(
        self,
        organization_id: UUID,
        document_id: UUID,
        *,
        title: str | None = None,
        source_url: str | None = None,
        mime_type: str | None = None,
        checksum_latest: str | None = None,
        status: str | None = None,
        source_created_at: datetime | None = None,
        source_updated_at: datetime | None = None,
    ) -> Document | None:
        """Update controlled synchronization fields for a tenant document."""
        values: dict[str, object] = {}
        if title is not None:
            values["title"] = title
        if source_url is not None:
            values["source_url"] = source_url
        if mime_type is not None:
            values["mime_type"] = mime_type
        if checksum_latest is not None:
            values["checksum_latest"] = checksum_latest
        if status is not None:
            values["status"] = status
        if source_created_at is not None:
            values["source_created_at"] = source_created_at
        if source_updated_at is not None:
            values["source_updated_at"] = source_updated_at

        if not values:
            return self.get_by_id(organization_id=organization_id, document_id=document_id)

        statement = (
            update(Document)
            .where(
                Document.organization_id == organization_id,
                Document.id == document_id,
            )
            .values(**values)
            .returning(Document)
        )
        return self._session.execute(statement).scalar_one_or_none()

    def soft_delete(self, organization_id: UUID, document_id: UUID, deleted_at: datetime) -> Document | None:
        """Mark a tenant document as soft-deleted."""
        statement = (
            update(Document)
            .where(
                Document.organization_id == organization_id,
                Document.id == document_id,
            )
            .values(deleted_at=deleted_at)
            .returning(Document)
        )
        return self._session.execute(statement).scalar_one_or_none()

    def restore(self, organization_id: UUID, document_id: UUID) -> Document | None:
        """Restore a tenant document by clearing its soft-deletion timestamp."""
        statement = (
            update(Document)
            .where(
                Document.organization_id == organization_id,
                Document.id == document_id,
            )
            .values(deleted_at=None)
            .returning(Document)
        )
        return self._session.execute(statement).scalar_one_or_none()