from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from infrastructure.db.models import Connector, ConnectorScope
from infrastructure.repositories.connector_repository import (
    ConnectorPageCursor,
    ConnectorRepository,
    ConnectorRepositoryConflict,
    InvalidConnectorRepositoryRequest,
    MAX_CONNECTOR_PAGE_LIMIT,
)
from infrastructure.repositories.connector_scope_repository import (
    ConnectorScopePageCursor,
    ConnectorScopeRepository,
)

NOW = datetime(2026, 8, 23, tzinfo=timezone.utc)


def _connector(org=None, index=0):
    return Connector(id=uuid4(), organization_id=org or uuid4(), connector_type="local_folder", display_name="Local", slug=f"local-{index}", status="draft", acl_support="none", capabilities={}, safe_config={}, config_schema_version=1, created_at=NOW)


def _scope(org=None, index=0):
    return ConnectorScope(id=uuid4(), organization_id=org or uuid4(), connector_id=uuid4(), knowledge_space_id=uuid4(), display_name="Root", slug=f"root-{index}", scope_type="folder", external_scope_key=f"/root/{index}", access_mode="platform_managed", status="draft", safe_config={}, config_schema_version=1, created_at=NOW)


def _session_with_rows(rows):
    session = Mock(); session.execute.return_value.scalars.return_value.all.return_value = rows; return session


@pytest.mark.parametrize("repository_method,args", [
    ("connector_get", ("bad", uuid4())), ("connector_get", (uuid4(), "bad")),
    ("scope_get", ("bad", uuid4())), ("scope_get", (uuid4(), "bad")),
])
def test_invalid_uuids_fail_before_execution(repository_method, args):
    session = Mock()
    repository = ConnectorRepository(session) if repository_method.startswith("connector") else ConnectorScopeRepository(session)
    with pytest.raises(InvalidConnectorRepositoryRequest): repository.get_by_id(*args)
    session.execute.assert_not_called()


@pytest.mark.parametrize("method,args", [
    ("connector_slug", (uuid4(), "Bad_Slug")),
    ("scope_slug", (uuid4(), uuid4(), "Bad_Slug")),
])
def test_invalid_slugs_fail_before_execution(method, args):
    session = Mock(); repository = ConnectorRepository(session) if method.startswith("connector") else ConnectorScopeRepository(session)
    call = repository.get_by_slug if method == "connector_slug" else repository.get_by_connector_and_slug
    with pytest.raises(InvalidConnectorRepositoryRequest): call(*args)
    session.execute.assert_not_called()


@pytest.mark.parametrize("limit", [0, -1, True, MAX_CONNECTOR_PAGE_LIMIT + 1])
def test_invalid_limits_fail_before_execution(limit):
    for repository in (ConnectorRepository(Mock()), ConnectorScopeRepository(Mock())):
        with pytest.raises(InvalidConnectorRepositoryRequest): repository.list_page(uuid4(), limit=limit)
        repository._session.execute.assert_not_called()


def test_invalid_cursor_json_and_filters_fail_before_execution():
    connector_session, scope_session = Mock(), Mock()
    connector_repository, scope_repository = ConnectorRepository(connector_session), ConnectorScopeRepository(scope_session)
    with pytest.raises(InvalidConnectorRepositoryRequest): connector_repository.list_page(uuid4(), cursor=ConnectorPageCursor(datetime.now(), uuid4()))
    with pytest.raises(InvalidConnectorRepositoryRequest): scope_repository.list_page(uuid4(), cursor=ConnectorScopePageCursor(datetime.now(), uuid4()))
    with pytest.raises(InvalidConnectorRepositoryRequest): connector_repository.update_safe_configuration(uuid4(), uuid4(), [], 1)  # type: ignore[arg-type]
    with pytest.raises(InvalidConnectorRepositoryRequest): connector_repository.update_safe_configuration(uuid4(), uuid4(), {"bad": float("nan")}, 1)
    with pytest.raises(InvalidConnectorRepositoryRequest): connector_repository.list_page(uuid4(), status="unknown")
    with pytest.raises(InvalidConnectorRepositoryRequest): scope_repository.list_page(uuid4(), access_mode="public")
    connector_session.execute.assert_not_called(); scope_session.execute.assert_not_called()


def test_list_pages_are_immutable_bounded_and_use_limit_plus_one():
    org = uuid4(); connectors = [_connector(org, i) for i in range(3)]; scopes = [_scope(org, i) for i in range(3)]
    connector_session, scope_session = _session_with_rows(connectors), _session_with_rows(scopes)
    connector_page = ConnectorRepository(connector_session).list_page(org, limit=2)
    scope_page = ConnectorScopeRepository(scope_session).list_page(org, limit=2)
    assert connector_page.items == tuple(connectors[:2]) and connector_page.has_more and connector_page.next_cursor is not None
    assert scope_page.items == tuple(scopes[:2]) and scope_page.has_more and scope_page.next_cursor is not None
    with pytest.raises(FrozenInstanceError): connector_page.limit = 3  # type: ignore[misc]
    connector_sql = str(connector_session.execute.call_args.args[0]); scope_sql = str(scope_session.execute.call_args.args[0])
    assert "LIMIT" in connector_sql and "LIMIT" in scope_sql
    assert connector_session.execute.call_args.args[0]._limit_clause.value == 3
    assert scope_session.execute.call_args.args[0]._limit_clause.value == 3


def test_empty_and_final_pages_have_no_cursor():
    org = uuid4()
    assert ConnectorRepository(_session_with_rows([])).list_page(org, limit=2).next_cursor is None
    page = ConnectorScopeRepository(_session_with_rows([_scope(org)])).list_page(org, limit=2)
    assert page.has_more is False and page.next_cursor is None


def test_lock_queries_are_tenant_scoped_for_update():
    for repository in (ConnectorRepository(Mock()), ConnectorScopeRepository(Mock())):
        repository._session.execute.return_value.scalar_one_or_none.return_value = None
        repository.lock_by_id(uuid4(), uuid4())
        sql = str(repository._session.execute.call_args.args[0]).upper()
        assert "ORGANIZATION_ID" in sql and "FOR UPDATE" in sql


def test_add_flushes_maps_conflict_and_never_commits_or_rolls_back():
    org = uuid4(); session = Mock(); repository = ConnectorRepository(session); connector = _connector(org)
    repository.add(org, connector)
    session.add.assert_called_once_with(connector); session.flush.assert_called_once(); session.commit.assert_not_called(); session.rollback.assert_not_called()
    session.flush.side_effect = IntegrityError("sql", {}, Exception("secret detail"))
    with pytest.raises(ConnectorRepositoryConflict) as error: repository.add(org, _connector(org, 2))
    assert str(error.value) == "connector could not be created" and isinstance(error.value.__cause__, IntegrityError)


def test_scope_validation_and_no_generic_patch_api():
    session = Mock(); repository = ConnectorScopeRepository(session); org = uuid4(); scope = _scope(org)
    repository.add(org, scope); session.flush.assert_called_once()
    assert not hasattr(repository, "update") and not hasattr(repository, "patch")
    assert not hasattr(ConnectorRepository(Mock()), "update")
