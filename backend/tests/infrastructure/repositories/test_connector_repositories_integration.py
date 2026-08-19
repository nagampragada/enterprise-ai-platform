from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.models import Connector, ConnectorScope
from infrastructure.repositories.connector_repository import ConnectorRepository, ConnectorRepositoryConflict
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL, DEV_URL = "TEST_DATABASE_URL", "DATABASE_URL"
NOW = datetime(2026, 8, 23, tzinfo=timezone.utc)


def _identity(url):
    value = make_url(url); return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ[TEST_URL]; development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(url): raise RuntimeError("test DB must differ")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE")); connection.execute(text("CREATE SCHEMA public"))
    reset.dispose(); environment = os.environ.copy(); environment[DEV_URL] = url
    subprocess.run([str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"], check=True, cwd=str(ROOT), env=environment)
    value = create_engine(url, future=True)
    try: yield value
    finally: value.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in ("source_acl_entries", "source_acl_snapshots", "external_group_memberships", "external_directory_states", "user_external_identity_links", "external_principals", "document_indexing_attempts", "document_indexing_states", "document_version_documents", "document_versions", "connector_sync_cursors", "connector_sync_errors", "connector_sync_items", "connector_sync_runs", "source_item_scope_memberships", "source_items", "connector_scopes", "connectors", "audit_events", "knowledge_space_user_grants", "knowledge_space_team_grants", "knowledge_space_department_grants", "knowledge_space_organization_grants", "knowledge_spaces", "team_memberships", "department_memberships", "teams", "departments", "document_chunks", "documents", "authentication_sessions", "user_roles", "users", "organization_settings", "organizations", "industries"):
            connection.execute(text(f"DELETE FROM {table}"))


@pytest.fixture
def session(engine):
    value = sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)()
    try: yield value
    finally: value.rollback(); value.close()


def _org(session: Session, name: str):
    value = uuid.uuid4(); session.execute(text("INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)"), {"id":value,"name":name,"slug":f"{name.lower()}-{value}"}); return value


def _user(session: Session, org, name):
    value=uuid.uuid4(); email=f"{value}@example.com"; session.execute(text("INSERT INTO users (id,organization_id,email,normalized_email,password_hash,display_name) VALUES (:id,:org,:email,:email,'hash',:name)"),{"id":value,"org":org,"email":email,"name":name}); return value


def _space(session: Session, org, name):
    value=uuid.uuid4(); session.execute(text("INSERT INTO knowledge_spaces (id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)"),{"id":value,"org":org,"name":name,"slug":f"space-{str(value)[:8]}"}); return value


def _connector(org, slug, *, creator=None, status="draft", kind="local_folder"):
    return Connector(id=uuid.uuid4(), organization_id=org, connector_type=kind, display_name=slug, slug=slug, status=status, acl_support="none", capabilities={"supports_folders": True}, safe_config={"root_path": "/mounted/docs"}, config_schema_version=1, created_by_user_id=creator)


def _scope(org, connector, space, slug, *, creator=None, status="draft", mode="platform_managed"):
    return ConnectorScope(id=uuid.uuid4(), organization_id=org, connector_id=connector, knowledge_space_id=space, display_name=slug, slug=slug, scope_type="folder", external_scope_key=f"/{slug}", access_mode=mode, status=status, safe_config={"follow_symlinks": False}, config_schema_version=1, created_by_user_id=creator)


def test_connector_add_commit_defaults_get_and_rollback(session):
    org=_org(session,"Alpha"); creator=_user(session,org,"Creator"); repository=ConnectorRepository(session); connector=_connector(org,"local",creator=creator)
    repository.add(org,connector)
    assert connector.created_at is not None and connector.updated_at is not None
    session.commit(); assert repository.get_by_id(org,connector.id).id==connector.id; assert repository.get_by_slug(org,"local").id==connector.id
    transient=_connector(org,"rollback"); repository.add(org,transient); session.rollback(); assert repository.get_by_id(org,transient.id) is None


def test_connector_tenant_isolation_duplicate_and_creator_fk(session):
    org_a,org_b=_org(session,"Beta"),_org(session,"Gamma"); creator_b=_user(session,org_b,"Other"); repository=ConnectorRepository(session)
    first=_connector(org_a,"shared"); repository.add(org_a,first); session.commit()
    assert repository.get_by_id(org_b,first.id) is None and repository.get_by_slug(org_b,"shared") is None
    assert repository.lock_by_id(org_b,first.id) is None
    same_other=_connector(org_b,"shared"); repository.add(org_b,same_other); session.commit()
    with pytest.raises(ConnectorRepositoryConflict): repository.add(org_a,_connector(org_a,"shared"))
    session.rollback()
    with pytest.raises(ConnectorRepositoryConflict): repository.add(org_a,_connector(org_a,"foreign-creator",creator=creator_b))
    session.rollback()


def test_connector_keyset_filters_and_final_page(session):
    org=_org(session,"Delta"); repository=ConnectorRepository(session)
    connectors=[]
    for index in range(5):
        item=_connector(org,f"connector-{index}",status="active" if index%2==0 else "paused",kind="local_folder" if index<4 else "google_drive"); item.created_at=NOW; repository.add(org,item); connectors.append(item)
    session.commit(); first=repository.list_page(org,limit=2); second=repository.list_page(org,limit=2,cursor=first.next_cursor); final=repository.list_page(org,limit=2,cursor=second.next_cursor)
    ids=[x.id for x in first.items+second.items+final.items]; assert ids==sorted([x.id for x in connectors]); assert len(ids)==len(set(ids)); assert final.next_cursor is None
    assert all(x.status=="active" for x in repository.list_page(org,status="active").items)
    assert [x.connector_type for x in repository.list_page(org,connector_type="google_drive").items]==["google_drive"]


def test_connector_controlled_updates_are_caller_owned(session):
    org=_org(session,"Epsilon"); repository=ConnectorRepository(session); connector=_connector(org,"managed"); repository.add(org,connector); session.commit()
    repository.update_safe_configuration(org,connector.id,{"root_path":"/safe"},2); repository.update_validation(org,connector.id,status="active",validated_at=NOW); repository.set_status(org,connector.id,"archived",archived_at=NOW)
    changed=repository.get_by_id(org,connector.id); assert changed.safe_config=={"root_path":"/safe"} and changed.config_schema_version==2 and changed.archived_at==NOW
    session.rollback(); restored=repository.get_by_id(org,connector.id); assert restored.status=="draft" and restored.safe_config=={"root_path":"/mounted/docs"}


def test_scope_add_commit_get_rollback_and_tenant_fks(session):
    org_a,org_b=_org(session,"Zeta"),_org(session,"Eta"); creator_a,creator_b=_user(session,org_a,"A"),_user(session,org_b,"B"); space_a,space_b=_space(session,org_a,"A"),_space(session,org_b,"B")
    connector_a=_connector(org_a,"a"); ConnectorRepository(session).add(org_a,connector_a); session.commit(); repository=ConnectorScopeRepository(session)
    scope=_scope(org_a,connector_a.id,space_a,"root",creator=creator_a); repository.add(org_a,scope); session.commit(); assert repository.get_by_id(org_a,scope.id).id==scope.id; assert repository.get_by_connector_and_slug(org_a,connector_a.id,"root").id==scope.id; assert repository.get_by_id(org_b,scope.id) is None; assert repository.lock_by_id(org_b,scope.id) is None
    transient=_scope(org_a,connector_a.id,space_a,"rollback"); repository.add(org_a,transient); session.rollback(); assert repository.get_by_id(org_a,transient.id) is None
    for invalid in (_scope(org_b,connector_a.id,space_b,"bad-connector"),_scope(org_a,connector_a.id,space_b,"bad-space"),_scope(org_a,connector_a.id,space_a,"bad-creator",creator=creator_b)):
        with pytest.raises(ConnectorRepositoryConflict): repository.add(invalid.organization_id,invalid)
        session.rollback()


def test_scope_duplicate_pagination_filters_active_and_updates(session):
    org=_org(session,"Theta"); creator=_user(session,org,"Creator"); spaces=[_space(session,org,f"S{i}") for i in range(2)]; connector=_connector(org,"connector"); ConnectorRepository(session).add(org,connector); session.commit(); repository=ConnectorScopeRepository(session); scopes=[]
    for index in range(5):
        scope=_scope(org,connector.id,spaces[index%2],f"scope-{index}",creator=creator,status="active" if index<3 else "paused",mode="platform_managed" if index%2==0 else "hybrid"); scope.created_at=NOW; repository.add(org,scope); scopes.append(scope)
    session.commit()
    with pytest.raises(ConnectorRepositoryConflict): repository.add(org,_scope(org,connector.id,spaces[0],"scope-0"))
    session.rollback(); first=repository.list_page(org,limit=2); second=repository.list_page(org,limit=2,cursor=first.next_cursor); final=repository.list_page(org,limit=2,cursor=second.next_cursor); assert len({x.id for x in first.items+second.items+final.items})==5 and final.next_cursor is None
    assert all(x.connector_id==connector.id for x in repository.list_page(org,connector_id=connector.id).items); assert all(x.knowledge_space_id==spaces[0] for x in repository.list_page(org,knowledge_space_id=spaces[0]).items); assert all(x.access_mode=="hybrid" for x in repository.list_page(org,access_mode="hybrid").items)
    active=repository.list_active_for_connector(org,connector.id,limit=2); assert len(active.items)==2 and active.has_more
    target=scopes[0]; repository.update_safe_configuration(org,target.id,{"folder":"safe"},2); repository.update_validation(org,target.id,status="paused",validated_at=NOW); repository.set_status(org,target.id,"removed",removed_at=NOW); changed=repository.get_by_id(org,target.id); assert changed.safe_config=={"folder":"safe"} and changed.removed_at==NOW
    session.rollback(); restored=repository.get_by_id(org,target.id); assert restored.status=="active" and restored.connector_id==connector.id and restored.knowledge_space_id==spaces[0]


def test_atomic_connector_scope_transaction_and_caller_rollback(session):
    org=_org(session,"Iota"); space=_space(session,org,"Space"); connector_repository=ConnectorRepository(session); scope_repository=ConnectorScopeRepository(session)
    connector=_connector(org,"atomic"); connector_repository.add(org,connector); scope= _scope(org,connector.id,space,"atomic-root"); scope_repository.add(org,scope); session.commit(); assert connector_repository.get_by_id(org,connector.id) and scope_repository.get_by_id(org,scope.id)
    failing=_connector(org,"failing"); connector_repository.add(org,failing)
    with pytest.raises(ConnectorRepositoryConflict): scope_repository.add(org,_scope(org,failing.id,uuid.uuid4(),"invalid-space"))
    session.rollback(); assert connector_repository.get_by_id(org,failing.id) is None
