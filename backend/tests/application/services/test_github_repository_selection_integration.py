from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import threading
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.ports.github_app import (
    GitHubBranchReference,
    GitHubCommitReference,
    GitHubInstallationAccessToken,
    GitHubRepository,
    GitHubRepositoryAccessGrant,
    GitHubRepositoryPage,
)
from application.services.github_repository_content_service import (
    GitHubRepositoryContentNotFound,
    GitHubRepositoryContentService,
)
from application.services.github_repository_selection_service import (
    GitHubRepositorySelectionConflict,
    GitHubRepositorySelectionNotFound,
    GitHubRepositorySelectionService,
)
from infrastructure.db.models import ConnectorScope, Document, SourceItem


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)


def _identity(url):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ["TEST_DATABASE_URL"]
    development = os.environ.get("DATABASE_URL")
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy(); environment["DATABASE_URL"] = url
    subprocess.run([str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True, cwd=str(ROOT), env=environment)
    value = create_engine(url, future=True)
    yield value
    value.dispose()


@pytest.fixture
def factory(engine):
    return sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in (
            "documents", "source_items", "connector_scopes", "github_app_installations",
            "connector_credentials", "connectors", "knowledge_spaces", "users",
            "organization_settings", "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def repository():
    return GitHubRepository(501, "docs", "fake-org/docs", "fake-org", True,
        "private", False, False, "main", "https://github.com/fake-org/docs", NOW)


class Client:
    app_id = 123

    def __init__(self, session):
        self.session = session; self.token_calls = []; self.list_calls = []

    def create_repository_access_token(self, installation_id, repository_id, *, account_id, account_login):
        assert not self.session.in_transaction()
        self.token_calls.append((installation_id, repository_id, account_id, account_login))
        return GitHubRepositoryAccessGrant(
            GitHubInstallationAccessToken("ghs_request_scoped", NOW + timedelta(hours=1)),
            repository(),
        )

    def list_installation_repositories(self, token, *, page, page_size, account_id, account_login):
        assert not self.session.in_transaction()
        self.list_calls.append((page, page_size, account_id, account_login))
        return GitHubRepositoryPage((repository(),), 1, 1, False, 1)

    def create_repository_content_access_token(
        self, installation_id, repository_id, *, account_id, account_login
    ):
        assert not self.session.in_transaction()
        self.token_calls.append((installation_id, repository_id, account_id, account_login))
        return GitHubRepositoryAccessGrant(
            GitHubInstallationAccessToken("ghs_request_scoped", NOW + timedelta(hours=1)),
            repository(),
        )

    def get_branch_reference(self, token, **kwargs):
        assert not self.session.in_transaction()
        return GitHubBranchReference("main", "a" * 40, "b" * 40)

    def get_commit_reference(self, token, **kwargs):
        assert not self.session.in_transaction()
        return GitHubCommitReference("a" * 40, "b" * 40)


def _seed(factory):
    org_a, org_b, user_id, connector_id, credential_id = (
        uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    )
    spaces = (uuid.uuid4(), uuid.uuid4())
    with factory() as session:
        for org, name in ((org_a, "Alpha"), (org_b, "Beta")):
            session.execute(text("INSERT INTO organizations(id,name,slug) VALUES(:id,:name,:slug)"),
                {"id":org,"name":name,"slug":str(org)})
        session.execute(text("INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) "
            "VALUES(:id,:org,:email,:email,'hash','Admin')"),
            {"id":user_id,"org":org_a,"email":f"{user_id}@example.test"})
        for index, space_id in enumerate(spaces):
            session.execute(text("INSERT INTO knowledge_spaces(id,organization_id,name,slug,status) "
                "VALUES(:id,:org,:name,:slug,'active')"),
                {"id":space_id,"org":org_a,"name":f"Space {index}","slug":f"space-{index}"})
        session.execute(text("INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status,capabilities,created_by_user_id) "
            "VALUES(:id,:org,'github','GitHub',:slug,'active',CAST(:capabilities AS jsonb),:user)"),
            {"id":connector_id,"org":org_a,"slug":str(connector_id),
             "capabilities":'{"supports_repository_discovery":true,"supports_repository_selection":true}',"user":user_id})
        session.execute(text("INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,status,external_subject,display_label,granted_scopes,created_by_user_id) "
            "VALUES(:id,:org,:connector,'github','app_installation','active','77','fake-org',CAST(:scopes AS jsonb),:user)"),
            {"id":credential_id,"org":org_a,"connector":connector_id,
             "scopes":'["contents:read","metadata:read"]',"user":user_id})
        session.execute(text("INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) "
            "VALUES(:id,:org,:connector,:credential,123,77,99,'fake-org','Organization','selected','connected',:now,:now,:now,:now,:now)"),
            {"id":uuid.uuid4(),"org":org_a,"connector":connector_id,
             "credential":credential_id,"now":NOW})
        session.commit()
    return org_a, org_b, user_id, connector_id, spaces


def _verified(service, session, org, connector, space):
    context=service.prepare(org,connector,space,501)
    assert session.in_transaction();session.rollback()
    return context,service.verify(context)


def test_selection_is_tenant_safe_staged_and_creates_only_one_scope(factory):
    org, other_org, user, connector, spaces = _seed(factory)
    session=factory();client=Client(session);service=GitHubRepositorySelectionService(session,client,clock=lambda:NOW)
    with pytest.raises(GitHubRepositorySelectionNotFound):
        service.prepare(other_org,connector,spaces[0],501)
    session.rollback()
    context,repo=_verified(service,session,org,connector,spaces[0])
    view=service.persist(context,repo,user);session.commit()
    assert view.repository_id==501 and view.status=="active"
    assert client.token_calls==[(77,501,99,"fake-org")] and len(client.list_calls)==1
    assert session.scalar(select(func.count()).select_from(ConnectorScope))==1
    assert session.scalar(select(func.count()).select_from(SourceItem))==0
    assert session.scalar(select(func.count()).select_from(Document))==0
    for table in ("connector_sync_jobs", "connector_sync_runs", "document_chunks"):
        assert session.scalar(text(f"SELECT count(*) FROM {table}"))==0
    scope=session.scalar(select(ConnectorScope))
    assert scope.external_scope_key=="github:repository:501"
    assert set(scope.safe_config)=={"repository_id","repository_name","repository_full_name",
        "owner_login","private","visibility","archived","disabled","default_branch"}
    session.close()


def test_duplicate_reactivation_listing_and_deselection_are_local_and_idempotent(factory):
    org, _, user, connector, spaces = _seed(factory)
    session=factory();client=Client(session);service=GitHubRepositorySelectionService(session,client,clock=lambda:NOW)
    context,repo=_verified(service,session,org,connector,spaces[0])
    first=service.persist(context,repo,user);session.commit()
    context,repo=_verified(service,session,org,connector,spaces[0])
    duplicate=service.persist(context,repo,user);session.commit()
    assert duplicate.scope_id==first.scope_id
    with pytest.raises(GitHubRepositorySelectionConflict):
        moved=GitHubRepositorySelectionService(session,client,clock=lambda:NOW)
        context2,repo2=_verified(moved,session,org,connector,spaces[1])
        moved.persist(context2,repo2,user)
    session.rollback()
    before_calls=(len(client.token_calls),len(client.list_calls))
    removed=service.deselect(org,connector,first.scope_id);session.commit()
    removed_again=service.deselect(org,connector,first.scope_id);session.commit()
    page=service.list(org,connector,limit=20)
    assert removed.status==removed_again.status=="removed"
    assert page.items[0].scope_id==first.scope_id and page.items[0].status=="removed"
    assert (len(client.token_calls),len(client.list_calls))==before_calls
    context,repo=_verified(service,session,org,connector,spaces[0])
    active=service.persist(context,repo,user);session.commit()
    assert active.scope_id==first.scope_id and active.status=="active"
    session.close()


def test_rollback_restores_removed_state(factory):
    org, _, user, connector, spaces = _seed(factory)
    session=factory();client=Client(session);service=GitHubRepositorySelectionService(session,client,clock=lambda:NOW)
    context,repo=_verified(service,session,org,connector,spaces[0])
    scope=service.persist(context,repo,user);session.commit()
    service.deselect(org,connector,scope.scope_id);session.commit()
    context,repo=_verified(service,session,org,connector,spaces[0])
    service.persist(context,repo,user);session.rollback();session.close()
    with factory() as check:
        assert check.get(ConnectorScope,scope.scope_id).status=="removed"


def test_concurrent_identical_and_different_space_requests_serialize(factory):
    org, _, user, connector, spaces = _seed(factory)
    barrier=threading.Barrier(2)
    def select_space(space):
        session=factory();client=Client(session);service=GitHubRepositorySelectionService(session,client,clock=lambda:NOW)
        context,repo=_verified(service,session,org,connector,space);barrier.wait()
        try:
            result=service.persist(context,repo,user);session.commit();return ("ok",result.scope_id)
        except GitHubRepositorySelectionConflict:
            session.rollback();return ("conflict",None)
        finally: session.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        identical=list(pool.map(select_space,(spaces[0],spaces[0])))
    assert [status for status,_ in identical]==["ok","ok"]
    assert len({scope_id for _,scope_id in identical})==1
    with factory() as cleanup:
        cleanup.execute(text("DELETE FROM connector_scopes"));cleanup.commit()
    barrier=threading.Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        competing=list(pool.map(select_space,spaces))
    assert sorted(status for status,_ in competing)==["conflict","ok"]
    with factory() as check:
        assert check.scalar(select(func.count()).select_from(ConnectorScope))==1


def test_concurrent_select_and_deselect_leave_one_consistent_scope(factory):
    org, _, user, connector, spaces = _seed(factory)
    seed_session=factory();seed_client=Client(seed_session)
    seed_service=GitHubRepositorySelectionService(seed_session,seed_client,clock=lambda:NOW)
    context,repo=_verified(seed_service,seed_session,org,connector,spaces[0])
    scope=seed_service.persist(context,repo,user);seed_session.commit();seed_session.close()
    barrier=threading.Barrier(2)
    def select_again():
        session=factory();service=GitHubRepositorySelectionService(session,Client(session),clock=lambda:NOW)
        context,repo=_verified(service,session,org,connector,spaces[0]);barrier.wait()
        result=service.persist(context,repo,user);session.commit();session.close();return result.status
    def deselect():
        session=factory();service=GitHubRepositorySelectionService(session,clock=lambda:NOW)
        barrier.wait();result=service.deselect(org,connector,scope.scope_id)
        session.commit();session.close();return result.status
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=(pool.submit(select_again),pool.submit(deselect))
        outcomes=[future.result() for future in futures]
    assert sorted(outcomes)==["active","removed"]
    with factory() as check:
        rows=tuple(check.scalars(select(ConnectorScope)).all())
        assert len(rows)==1 and rows[0].status in {"active","removed"}
        assert (rows[0].status=="removed") == (rows[0].removed_at is not None)


def test_content_authorization_is_tenant_safe_read_only_and_provider_runs_after_rollback(factory):
    org, other_org, user, connector, spaces = _seed(factory)
    session = factory()
    client = Client(session)
    selection = GitHubRepositorySelectionService(session, client, clock=lambda: NOW)
    context, repo = _verified(selection, session, org, connector, spaces[0])
    selected = selection.persist(context, repo, user)
    session.commit()
    reader = GitHubRepositoryContentService(session, client)
    with pytest.raises(GitHubRepositoryContentNotFound):
        reader.authorize(other_org, connector, selected.scope_id)
    session.rollback()
    authorization = reader.authorize(org, connector, selected.scope_id)
    assert session.in_transaction()
    before = {
        table: session.scalar(text(f"SELECT count(*) FROM {table}"))
        for table in ("source_items", "documents", "connector_sync_runs")
    }
    session.rollback()
    result = reader.resolve_default_branch_snapshot(authorization)
    assert result.repository_id == 501 and result.commit_object_id == "a" * 40
    after = {
        table: session.scalar(text(f"SELECT count(*) FROM {table}"))
        for table in ("source_items", "documents", "connector_sync_runs")
    }
    assert after == before == {"source_items": 0, "documents": 0, "connector_sync_runs": 0}
    session.close()
