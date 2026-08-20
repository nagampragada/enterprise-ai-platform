from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.ports.github_app import (
    GitHubInstallationAccessToken,
    GitHubRepository,
    GitHubRepositoryPage,
)
from application.services.github_repository_discovery_service import (
    GitHubRepositoryDiscoveryNotFound,
    GitHubRepositoryDiscoveryService,
)
from infrastructure.db.models import Connector, ConnectorCredential, GitHubAppInstallation


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
    environment = os.environ.copy()
    environment["DATABASE_URL"] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True,
        cwd=str(ROOT),
        env=environment,
    )
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
            "github_app_installations",
            "connector_credentials",
            "connectors",
            "users",
            "organization_settings",
            "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


class Client:
    app_id = 123

    def __init__(self, session):
        self.session = session
        self.token_calls = []
        self.list_calls = []

    def create_installation_access_token(self, installation_id):
        assert not self.session.in_transaction()
        self.token_calls.append(installation_id)
        return GitHubInstallationAccessToken(
            "ghs_request_scoped", NOW + timedelta(hours=1)
        )

    def list_installation_repositories(
        self, token, *, page, page_size, account_id, account_login
    ):
        assert not self.session.in_transaction()
        self.list_calls.append((token, page, page_size, account_id, account_login))
        return GitHubRepositoryPage(
            (
                GitHubRepository(
                    501,
                    "docs",
                    "fake-org/docs",
                    "fake-org",
                    True,
                    "private",
                    False,
                    False,
                    "main",
                    "https://github.com/fake-org/docs",
                    NOW,
                ),
            ),
            page,
            page_size,
            False,
            1,
        )


def _seed(factory):
    organization_a, organization_b = uuid.uuid4(), uuid.uuid4()
    user_a, user_b, connector_id, credential_id = (
        uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    )
    session = factory()
    for organization_id, name in ((organization_a, "Alpha"), (organization_b, "Beta")):
        session.execute(
            text("INSERT INTO organizations(id,name,slug) VALUES(:id,:name,:slug)"),
            {"id": organization_id, "name": name, "slug": str(organization_id)},
        )
    for user_id, organization_id in ((user_a, organization_a), (user_b, organization_b)):
        session.execute(
            text(
                "INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) "
                "VALUES(:id,:org,:email,:email,'hash','Admin')"
            ),
            {"id": user_id, "org": organization_id, "email": f"{user_id}@example.test"},
        )
    session.execute(
        text(
            "INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status,"
            "capabilities,created_by_user_id) VALUES(:id,:org,'github','GitHub',:slug,'active',"
            "CAST(:capabilities AS jsonb),:user)"
        ),
        {
            "id": connector_id,
            "org": organization_a,
            "slug": str(connector_id),
            "capabilities": '{"supports_repository_discovery":true}',
            "user": user_a,
        },
    )
    session.execute(
        text(
            "INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,"
            "auth_scheme,status,external_subject,display_label,granted_scopes,created_by_user_id) "
            "VALUES(:id,:org,:connector,'github','app_installation','active','77','fake-org',"
            "CAST(:scopes AS jsonb),:user)"
        ),
        {
            "id": credential_id,
            "org": organization_a,
            "connector": connector_id,
            "scopes": '["contents:read","metadata:read"]',
            "user": user_a,
        },
    )
    session.execute(
        text(
            "INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,"
            "github_app_id,github_installation_id,account_id,account_login,account_type,"
            "repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,"
            "created_at,updated_at) "
            "VALUES(:id,:org,:connector,:credential,123,77,99,'fake-org','Organization',"
            "'selected','connected',:now,:now,:now,:now,:now)"
        ),
        {
            "id": uuid.uuid4(),
            "org": organization_a,
            "connector": connector_id,
            "credential": credential_id,
            "now": NOW,
        },
    )
    session.commit()
    session.close()
    return organization_a, organization_b, connector_id


def test_discovery_is_tenant_safe_read_only_and_provider_io_has_no_open_transaction(factory):
    organization_a, organization_b, connector_id = _seed(factory)
    session = factory()
    client = Client(session)
    service = GitHubRepositoryDiscoveryService(session, client)

    with pytest.raises(GitHubRepositoryDiscoveryNotFound):
        service.prepare(organization_b, connector_id)
    session.rollback()

    before = (
        session.scalar(select(func.count()).select_from(Connector)),
        session.scalar(select(func.count()).select_from(ConnectorCredential)),
        session.scalar(select(func.count()).select_from(GitHubAppInstallation)),
    )
    session.rollback()
    context = service.prepare(organization_a, connector_id)
    assert session.in_transaction()
    session.rollback()
    result = service.discover(context, page=1, page_size=50)
    assert result.items[0].full_name == "fake-org/docs"
    assert client.token_calls == [77]
    assert len(client.list_calls) == 1

    after = (
        session.scalar(select(func.count()).select_from(Connector)),
        session.scalar(select(func.count()).select_from(ConnectorCredential)),
        session.scalar(select(func.count()).select_from(GitHubAppInstallation)),
    )
    assert after == before == (1, 1, 1)
    session.close()
