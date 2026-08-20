from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import threading
from urllib.parse import parse_qs, urlencode, urlparse
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.ports.github_app import GitHubInstallation, GitHubUser, GitHubUserAccessToken
from application.ports.secret_store import SecretNotFound, SecretReference, SecretValue
from application.services.github_app_installation_service import (
    GitHubAppInstallationService,
    GitHubInstallationRejected,
)
from infrastructure.db.models import (
    Connector,
    ConnectorCredential,
    GitHubAppInstallation,
    OAuthAuthorizationTransaction,
)


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
        check=True, cwd=str(ROOT), env=environment,
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
            "github_app_installations", "oauth_authorization_transactions",
            "connector_credentials", "connectors", "users", "organization_settings",
            "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


class Store:
    def __init__(self):
        self._lock = threading.Lock()
        self.values = {}
        self.deleted = []

    def store(self, secret):
        reference = f"test://pkce/{uuid.uuid4()}"
        with self._lock:
            self.values[reference] = secret.value
        return SecretReference(reference)

    def retrieve(self, reference):
        with self._lock:
            value = self.values.get(reference.value)
        if value is None:
            raise SecretNotFound()
        return SecretValue(value)

    def delete(self, reference):
        with self._lock:
            if reference.value not in self.values:
                raise SecretNotFound()
            self.deleted.append(reference.value)
            del self.values[reference.value]


class Client:
    app_id = 123
    web_base_url = "https://github.test"
    client_id = "Iv1.client-id"
    callback_url = "https://platform.test/api/v1/connectors/github/callback"

    def __init__(self, *, installation=None):
        self.installation = installation or GitHubInstallation(
            77, 123, 99, "fake-org", "Organization", "selected",
            (("contents", "read"), ("metadata", "read")), NOW, NOW,
        )
        self.exchange_count = 0
        self._lock = threading.Lock()

    def build_installation_url(self, state):
        return f"https://github.test/apps/test/installations/new?{urlencode({'state': state})}"

    def build_authorization_url(self, state, challenge):
        return "https://github.test/login/oauth/authorize?" + urlencode(
            {
                "client_id": "Iv1.client-id",
                "redirect_uri": "https://platform.test/api/v1/connectors/github/callback",
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )

    def exchange_authorization_code(self, code, verifier):
        with self._lock:
            self.exchange_count += 1
        return GitHubUserAccessToken("ghu_temporary")

    def get_authenticated_user(self, token):
        return GitHubUser(55, "installer")

    def list_user_installations(self, token):
        return (self.installation,)

    def verify_installation(self, installation_id):
        return self.installation


def seed(factory):
    organization_id, user_id, connector_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    session = factory()
    session.execute(
        text("INSERT INTO organizations(id,name,slug) VALUES(:id,'Org',:slug)"),
        {"id": organization_id, "slug": str(organization_id)},
    )
    session.execute(
        text(
            "INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) "
            "VALUES(:id,:org,:email,:email,'hash','Admin')"
        ),
        {"id": user_id, "org": organization_id, "email": f"{user_id}@example.test"},
    )
    session.execute(
        text(
            "INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status) "
            "VALUES(:id,:org,'github','GitHub',:slug,'draft')"
        ),
        {"id": connector_id, "org": organization_id, "slug": str(connector_id)},
    )
    session.commit()
    session.close()
    return organization_id, user_id, connector_id


def prepare_setup(factory, store, client):
    organization_id, user_id, connector_id = seed(factory)
    session = factory()
    service = GitHubAppInstallationService(session, store, client, clock=lambda: NOW)
    initiated = service.initiate(organization_id, connector_id, user_id)
    state = parse_qs(urlparse(initiated.installation_url).query)["state"][0]
    session.commit()
    service.complete_setup(state=state, installation_id=77, setup_action="install")
    session.commit()
    session.close()
    return connector_id, state


def test_browser_flow_persists_candidate_before_binding_then_consumes_atomically(factory):
    store, client = Store(), Client()
    connector_id, state = prepare_setup(factory, store, client)
    session = factory()
    transaction = session.scalar(select(OAuthAuthorizationTransaction))
    assert transaction.provider_candidate_installation_id == 77
    assert transaction.provider_setup_completed_at == NOW
    assert transaction.status == "pending"
    assert session.scalar(select(func.count()).select_from(ConnectorCredential)) == 0
    assert session.scalar(select(func.count()).select_from(GitHubAppInstallation)) == 0
    session.close()

    session = factory()
    result = GitHubAppInstallationService(
        session, store, client, clock=lambda: NOW
    ).complete_callback(state=state, code="temporary-code")
    assert result.connected
    session.commit()
    assert session.scalar(select(OAuthAuthorizationTransaction.status)) == "consumed"
    assert session.get(Connector, connector_id).status == "active"
    assert session.scalar(select(func.count()).select_from(ConnectorCredential)) == 1
    assert session.scalar(select(func.count()).select_from(GitHubAppInstallation)) == 1
    assert client.exchange_count == 1 and not store.values and len(store.deleted) == 1
    rendered = repr(tuple(session.scalars(select(OAuthAuthorizationTransaction))))
    assert all(value not in rendered for value in (state, "temporary-code", "ghu_temporary"))
    session.close()


def test_concurrent_callbacks_exchange_once_and_have_one_atomic_winner(factory):
    store, client = Store(), Client()
    _, state = prepare_setup(factory, store, client)
    barrier = threading.Barrier(2)
    outcomes = []

    def complete():
        session = factory()
        try:
            barrier.wait()
            GitHubAppInstallationService(
                session, store, client, clock=lambda: NOW
            ).complete_callback(state=state, code="temporary-code")
            session.commit()
            outcomes.append("connected")
        except GitHubInstallationRejected:
            session.rollback()
            outcomes.append("rejected")
        finally:
            session.close()

    threads = [threading.Thread(target=complete) for _ in range(2)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    assert sorted(outcomes) == ["connected", "rejected"]
    assert client.exchange_count == 1


def test_failed_verification_rolls_back_without_activation_or_state_consumption(factory):
    store = Store()
    client = Client(
        installation=GitHubInstallation(
            77, 123, 99, "person", "User", "selected",
            (("contents", "read"), ("metadata", "read")), NOW, NOW,
        )
    )
    connector_id, state = prepare_setup(factory, store, client)
    session = factory()
    with pytest.raises(GitHubInstallationRejected):
        GitHubAppInstallationService(
            session, store, client, clock=lambda: NOW
        ).complete_callback(state=state, code="temporary-code")
    session.rollback()
    assert session.scalar(select(OAuthAuthorizationTransaction.status)) == "pending"
    assert session.get(Connector, connector_id).status == "draft"
    assert session.scalar(select(func.count()).select_from(ConnectorCredential)) == 0
    assert session.scalar(select(func.count()).select_from(GitHubAppInstallation)) == 0
    assert client.exchange_count == 1 and not store.values
    session.close()
