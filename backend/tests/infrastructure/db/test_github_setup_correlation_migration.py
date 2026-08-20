from __future__ import annotations

import os
from pathlib import Path
import subprocess
import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


ROOT = Path(__file__).resolve().parents[3]
INI = ROOT / "alembic.ini"
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
PRIOR = "20260826_000017"


def config():
    value = Config(str(INI))
    value.set_main_option("sqlalchemy.url", os.environ["TEST_DATABASE_URL"])
    return value


def identity(url):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ["TEST_DATABASE_URL"]
    development = os.environ.get("DATABASE_URL")
    if development and identity(development) == identity(url):
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


def setup(connection):
    organization_id, user_id, connector_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    connection.execute(
        text("INSERT INTO organizations(id,name,slug) VALUES(:id,'Org',:slug)"),
        {"id": organization_id, "slug": str(organization_id)},
    )
    connection.execute(
        text(
            "INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) "
            "VALUES(:id,:org,:email,:email,'hash','Admin')"
        ),
        {"id": user_id, "org": organization_id, "email": f"{user_id}@example.test"},
    )
    connection.execute(
        text(
            "INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status) "
            "VALUES(:id,:org,'github','GitHub',:slug,'draft')"
        ),
        {"id": connector_id, "org": organization_id, "slug": str(connector_id)},
    )
    return organization_id, user_id, connector_id


def insert_transaction(connection, context, *, provider="github", candidate=None, setup_at=None):
    organization_id, user_id, connector_id = context
    connection.execute(
        text(
            """INSERT INTO oauth_authorization_transactions
            (id,organization_id,connector_id,initiating_user_id,provider_key,state_hash,
             pkce_verifier_secret_reference,callback_identifier,status,created_at,expires_at,
             provider_candidate_installation_id,provider_setup_completed_at)
            VALUES(:id,:org,:connector,:user,:provider,:hash,'vault://pkce','github_app_installation',
                   'pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP + interval '10 minutes',
                   :candidate,:setup_at)"""
        ),
        {
            "id": uuid.uuid4(), "org": organization_id, "connector": connector_id,
            "user": user_id, "provider": provider, "hash": os.urandom(32),
            "candidate": candidate, "setup_at": setup_at,
        },
    )


def test_schema_matches_orm_and_accepts_only_bounded_github_correlation(engine):
    columns = {item["name"] for item in inspect(engine).get_columns("oauth_authorization_transactions")}
    assert columns == set(Base.metadata.tables["oauth_authorization_transactions"].columns.keys())
    assert {"provider_candidate_installation_id", "provider_setup_completed_at"}.issubset(columns)
    with engine.begin() as connection:
        context = setup(connection)
        insert_transaction(connection, context)
        insert_transaction(
            connection, context, candidate=77,
            setup_at=connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one(),
        )
        invalid = (
            {"candidate": 77, "setup_at": None},
            {"candidate": 0, "setup_at": connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()},
            {
                "provider": "other", "candidate": 77,
                "setup_at": connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one(),
            },
        )
        for values in invalid:
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    insert_transaction(connection, context, **values)


def test_downgrade_and_reupgrade(engine):
    command.downgrade(config(), PRIOR)
    columns = {item["name"] for item in inspect(engine).get_columns("oauth_authorization_transactions")}
    assert "provider_candidate_installation_id" not in columns
    assert "provider_setup_completed_at" not in columns
    command.upgrade(config(), "head")
    columns = {item["name"] for item in inspect(engine).get_columns("oauth_authorization_transactions")}
    assert {"provider_candidate_installation_id", "provider_setup_completed_at"}.issubset(columns)
