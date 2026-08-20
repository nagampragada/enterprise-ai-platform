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
from sqlalchemy.exc import DBAPIError, IntegrityError

from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
PRIOR = "20260824_000015"


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


def _config() -> Config:
    config = Config(str(INI))
    config.set_main_option("sqlalchemy.url", os.environ["TEST_DATABASE_URL"])
    return config


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
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True, cwd=str(ROOT), env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


def _setup(engine):
    organization_id, user_id, connector_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO organizations(id,name,slug) VALUES (:id,'Org',:slug)"),
            {"id": organization_id, "slug": str(organization_id)},
        )
        connection.execute(
            text("""INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name)
                    VALUES (:id,:org,:email,:email,'hash','Admin')"""),
            {"id": user_id, "org": organization_id, "email": f"{user_id}@example.com"},
        )
        connection.execute(
            text("""INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status)
                    VALUES (:id,:org,'github','GitHub',:slug,'active')"""),
            {"id": connector_id, "org": organization_id, "slug": str(connector_id)},
        )
    return organization_id, user_id, connector_id


def test_schema_matches_orm_and_removes_legacy_connector_source(engine):
    inspector = inspect(engine)
    assert len(Base.metadata.tables) == 42
    assert {"connector_credentials", "oauth_authorization_transactions"}.issubset(
        inspector.get_table_names(schema="public")
    )
    connector_columns = {item["name"] for item in inspector.get_columns("connectors")}
    assert not {"secret_reference", "credential_status", "credential_expires_at"}.intersection(
        connector_columns
    )
    for table_name in ("connector_credentials", "oauth_authorization_transactions"):
        reflected = inspector.get_columns(table_name)
        model = list(Base.metadata.tables[table_name].columns)
        assert [item["name"] for item in reflected] == [item.name for item in model]
        assert {constraint.name for constraint in Base.metadata.tables[table_name].constraints if constraint.name} == {
            inspector.get_pk_constraint(table_name)["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table_name)),
            *(item["name"] for item in inspector.get_check_constraints(table_name)),
            *(item["name"] for item in inspector.get_foreign_keys(table_name)),
        }
    credential_fks = {item["name"]: item for item in inspector.get_foreign_keys("connector_credentials")}
    assert credential_fks["fk_connector_credentials_connector_tenant"]["constrained_columns"] == [
        "organization_id", "connector_id"
    ]
    assert credential_fks["fk_connector_credentials_connector_tenant"]["options"]["ondelete"] == "CASCADE"
    assert credential_fks["fk_connector_credentials_creator_tenant"]["options"]["ondelete"] == "SET NULL (created_by_user_id)"
    oauth_fks = {item["name"]: item for item in inspector.get_foreign_keys("oauth_authorization_transactions")}
    assert oauth_fks["fk_oauth_transactions_user_tenant"]["options"]["ondelete"] == "SET NULL (initiating_user_id)"
    indexes = {item["name"]: item for item in inspector.get_indexes("oauth_authorization_transactions")}
    assert indexes["ix_oauth_transactions_pending_state"]["dialect_options"]["postgresql_where"] is not None
    assert indexes["ix_oauth_transactions_pending_expiry"]["column_names"] == ["expires_at", "id"]


def test_constraints_tenant_boundaries_and_delete_actions(engine):
    organization_id, user_id, connector_id = _setup(engine)
    credential_id, transaction_id = uuid.uuid4(), uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("""INSERT INTO connector_credentials
                (id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status,
                 granted_scopes,created_by_user_id)
                VALUES (:id,:org,:connector,'github','oauth2','vault://opaque','active',
                        '["repo:read"]'::jsonb,:user)"""),
            {"id": credential_id, "org": organization_id, "connector": connector_id, "user": user_id},
        )
        connection.execute(
            text("""INSERT INTO oauth_authorization_transactions
                (id,organization_id,connector_id,initiating_user_id,provider_key,state_hash,
                 pkce_verifier_secret_reference,callback_identifier,status,created_at,expires_at)
                VALUES (:id,:org,:connector,:user,'github',:hash,'vault://pkce','github_callback',
                        'pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP + interval '10 minutes')"""),
            {"id": transaction_id, "org": organization_id, "connector": connector_id,
             "user": user_id, "hash": b"a" * 32},
        )
        connection.execute(text("DELETE FROM users WHERE id=:id"), {"id": user_id})
        assert connection.execute(
            text("SELECT created_by_user_id FROM connector_credentials WHERE id=:id"),
            {"id": credential_id},
        ).scalar_one() is None
        assert connection.execute(
            text("SELECT initiating_user_id FROM oauth_authorization_transactions WHERE id=:id"),
            {"id": transaction_id},
        ).scalar_one() is None
        connection.execute(text("DELETE FROM connectors WHERE id=:id"), {"id": connector_id})
        assert connection.execute(text("SELECT count(*) FROM connector_credentials")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM oauth_authorization_transactions")).scalar_one() == 0

    organization_id, user_id, connector_id = _setup(engine)
    invalid_statements = (
        ("""INSERT INTO connector_credentials
            (id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status,granted_scopes)
            VALUES (:id,:org,:connector,'Bad-Key','oauth2','ref','active','[]')""", {}),
        ("""INSERT INTO connector_credentials
            (id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status,granted_scopes)
            VALUES (:id,:org,:connector,'github','oauth2','ref','active','[""]')""", {}),
        ("""INSERT INTO oauth_authorization_transactions
            (id,organization_id,connector_id,provider_key,state_hash,callback_identifier,status,created_at,expires_at)
            VALUES (:id,:org,:connector,'github',:hash,'callback','pending',CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP + interval '21 minutes')""", {"hash": b"b" * 32}),
        ("""INSERT INTO oauth_authorization_transactions
            (id,organization_id,connector_id,provider_key,state_hash,callback_identifier,status,created_at,expires_at)
            VALUES (:id,:org,:connector,'github',:hash,'callback','consumed',CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP + interval '10 minutes')""", {"hash": b"c" * 32}),
    )
    with engine.begin() as connection:
        for statement, extra in invalid_statements:
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        text(statement),
                        {"id": uuid.uuid4(), "org": organization_id, "connector": connector_id, **extra},
                    )


def test_downgrade_guard_and_reupgrade(engine):
    before = set(inspect(engine).get_table_names())
    command.downgrade(_config(), PRIOR)
    after = set(inspect(engine).get_table_names())
    assert before - after == {"connector_credentials", "oauth_authorization_transactions", "github_app_installations"}
    connector_columns = {item["name"] for item in inspect(engine).get_columns("connectors")}
    assert {"secret_reference", "credential_status", "credential_expires_at"}.issubset(connector_columns)
    organization_id, _, connector_id = _setup(engine)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE connectors SET secret_reference='vault://legacy' WHERE id=:id"),
            {"id": connector_id},
        )
    with pytest.raises(DBAPIError):
        command.upgrade(_config(), "head")
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE connectors SET secret_reference=NULL WHERE id=:id"), {"id": connector_id}
        )
    command.upgrade(_config(), "head")
    assert {"connector_credentials", "oauth_authorization_transactions"}.issubset(
        inspect(engine).get_table_names()
    )
