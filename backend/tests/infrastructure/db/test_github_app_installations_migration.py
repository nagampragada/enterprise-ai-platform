from __future__ import annotations
import os,subprocess,uuid
from pathlib import Path
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine,inspect,text
from sqlalchemy.exc import IntegrityError
from infrastructure.db import models as db_models
from infrastructure.db.base import Base

ROOT=Path(__file__).resolve().parents[3];INI=ROOT/"alembic.ini";PYTHON=ROOT/".venv"/"Scripts"/"python.exe";PRIOR="20260825_000016"
def config():
    value=Config(str(INI));value.set_main_option("sqlalchemy.url",os.environ["TEST_DATABASE_URL"]);return value
@pytest.fixture(scope="module")
def engine():
    url=os.environ["TEST_DATABASE_URL"];reset=create_engine(url,future=True)
    with reset.begin() as c:c.execute(text("DROP SCHEMA IF EXISTS public CASCADE"));c.execute(text("CREATE SCHEMA public"))
    reset.dispose();environment=os.environ.copy();environment["DATABASE_URL"]=url
    subprocess.run([str(PYTHON),"-m","alembic","-c",str(INI),"upgrade","head"],check=True,cwd=str(ROOT),env=environment)
    value=create_engine(url,future=True);yield value;value.dispose()
def setup(c):
    org,user,connector,credential=[uuid.uuid4() for _ in range(4)]
    c.execute(text("INSERT INTO organizations(id,name,slug) VALUES(:id,'Org',:slug)"),{"id":org,"slug":str(org)})
    c.execute(text("INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) VALUES(:id,:org,:email,:email,'hash','Admin')"),{"id":user,"org":org,"email":f"{user}@example.test"})
    c.execute(text("INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status) VALUES(:id,:org,'github','GitHub',:slug,'draft')"),{"id":connector,"org":org,"slug":str(connector)})
    c.execute(text("INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status,created_by_user_id) VALUES(:id,:org,:connector,'github','app_installation',NULL,'active',:user)"),{"id":credential,"org":org,"connector":connector,"user":user})
    return org,connector,credential
def test_schema_matches_orm_and_secret_reference_is_scheme_constrained(engine):
    inspector=inspect(engine);assert "github_app_installations" in inspector.get_table_names()
    assert set(c["name"] for c in inspector.get_columns("github_app_installations"))==set(Base.metadata.tables["github_app_installations"].columns.keys())
    with engine.begin() as c:
        org,connector,credential=setup(c)
        with pytest.raises(IntegrityError):
            with c.begin_nested():c.execute(text("INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status) VALUES(:id,:org,:connector,'github','oauth2',NULL,'active')"),{"id":uuid.uuid4(),"org":org,"connector":uuid.uuid4()})
        with pytest.raises(IntegrityError):
            with c.begin_nested():c.execute(text("INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status) VALUES(:id,:org,:connector,'other','app_installation',NULL,'active')"),{"id":uuid.uuid4(),"org":org,"connector":uuid.uuid4()})
def test_binding_uniqueness_tenant_fk_and_cascade(engine):
    with engine.begin() as c:
        org,connector,credential=setup(c);params={"id":uuid.uuid4(),"org":org,"connector":connector,"credential":credential}
        other_connector,other_credential=uuid.uuid4(),uuid.uuid4()
        c.execute(text("INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status) VALUES(:id,:org,'github','Other',:slug,'draft')"),{"id":other_connector,"org":org,"slug":str(other_connector)})
        c.execute(text("INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,secret_reference,status) VALUES(:id,:org,:connector,'github','app_installation',NULL,'active')"),{"id":other_credential,"org":org,"connector":other_connector})
        with pytest.raises(IntegrityError):
            with c.begin_nested():c.execute(text("INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) VALUES(:id,:org,:connector,:credential,123,76,99,'fake-org','Organization','selected','connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"),{"id":uuid.uuid4(),"org":org,"connector":connector,"credential":other_credential})
        c.execute(text("INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) VALUES(:id,:org,:connector,:credential,123,77,99,'fake-org','Organization','selected','connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"),params)
        other_org,tenant_connector,tenant_credential=setup(c)
        with pytest.raises(IntegrityError):
            with c.begin_nested():c.execute(text("INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) VALUES(:id,:org,:connector,:credential,123,77,99,'fake-org','Organization','selected','connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"),{"id":uuid.uuid4(),"org":other_org,"connector":tenant_connector,"credential":tenant_credential})
        c.execute(text("DELETE FROM connectors WHERE id=:connector"),params)
        assert c.execute(text("SELECT count(*) FROM github_app_installations WHERE connector_id=:connector"),params).scalar_one()==0
def test_personal_installation_is_database_rejected(engine):
    with engine.begin() as c:
        org,connector,credential=setup(c)
        with pytest.raises(IntegrityError):
            with c.begin_nested():c.execute(text("INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) VALUES(:id,:org,:connector,:credential,123,77,99,'person','User','selected','connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"),{"id":uuid.uuid4(),"org":org,"connector":connector,"credential":credential})
def test_downgrade_and_reupgrade(engine):
    with engine.begin() as c:c.execute(text("DELETE FROM connector_credentials WHERE auth_scheme='app_installation'"))
    command.downgrade(config(),PRIOR);assert "github_app_installations" not in inspect(engine).get_table_names()
    command.upgrade(config(),"head");assert "github_app_installations" in inspect(engine).get_table_names()
