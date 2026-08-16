from __future__ import annotations
import os, subprocess, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from alembic import command
from alembic.config import Config
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL, DEV_URL = "TEST_DATABASE_URL", "DATABASE_URL"
PRIOR = "20260821_000012"
NOW = datetime(2026, 8, 22, tzinfo=timezone.utc)
TABLES = {"external_principals", "user_external_identity_links", "external_directory_states", "external_group_memberships", "source_acl_snapshots", "source_acl_entries"}


def _identity(url):
    value = make_url(url); return value.drivername, value.host, value.port, value.database

def _required(name):
    value = os.environ.get(name)
    if not value: raise RuntimeError(f"{name} must be set")
    return value

def _upgrade(url):
    env = os.environ.copy(); env[DEV_URL] = url
    subprocess.run([str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"], check=True, cwd=str(ROOT), env=env)

def _config():
    value = Config(str(INI)); value.set_main_option("sqlalchemy.url", _required(TEST_URL)); return value

@pytest.fixture(scope="module")
def engine():
    test_url = _required(TEST_URL); development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(test_url): raise RuntimeError("test DB must differ")
    reset = create_engine(test_url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE")); connection.execute(text("CREATE SCHEMA public"))
    reset.dispose(); _upgrade(test_url); value = create_engine(test_url, future=True)
    try: yield value
    finally: value.dispose()

@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in ("source_acl_entries", "source_acl_snapshots", "external_group_memberships", "external_directory_states", "user_external_identity_links", "external_principals", "document_indexing_attempts", "document_indexing_states", "document_version_documents", "document_versions", "connector_sync_cursors", "connector_sync_errors", "connector_sync_items", "connector_sync_runs", "source_item_scope_memberships", "source_items", "connector_scopes", "connectors", "audit_events", "knowledge_space_user_grants", "knowledge_space_team_grants", "knowledge_space_department_grants", "knowledge_space_organization_grants", "knowledge_spaces", "team_memberships", "department_memberships", "teams", "departments", "document_chunks", "documents", "authentication_sessions", "user_roles", "users", "organization_settings", "organizations", "industries"):
            connection.execute(text(f"DELETE FROM {table}"))

def _exec(engine, sql, **params):
    with engine.begin() as connection: return connection.execute(text(sql), params)

def _count(engine, table):
    with engine.connect() as connection: return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()

def _setup(engine, name="Alpha"):
    org, connector, space, scope, source = (uuid.uuid4() for _ in range(5))
    _exec(engine, "INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)", id=org,name=name,slug=f"{name.lower()}-{org}")
    _exec(engine, "INSERT INTO connectors (id,organization_id,connector_type,display_name,slug,acl_support) VALUES (:id,:org,'google_drive',:name,:slug,'complete')", id=connector,org=org,name=name,slug=f"connector-{str(connector)[:8]}")
    _exec(engine, "INSERT INTO knowledge_spaces (id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)", id=space,org=org,name=name,slug=f"space-{str(space)[:8]}")
    _exec(engine, "INSERT INTO connector_scopes (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode) VALUES (:id,:org,:connector,:space,:name,:slug,'drive','root','source_acl')", id=scope,org=org,connector=connector,space=space,name=name,slug=f"scope-{str(scope)[:8]}")
    _exec(engine, "INSERT INTO source_items (id,organization_id,connector_id,source_item_key,source_item_type,title,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:key,'file',:key,:now,:now)", id=source,org=org,connector=connector,key=f"file-{source}",now=NOW)
    return org, connector, scope, source

def _user(engine, org, email):
    value=uuid.uuid4(); _exec(engine,"INSERT INTO users (id,organization_id,email,normalized_email,password_hash,display_name) VALUES (:id,:org,:email,:email,'hash',:email)",id=value,org=org,email=email); return value

def _principal(engine, org, connector, key, *, kind="user", email=None, domain=None, login=None, lifecycle="active", deleted=None, metadata="{}"):
    value=uuid.uuid4(); _exec(engine,"""INSERT INTO external_principals (id,organization_id,connector_id,principal_key,principal_type,normalized_email,normalized_domain,provider_login,lifecycle,first_seen_at,last_seen_at,deleted_at,metadata) VALUES (:id,:org,:connector,:key,:kind,:email,:domain,:login,:lifecycle,:now,:now,:deleted,CAST(:metadata AS jsonb))""",id=value,org=org,connector=connector,key=key,kind=kind,email=email,domain=domain,login=login,lifecycle=lifecycle,now=NOW,deleted=deleted,metadata=metadata); return value

def _snapshot(engine, org, connector, source, version, *, status="building", current=False, completed=None, captured=None, inheritance="unknown", error_category=None,error_code=None,run=None,item=None):
    value=uuid.uuid4(); _exec(engine,"""INSERT INTO source_acl_snapshots (id,organization_id,connector_id,source_item_id,snapshot_version,connector_sync_run_id,connector_sync_item_id,status,is_current,started_at,completed_at,captured_at,inheritance_completeness,error_category,error_code) VALUES (:id,:org,:connector,:source,:version,:run,:item,:status,:current,:now,:completed,:captured,:inheritance,:error_category,:error_code)""",id=value,org=org,connector=connector,source=source,version=version,run=run,item=item,status=status,current=current,now=NOW,completed=completed,captured=captured,inheritance=inheritance,error_category=error_category,error_code=error_code); return value

def _entry(engine,org,connector,source,snapshot,principal,*,effect="allow",permission="viewer",read=True,key=None,expires=None,metadata="{}"):
    value=uuid.uuid4(); _exec(engine,"""INSERT INTO source_acl_entries (id,organization_id,connector_id,source_item_id,acl_snapshot_id,external_principal_id,provider_permission_key,effect,permission_level,grants_read,expires_at,metadata) VALUES (:id,:org,:connector,:source,:snapshot,:principal,:key,:effect,:permission,:read,:expires,CAST(:metadata AS jsonb))""",id=value,org=org,connector=connector,source=source,snapshot=snapshot,principal=principal,key=key,effect=effect,permission=permission,read=read,expires=expires,metadata=metadata); return value

def _sync(engine,org,connector,scope):
    run,item=uuid.uuid4(),uuid.uuid4(); _exec(engine,"INSERT INTO connector_sync_runs (id,organization_id,connector_id,connector_scope_id,mode,trigger_type) VALUES (:id,:org,:connector,:scope,'incremental','manual')",id=run,org=org,connector=connector,scope=scope); _exec(engine,"INSERT INTO connector_sync_items (id,organization_id,connector_id,connector_scope_id,sync_run_id,source_item_key,change_type) VALUES (:id,:org,:connector,:scope,:run,:key,'changed')",id=item,org=org,connector=connector,scope=scope,run=run,key=str(item)); return run,item


def test_schema_matches_orm_and_prior_schema(engine):
    inspector=inspect(engine); assert TABLES.issubset(inspector.get_table_names(schema="public")); assert {"users","roles","departments","teams","knowledge_spaces","connectors","source_items","connector_sync_runs","document_versions","document_chunks","audit_events"}.issubset(inspector.get_table_names(schema="public")); assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname='vector'")).scalar_one()==1
    for table in TABLES:
        reflected=inspector.get_columns(table,schema="public"); model=list(Base.metadata.tables[table].columns); assert [x.name for x in model]==[x["name"] for x in reflected]
        for m,d in zip(model,reflected,strict=True): assert m.type._type_affinity is d["type"]._type_affinity; assert m.nullable==d["nullable"]; assert (m.server_default is None)==(d["default"] is None)
        model_names={c.name for c in Base.metadata.tables[table].constraints if c.name}; db_names={inspector.get_pk_constraint(table,schema="public")["name"],*(x["name"] for x in inspector.get_unique_constraints(table,schema="public")),*(x["name"] for x in inspector.get_check_constraints(table,schema="public")),*(x["name"] for x in inspector.get_foreign_keys(table,schema="public"))}; assert model_names==db_names
        assert {x.name for x in Base.metadata.tables[table].indexes}.issubset({x["name"] for x in inspector.get_indexes(table,schema="public")})


def test_principal_identity_types_lifecycle_and_json(engine):
    org,connector,_,_=_setup(engine); assert _principal(engine,org,connector,"UserA",email="a@example.com"); assert _principal(engine,org,connector,"usera",email="b@example.com")
    with pytest.raises(IntegrityError): _principal(engine,org,connector,"UserA")
    other_org,other_connector,_,_=_setup(engine,"Beta"); assert _principal(engine,other_org,other_connector,"UserA")
    invalid=(({"key":"any","kind":"anyone","email":"a@example.com"}),({"key":"domain","kind":"domain"}),({"key":"domain2","kind":"domain","domain":"EXAMPLE.COM"}),({"key":"email","email":"A@EXAMPLE.COM"}),({"key":"deleted","lifecycle":"deleted"}),({"key":"active-deleted","deleted":NOW}),({"key":"json","metadata":"[]"}))
    for params in invalid:
        key=params.pop("key")
        with pytest.raises(IntegrityError): _principal(engine,org,connector,key,**params)
    assert _principal(engine,org,connector,"domain-ok",kind="domain",domain="example.com")


def test_identity_links_and_creator_behavior(engine):
    org,connector,_,_=_setup(engine); user1,user2,creator=_user(engine,org,"one@example.com"),_user(engine,org,"two@example.com"),_user(engine,org,"creator@example.com"); principal=_principal(engine,org,connector,"subject")
    link=uuid.uuid4(); _exec(engine,"INSERT INTO user_external_identity_links (id,organization_id,connector_id,user_id,external_principal_id,verification_method,status,verified_at,created_by_user_id) VALUES (:id,:org,:connector,:user,:principal,'admin','verified',:now,:creator)",id=link,org=org,connector=connector,user=user1,principal=principal,now=NOW,creator=creator)
    with pytest.raises(IntegrityError): _exec(engine,"INSERT INTO user_external_identity_links (id,organization_id,connector_id,user_id,external_principal_id,verification_method,status,verified_at) VALUES (:id,:org,:connector,:user,:principal,'admin','verified',:now)",id=uuid.uuid4(),org=org,connector=connector,user=user2,principal=principal,now=NOW)
    for status,verified,revoked in (("pending",NOW,None),("verified",None,None),("revoked",None,None)):
        with pytest.raises(IntegrityError): _exec(engine,"INSERT INTO user_external_identity_links (id,organization_id,connector_id,user_id,external_principal_id,verification_method,status,verified_at,revoked_at) VALUES (:id,:org,:connector,:user,:principal,'admin',:status,:verified,:revoked)",id=uuid.uuid4(),org=org,connector=connector,user=user2,principal=uuid.uuid4(),status=status,verified=verified,revoked=revoked)
    _exec(engine,"DELETE FROM users WHERE id=:id",id=creator)
    with engine.connect() as connection: assert connection.execute(text("SELECT created_by_user_id FROM user_external_identity_links WHERE id=:id"),{"id":link}).scalar_one() is None
    other_org,other_connector,_,_=_setup(engine,"CrossTenant"); other_principal=_principal(engine,other_org,other_connector,"foreign")
    with pytest.raises(IntegrityError): _exec(engine,"INSERT INTO user_external_identity_links (id,organization_id,connector_id,user_id,external_principal_id,verification_method) VALUES (:id,:org,:connector,:user,:principal,'admin')",id=uuid.uuid4(),org=org,connector=other_connector,user=user2,principal=other_principal)


def test_directory_and_group_generation_semantics(engine):
    org,connector,_,_=_setup(engine); group=_principal(engine,org,connector,"group",kind="group"); nested=_principal(engine,org,connector,"nested",kind="group"); member=_principal(engine,org,connector,"member")
    state=uuid.uuid4(); _exec(engine,"INSERT INTO external_directory_states (id,organization_id,connector_id,status) VALUES (:id,:org,:connector,'not_started')",id=state,org=org,connector=connector)
    _exec(engine,"UPDATE external_directory_states SET status='syncing',in_progress_generation=1,started_at=:now WHERE id=:id",id=state,now=NOW)
    _exec(engine,"UPDATE external_directory_states SET status='complete',current_generation=1,in_progress_generation=NULL,completed_at=:now,last_successful_at=:now WHERE id=:id",id=state,now=NOW)
    with pytest.raises(IntegrityError): _exec(engine,"UPDATE external_directory_states SET status='syncing',in_progress_generation=1 WHERE id=:id",id=state)
    _exec(engine,"UPDATE external_directory_states SET status='failed',in_progress_generation=2,error_category='authorization',error_code='directory_denied' WHERE id=:id",id=state)
    with engine.connect() as connection: assert connection.execute(text("SELECT current_generation FROM external_directory_states WHERE id=:id"),{"id":state}).scalar_one()==1
    for group_id,member_id in ((group,member),(group,nested)):
        _exec(engine,"INSERT INTO external_group_memberships (id,organization_id,connector_id,group_principal_id,member_principal_id,first_seen_generation,last_seen_generation,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:group,:member,1,1,:now,:now)",id=uuid.uuid4(),org=org,connector=connector,group=group_id,member=member_id,now=NOW)
    with pytest.raises(IntegrityError): _exec(engine,"INSERT INTO external_group_memberships (id,organization_id,connector_id,group_principal_id,member_principal_id,first_seen_generation,last_seen_generation,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:group,:group,1,1,:now,:now)",id=uuid.uuid4(),org=org,connector=connector,group=group,now=NOW)
    other_org,other_connector,_,_=_setup(engine,"GroupCross"); foreign_member=_principal(engine,other_org,other_connector,"foreign")
    with pytest.raises(IntegrityError): _exec(engine,"INSERT INTO external_group_memberships (id,organization_id,connector_id,group_principal_id,member_principal_id,first_seen_generation,last_seen_generation,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:group,:member,1,1,:now,:now)",id=uuid.uuid4(),org=org,connector=connector,group=group,member=foreign_member,now=NOW)


def test_acl_snapshots_fail_closed_and_promote_atomically(engine):
    org,connector,scope,source=_setup(engine); complete=_snapshot(engine,org,connector,source,1,status="complete",current=True,completed=NOW,captured=NOW,inheritance="complete")
    with pytest.raises(IntegrityError): _snapshot(engine,org,connector,source,2,status="complete",current=True,completed=NOW,captured=NOW,inheritance="complete")
    for kwargs in ({"status":"building","current":True},{"status":"complete","current":True,"completed":NOW,"captured":NOW,"inheritance":"partial"},{"status":"failed","current":True,"completed":NOW,"error_category":"authorization","error_code":"denied"},{"status":"stale","current":True}):
        with pytest.raises(IntegrityError): _snapshot(engine,org,connector,source,2,**kwargs)
    failed=_snapshot(engine,org,connector,source,2,status="failed",completed=NOW,error_category="authorization",error_code="denied")
    with engine.connect() as connection: assert connection.execute(text("SELECT is_current FROM source_acl_snapshots WHERE id=:id"),{"id":complete}).scalar_one() is True
    replacement=_snapshot(engine,org,connector,source,3,status="complete",completed=NOW,captured=NOW,inheritance="complete")
    with engine.begin() as connection:
        connection.execute(text("UPDATE source_acl_snapshots SET is_current=false WHERE id=:id"),{"id":complete}); connection.execute(text("UPDATE source_acl_snapshots SET is_current=true WHERE id=:id"),{"id":replacement})
    run,item=_sync(engine,org,connector,scope); attributed=_snapshot(engine,org,connector,source,4,status="building",run=run,item=item); _exec(engine,"DELETE FROM connector_sync_items WHERE id=:id",id=item)
    with engine.connect() as connection: assert connection.execute(text("SELECT connector_sync_item_id FROM source_acl_snapshots WHERE id=:id"),{"id":attributed}).scalar_one() is None
    _exec(engine,"DELETE FROM connector_sync_runs WHERE id=:id",id=run)
    with engine.connect() as connection: assert connection.execute(text("SELECT connector_sync_run_id FROM source_acl_snapshots WHERE id=:id"),{"id":attributed}).scalar_one() is None


def test_acl_entries_safety_wiring_and_retention(engine):
    org,connector,_,source=_setup(engine); principal=_principal(engine,org,connector,"subject"); snapshot=_snapshot(engine,org,connector,source,1,status="complete",current=True,completed=NOW,captured=NOW,inheritance="complete")
    assert _entry(engine,org,connector,source,snapshot,principal,expires=NOW+timedelta(days=1))
    for effect,permission,read in (("deny","viewer",True),("allow","unknown",True),("allow","viewer",False)):
        with pytest.raises(IntegrityError): _entry(engine,org,connector,source,snapshot,principal,effect=effect,permission=permission,read=read,key=str(uuid.uuid4()))
    with pytest.raises(IntegrityError): _entry(engine,org,connector,source,snapshot,principal)
    other_org,other_connector,_,other_source=_setup(engine,"AclCross"); other_principal=_principal(engine,other_org,other_connector,"foreign"); other_snapshot=_snapshot(engine,other_org,other_connector,other_source,1,status="complete",current=True,completed=NOW,captured=NOW,inheritance="complete")
    with pytest.raises(IntegrityError): _entry(engine,org,connector,source,other_snapshot,principal,key="cross-snapshot")
    with pytest.raises(IntegrityError): _entry(engine,org,connector,source,snapshot,other_principal,key="cross-principal")
    with pytest.raises(IntegrityError): _exec(engine,"DELETE FROM external_principals WHERE id=:id",id=principal)
    _exec(engine,"DELETE FROM source_items WHERE id=:id",id=source)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM source_acl_snapshots WHERE source_item_id=:source"),{"source":source}).scalar_one()==0
        assert connection.execute(text("SELECT count(*) FROM source_acl_entries WHERE source_item_id=:source"),{"source":source}).scalar_one()==0
    with engine.connect() as connection: assert connection.execute(text("SELECT count(*) FROM external_principals WHERE id=:id"),{"id":principal}).scalar_one()==1
    _exec(engine,"DELETE FROM connectors WHERE id=:id",id=connector)
    with engine.connect() as connection: assert connection.execute(text("SELECT count(*) FROM external_principals WHERE id=:id"),{"id":principal}).scalar_one()==0
    purge_org,purge_connector,_,purge_source=_setup(engine,"DirectPurge"); purge_principal=_principal(engine,purge_org,purge_connector,"subject"); purge_snapshot=_snapshot(engine,purge_org,purge_connector,purge_source,1,status="complete",current=True,completed=NOW,captured=NOW,inheritance="complete"); _entry(engine,purge_org,purge_connector,purge_source,purge_snapshot,purge_principal)
    _exec(engine,"DELETE FROM connectors WHERE id=:id",id=purge_connector)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM source_acl_snapshots WHERE connector_id=:connector"),{"connector":purge_connector}).scalar_one()==0
        assert connection.execute(text("SELECT count(*) FROM source_acl_entries WHERE connector_id=:connector"),{"connector":purge_connector}).scalar_one()==0


def test_downgrade_removes_only_slice_and_reupgrade(engine):
    command.downgrade(_config(),PRIOR); inspector=inspect(engine); tables=set(inspector.get_table_names(schema="public")); assert not TABLES.intersection(tables); assert {"users","roles","knowledge_spaces","connectors","source_items","connector_sync_runs","document_versions","document_chunks","audit_events"}.issubset(tables)
    command.upgrade(_config(),"head"); assert TABLES.issubset(inspect(engine).get_table_names(schema="public"))
