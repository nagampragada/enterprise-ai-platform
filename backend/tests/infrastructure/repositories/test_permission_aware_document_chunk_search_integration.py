from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.repositories.document_chunk_repository import EMBEDDING_DIMENSION
from infrastructure.repositories.permission_aware_document_chunk_search_repository import (
    MAX_GROUP_DEPTH,
    SEARCH_SQL,
    PermissionAwareDocumentChunkSearchRepository,
)

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
NOW = datetime.now(timezone.utc)
MODEL = "text-embedding-3-small"


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


def _vector(first: float, second: float = 0.0) -> list[float]:
    return [first, second] + [0.0] * (EMBEDDING_DIMENSION - 2)


@pytest.fixture(scope="module")
def engine():
    url = os.environ[TEST_URL]
    development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test DB must differ")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy(); environment[DEV_URL] = url
    subprocess.run([str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"], check=True, cwd=str(ROOT), env=environment)
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in (
            "source_acl_entries", "source_acl_snapshots", "external_group_memberships", "external_directory_states",
            "user_external_identity_links", "external_principals", "document_indexing_attempts", "document_indexing_states",
            "document_version_documents", "document_versions", "connector_sync_cursors", "connector_sync_errors",
            "connector_sync_items", "connector_sync_runs", "source_item_scope_memberships", "source_items",
            "connector_scopes", "connectors", "audit_events", "knowledge_space_user_grants",
            "knowledge_space_team_grants", "knowledge_space_department_grants", "knowledge_space_organization_grants",
            "knowledge_spaces", "team_memberships", "department_memberships", "teams", "departments",
            "document_chunks", "documents", "authentication_sessions", "user_roles", "users",
            "organization_settings", "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


@pytest.fixture
def session(engine):
    value = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)()
    try:
        yield value
    finally:
        value.rollback(); value.close()


def _exec(session: Session, sql: str, **params):
    return session.execute(text(sql), params)


def _tenant(session: Session, name: str):
    org, user = uuid.uuid4(), uuid.uuid4()
    _exec(session, "INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)", id=org, name=name, slug=f"{name.lower()}-{org}")
    _exec(session, "INSERT INTO users (id,organization_id,email,normalized_email,password_hash,display_name) VALUES (:id,:org,:email,:email,'hash',:name)", id=user, org=org, email=f"{str(user)[:8]}@example.com", name=name)
    return org, user


def _content_path(session: Session, org: UUID, *, mode="platform_managed", chunk_vector=None, source_status="active", membership_status="active", scope_status="active", connector_status="active", space_status="active", version_current=True, version_lifecycle="available", document_status="ready", indexing_status="indexed", embedding_model=MODEL):
    connector, space, scope, source, version, document, state, chunk = (uuid.uuid4() for _ in range(8))
    _exec(session, "INSERT INTO connectors (id,organization_id,connector_type,display_name,slug,status,acl_support) VALUES (:id,:org,'google_drive','Drive',:slug,:status,'complete')", id=connector, org=org, slug=f"connector-{str(connector)[:8]}", status=connector_status)
    _exec(session, "INSERT INTO knowledge_spaces (id,organization_id,name,slug,status,archived_at) VALUES (:id,:org,'Space',:slug,:status,:archived)", id=space, org=org, slug=f"space-{str(space)[:8]}", status=space_status, archived=NOW if space_status == "archived" else None)
    _exec(session, "INSERT INTO connector_scopes (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode,status) VALUES (:id,:org,:connector,:space,'Scope',:slug,'drive',:key,:mode,:status)", id=scope, org=org, connector=connector, space=space, slug=f"scope-{str(scope)[:8]}", key=f"root-{scope}", mode=mode, status=scope_status)
    _exec(session, "INSERT INTO source_items (id,organization_id,connector_id,source_item_key,source_item_type,title,first_seen_at,last_seen_at,status,deleted_at) VALUES (:id,:org,:connector,:key,'file','Source',:now,:now,:status,:deleted)", id=source, org=org, connector=connector, key=f"source-{source}", now=NOW, status=source_status, deleted=NOW if source_status == "deleted" else None)
    _exec(session, "INSERT INTO source_item_scope_memberships (id,organization_id,connector_id,source_item_id,connector_scope_id,status,first_discovered_at,last_seen_at,removed_at) VALUES (:id,:org,:connector,:source,:scope,:status,:now,:now,:removed)", id=uuid.uuid4(), org=org, connector=connector, source=source, scope=scope, status=membership_status, now=NOW, removed=NOW if membership_status == "removed" else None)
    _exec(session, "INSERT INTO document_versions (id,organization_id,connector_id,source_item_id,version_number,version_cause,lifecycle,is_current,discovered_at) VALUES (:id,:org,:connector,:source,1,'discovered',:lifecycle,:current,:now)", id=version, org=org, connector=connector, source=source, lifecycle=version_lifecycle, current=version_current, now=NOW)
    _exec(session, "INSERT INTO documents (id,organization_id,source_type,source_document_key,title,status) VALUES (:id,:org,'google_drive',:key,'Document',:status)", id=document, org=org, key=f"document-{document}", status=document_status)
    _exec(session, "INSERT INTO document_version_documents (id,organization_id,document_version_id,document_id) VALUES (:id,:org,:version,:document)", id=uuid.uuid4(), org=org, version=version, document=document)
    if indexing_status is not None:
        status = indexing_status
        completed = NOW if status in ("indexed", "failed") else None
        started = NOW if status in ("processing", "indexed", "failed") else None
        indexed = 1 if status == "indexed" else None
        error_category = "embedding" if status == "failed" else None
        error_code = "failed" if status == "failed" else None
        _exec(session, """INSERT INTO document_indexing_states (id,organization_id,document_version_id,extraction_profile,extraction_version,chunking_profile,chunking_version,embedding_provider,embedding_model,embedding_dimensions,profile_fingerprint,desired_generation,indexed_generation,status,reason,last_error_category,last_error_code,requested_at,started_at,completed_at) VALUES (:id,:org,:version,'default','v1','deterministic','v1','openai',:model,1536,:fingerprint,1,:indexed,:status,'new_version',:error_category,:error_code,:now,:started,:completed)""", id=state, org=org, version=version, model=MODEL, fingerprint=f"profile-{state}", indexed=indexed, status=status, error_category=error_category, error_code=error_code, now=NOW, started=started, completed=completed)
    if chunk_vector is not None:
        _exec(session, "INSERT INTO document_chunks (id,organization_id,document_id,chunk_index,chunk_text,content_hash,embedding,embedding_model) VALUES (:id,:org,:document,0,:content,:hash,CAST(:embedding AS vector),:model)", id=chunk, org=org, document=document, content=f"authorized-{chunk}", hash=str(chunk), embedding="[" + ",".join(str(v) for v in chunk_vector) + "]", model=embedding_model)
    else:
        _exec(session, "INSERT INTO document_chunks (id,organization_id,document_id,chunk_index,chunk_text,content_hash) VALUES (:id,:org,:document,0,:content,:hash)", id=chunk, org=org, document=document, content=f"authorized-{chunk}", hash=str(chunk))
    return {"connector": connector, "space": space, "scope": scope, "source": source, "version": version, "document": document, "chunk": chunk}


def _grant(session: Session, org: UUID, user: UUID, space: UUID, kind="user", *, active=True):
    revoked = None if active else NOW
    if kind == "organization":
        _exec(session, "INSERT INTO knowledge_space_organization_grants (id,organization_id,knowledge_space_id,permission_level,granted_at,revoked_at) VALUES (:id,:org,:space,'viewer',:now,:revoked)", id=uuid.uuid4(), org=org, space=space, now=NOW, revoked=revoked)
    elif kind == "user":
        _exec(session, "INSERT INTO knowledge_space_user_grants (id,organization_id,knowledge_space_id,user_id,permission_level,granted_at,revoked_at) VALUES (:id,:org,:space,:user,'viewer',:now,:revoked)", id=uuid.uuid4(), org=org, space=space, user=user, now=NOW, revoked=revoked)
    else:
        target = uuid.uuid4()
        table, membership, target_column = (("departments", "department_memberships", "department_id") if kind == "department" else ("teams", "team_memberships", "team_id"))
        grant_table = f"knowledge_space_{kind}_grants"
        _exec(session, f"INSERT INTO {table} (id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)", id=target, org=org, name=kind, slug=f"{kind}-{str(target)[:8]}")
        _exec(session, f"INSERT INTO {membership} (id,organization_id,{target_column},user_id,responsibility,status,effective_from) VALUES (:id,:org,:target,:user,'member','active',:now)", id=uuid.uuid4(), org=org, target=target, user=user, now=NOW)
        _exec(session, f"INSERT INTO {grant_table} (id,organization_id,knowledge_space_id,{target_column},permission_level,granted_at,revoked_at) VALUES (:id,:org,:space,:target,'viewer',:now,:revoked)", id=uuid.uuid4(), org=org, space=space, target=target, now=NOW, revoked=revoked)


def _principal(session: Session, org: UUID, connector: UUID, kind: str, key: str, *, email=None, domain=None):
    value = uuid.uuid4()
    _exec(session, "INSERT INTO external_principals (id,organization_id,connector_id,principal_key,principal_type,normalized_email,normalized_domain,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:key,:kind,:email,:domain,:now,:now)", id=value, org=org, connector=connector, key=key, kind=kind, email=email, domain=domain, now=NOW)
    return value


def _verified_link(session: Session, org: UUID, connector: UUID, user: UUID, principal: UUID, status="verified"):
    _exec(session, "INSERT INTO user_external_identity_links (id,organization_id,connector_id,user_id,external_principal_id,verification_method,status,verified_at,revoked_at) VALUES (:id,:org,:connector,:user,:principal,'admin',:status,:verified,:revoked)", id=uuid.uuid4(), org=org, connector=connector, user=user, principal=principal, status=status, verified=NOW if status == "verified" else None, revoked=NOW if status == "revoked" else None)


def _snapshot(session: Session, org: UUID, connector: UUID, source: UUID, *, status="complete", current=True, inheritance="complete"):
    value = uuid.uuid4()
    complete = status in ("complete", "failed")
    _exec(session, "INSERT INTO source_acl_snapshots (id,organization_id,connector_id,source_item_id,snapshot_version,status,is_current,started_at,completed_at,captured_at,inheritance_completeness,error_category,error_code) VALUES (:id,:org,:connector,:source,:version,:status,:current,:now,:completed,:captured,:inheritance,:error_category,:error_code)", id=value, org=org, connector=connector, source=source, version=int(str(value.int)[:8]), status=status, current=current, now=NOW, completed=NOW if complete else None, captured=NOW if status == "complete" else None, inheritance=inheritance, error_category="authorization" if status == "failed" else None, error_code="failed" if status == "failed" else None)
    return value


def _acl(session: Session, org: UUID, connector: UUID, source: UUID, snapshot: UUID, principal: UUID, *, effect="allow", permission="viewer", read=True, expires=None):
    _exec(session, "INSERT INTO source_acl_entries (id,organization_id,connector_id,source_item_id,acl_snapshot_id,external_principal_id,provider_permission_key,effect,permission_level,grants_read,expires_at) VALUES (:id,:org,:connector,:source,:snapshot,:principal,:key,:effect,:permission,:read,:expires)", id=uuid.uuid4(), org=org, connector=connector, source=source, snapshot=snapshot, principal=principal, key=str(uuid.uuid4()), effect=effect, permission=permission, read=read, expires=expires)


def _search(session: Session, org: UUID, user: UUID, vector=None, limit=10):
    return PermissionAwareDocumentChunkSearchRepository(session).search(org, user, vector or _vector(1.0), MODEL, limit)


@pytest.mark.parametrize("grant_kind", ["organization", "department", "team", "user"])
def test_platform_grant_paths_allow_without_role_bypass(session: Session, grant_kind: str):
    org, user = _tenant(session, f"Platform-{grant_kind}")
    path = _content_path(session, org, mode="platform_managed", chunk_vector=_vector(1.0))
    _grant(session, org, user, path["space"], grant_kind)
    session.flush()
    assert [result.chunk_id for result in _search(session, org, user)] == [path["chunk"]]
    other = _user_without_grant(session, org)
    assert bool(_search(session, org, other)) is (grant_kind == "organization")


def _user_without_grant(session: Session, org: UUID):
    value = uuid.uuid4(); email=f"{value}@example.com"
    _exec(session, "INSERT INTO users (id,organization_id,email,normalized_email,password_hash,display_name) VALUES (:id,:org,:email,:email,'hash','Other')", id=value, org=org, email=email)
    return value


def test_platform_role_alone_does_not_grant_content(session: Session):
    org, user = _tenant(session, "RoleOnly")
    _content_path(session, org, mode="platform_managed", chunk_vector=_vector(1.0))
    role_id = session.execute(text("SELECT id FROM roles WHERE name='organization_admin'")).scalar_one()
    _exec(session, "INSERT INTO user_roles (id,organization_id,user_id,role_id) VALUES (:id,:org,:user,:role)", id=uuid.uuid4(), org=org, user=user, role=role_id)
    session.flush()
    assert _search(session, org, user) == ()


def test_inactive_platform_grant_and_cross_tenant_chunks_deny(session: Session):
    org, user = _tenant(session, "Tenant-A")
    path = _content_path(session, org, chunk_vector=_vector(1.0))
    _grant(session, org, user, path["space"], active=False)
    other_org, other_user = _tenant(session, "Tenant-B")
    other_path = _content_path(session, other_org, chunk_vector=_vector(1.0))
    _grant(session, other_org, other_user, other_path["space"])
    session.flush()
    assert _search(session, org, user) == ()


@pytest.mark.parametrize("link_status,expected", [("pending", False), ("revoked", False), ("verified", True)])
def test_direct_external_identity_status(session: Session, link_status: str, expected: bool):
    org, user = _tenant(session, f"Identity-{link_status}")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    principal = _principal(session, org, path["connector"], "user", "subject", email=f"{user}@example.com")
    _verified_link(session, org, path["connector"], user, principal, link_status)
    snapshot = _snapshot(session, org, path["connector"], path["source"])
    _acl(session, org, path["connector"], path["source"], snapshot, principal)
    session.flush()
    assert bool(_search(session, org, user)) is expected


def test_email_similarity_without_link_and_other_tenant_user_deny(session: Session):
    org, user = _tenant(session, "NoLink")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    principal = _principal(session, org, path["connector"], "user", "subject", email=f"{user}@example.com")
    snapshot = _snapshot(session, org, path["connector"], path["source"])
    _acl(session, org, path["connector"], path["source"], snapshot, principal)
    session.flush()
    assert _search(session, org, user) == ()


def test_direct_and_nested_group_resolution_uses_completed_generation(session: Session):
    org, user = _tenant(session, "Groups")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    external_user = _principal(session, org, path["connector"], "user", "user")
    child_group = _principal(session, org, path["connector"], "group", "child")
    parent_group = _principal(session, org, path["connector"], "group", "parent")
    _verified_link(session, org, path["connector"], user, external_user)
    _exec(session, "INSERT INTO external_directory_states (id,organization_id,connector_id,status,current_generation,completed_at,last_successful_at) VALUES (:id,:org,:connector,'complete',1,:now,:now)", id=uuid.uuid4(), org=org, connector=path["connector"], now=NOW)
    for group, member in ((child_group, external_user), (parent_group, child_group), (child_group, parent_group)):
        _exec(session, "INSERT INTO external_group_memberships (id,organization_id,connector_id,group_principal_id,member_principal_id,first_seen_generation,last_seen_generation,first_seen_at,last_seen_at) VALUES (:id,:org,:connector,:group,:member,1,1,:now,:now)", id=uuid.uuid4(), org=org, connector=path["connector"], group=group, member=member, now=NOW)
    snapshot = _snapshot(session, org, path["connector"], path["source"])
    _acl(session, org, path["connector"], path["source"], snapshot, parent_group)
    session.flush()
    assert bool(_search(session, org, user))
    _exec(session, "UPDATE external_directory_states SET current_generation=NULL,last_successful_at=NULL,status='failed',error_category='authorization',error_code='failed' WHERE connector_id=:connector", connector=path["connector"])
    session.flush()
    assert _search(session, org, user) == ()


def test_domain_and_anyone_require_complete_current_snapshot(session: Session):
    org, user = _tenant(session, "Domain")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    ext = _principal(session, org, path["connector"], "user", "subject", email="person@example.com")
    domain = _principal(session, org, path["connector"], "domain", "example.com", domain="example.com")
    anyone = _principal(session, org, path["connector"], "anyone", "anyone")
    _verified_link(session, org, path["connector"], user, ext)
    snapshot = _snapshot(session, org, path["connector"], path["source"])
    _acl(session, org, path["connector"], path["source"], snapshot, domain)
    session.flush(); assert bool(_search(session, org, user))
    _exec(session, "DELETE FROM source_acl_entries WHERE acl_snapshot_id=:snapshot", snapshot=snapshot)
    _acl(session, org, path["connector"], path["source"], snapshot, anyone)
    session.flush(); assert bool(_search(session, org, user))


@pytest.mark.parametrize("status,current,inheritance", [("building", False, "unknown"), ("failed", False, "unknown"), ("stale", False, "complete"), ("complete", False, "complete")])
def test_incomplete_or_noncurrent_snapshots_deny(session: Session, status: str, current: bool, inheritance: str):
    org, user = _tenant(session, f"Snapshot-{status}-{current}")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    principal = _principal(session, org, path["connector"], "user", "subject")
    _verified_link(session, org, path["connector"], user, principal)
    snapshot = _snapshot(session, org, path["connector"], path["source"], status=status, current=current, inheritance=inheritance)
    _acl(session, org, path["connector"], path["source"], snapshot, principal)
    session.flush(); assert _search(session, org, user) == ()


def test_previous_current_complete_survives_failed_replacement(session: Session):
    org, user = _tenant(session, "FailedReplacement")
    path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
    principal = _principal(session, org, path["connector"], "user", "subject")
    _verified_link(session, org, path["connector"], user, principal)
    current = _snapshot(session, org, path["connector"], path["source"])
    _acl(session, org, path["connector"], path["source"], current, principal)
    _snapshot(session, org, path["connector"], path["source"], status="failed", current=False)
    session.flush(); assert bool(_search(session, org, user))


def test_acl_deny_unknown_expired_and_false_read_fail_closed(session: Session):
    for case in ("deny", "unknown", "expired", "false_read"):
        org, user = _tenant(session, f"Acl-{case}")
        path = _content_path(session, org, mode="source_acl", chunk_vector=_vector(1.0))
        principal = _principal(session, org, path["connector"], "user", "subject")
        _verified_link(session, org, path["connector"], user, principal)
        snapshot = _snapshot(session, org, path["connector"], path["source"])
        if case == "deny":
            _acl(session, org, path["connector"], path["source"], snapshot, principal)
            _acl(session, org, path["connector"], path["source"], snapshot, principal, effect="deny", read=False)
        elif case == "unknown": _acl(session, org, path["connector"], path["source"], snapshot, principal, permission="unknown", read=False)
        elif case == "expired": _acl(session, org, path["connector"], path["source"], snapshot, principal, expires=NOW-timedelta(seconds=1))
        else: _acl(session, org, path["connector"], path["source"], snapshot, principal, permission="unknown", read=False)
        session.flush(); assert _search(session, org, user) == ()


@pytest.mark.parametrize("mode,platform,acl,allowed", [("platform_managed",True,False,True),("source_acl",True,False,False),("source_acl",False,True,True),("hybrid",True,False,False),("hybrid",False,True,False),("hybrid",True,True,True)])
def test_access_mode_formulas(session: Session, mode: str, platform: bool, acl: bool, allowed: bool):
    org, user = _tenant(session, f"Mode-{mode}-{platform}-{acl}")
    path = _content_path(session, org, mode=mode, chunk_vector=_vector(1.0))
    if platform: _grant(session, org, user, path["space"])
    if acl:
        principal = _principal(session, org, path["connector"], "user", "subject")
        _verified_link(session, org, path["connector"], user, principal)
        snapshot = _snapshot(session, org, path["connector"], path["source"])
        _acl(session, org, path["connector"], path["source"], snapshot, principal)
    session.flush(); assert bool(_search(session, org, user)) is allowed


@pytest.mark.parametrize("kwargs", [{"membership_status":"removed"},{"source_status":"deleted"},{"source_status":"unavailable"},{"version_current":False},{"version_lifecycle":"unavailable"},{"indexing_status":None},{"indexing_status":"stale"},{"indexing_status":"failed"},{"embedding_model":"wrong"},{"chunk_vector":None}])
def test_content_lifecycle_and_embedding_eligibility(session: Session, kwargs):
    org, user = _tenant(session, f"Lifecycle-{uuid.uuid4()}")
    defaults={"chunk_vector":_vector(1.0)}; defaults.update(kwargs)
    path=_content_path(session,org,mode="platform_managed",**defaults); _grant(session,org,user,path["space"]); session.flush()
    assert _search(session,org,user)==()


def test_authorization_precedes_ranking_limit_dedup_and_ties(session: Session):
    org,user=_tenant(session,"Ranking")
    authorized=_content_path(session,org,mode="platform_managed",chunk_vector=_vector(0.8,0.2)); _grant(session,org,user,authorized["space"])
    unauthorized=_content_path(session,org,mode="platform_managed",chunk_vector=_vector(1.0,0.0))
    second=_content_path(session,org,mode="platform_managed",chunk_vector=_vector(0.8,0.2)); _grant(session,org,user,second["space"])
    # Duplicate grant path must not duplicate the same chunk.
    _grant(session,org,user,authorized["space"],"organization")
    session.flush(); results=_search(session,org,user,_vector(1.0),limit=2)
    assert {r.chunk_id for r in results}=={authorized["chunk"],second["chunk"]}
    assert unauthorized["chunk"] not in {r.chunk_id for r in results}
    assert [r.chunk_id for r in results]==sorted([r.chunk_id for r in results])
    assert all(not hasattr(r,"embedding") and not hasattr(r,"external_principal_id") for r in results)


def test_generated_plan_contains_authorization_relations(session: Session):
    org,user=_tenant(session,"Plan")
    params={"organization_id":org,"user_id":user,"query_embedding":"["+",".join("0" for _ in range(EMBEDDING_DIMENSION))+"]","embedding_model":MODEL,"embedding_dimension":EMBEDDING_DIMENSION,"result_limit":1,"max_group_depth":MAX_GROUP_DEPTH,"knowledge_space_ids":None,"connector_ids":None,"source_item_types":None}
    plan=session.execute(text("EXPLAIN "+SEARCH_SQL),params).scalars().all(); rendered="\n".join(plan).lower()
    # PostgreSQL may inline CTEs, but secure authorization relations remain in the plan.
    assert "document_chunks" in rendered
    assert any(name in rendered for name in ("source_acl_entries","knowledge_space_user_grants","connector_scopes"))
