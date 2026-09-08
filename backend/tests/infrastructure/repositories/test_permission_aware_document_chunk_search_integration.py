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


def _activate_staged_generation(session: Session, org: UUID, path: dict[str, UUID]):
    job, generation, work, materialization, chunk, activation = (
        uuid.uuid4() for _ in range(6)
    )
    commit, tree, blob, checksum = "a" * 40, "b" * 40, "c" * 40, "d" * 64
    profile = session.execute(
        text("SELECT profile_fingerprint FROM document_indexing_states WHERE document_version_id=:version"),
        {"version": path["version"]},
    ).scalar_one()
    source_key = session.execute(
        text("SELECT source_item_key FROM source_items WHERE id=:source"),
        {"source": path["source"]},
    ).scalar_one()
    session.execute(text("UPDATE connectors SET connector_type='github' WHERE id=:id"), {"id": path["connector"]})
    session.execute(text("UPDATE documents SET source_type='github',source_document_key=:key WHERE id=:id"), {"id": path["document"], "key": source_key})
    session.execute(
        text(
            "UPDATE document_versions SET provider_version_id=:blob,"
            "content_checksum=:checksum,checksum_algorithm='sha256',"
            "metadata=jsonb_build_object('provider','github',"
            "'commit_object_id',CAST(:commit AS varchar),"
            "'blob_object_id',CAST(:blob AS varchar)) WHERE id=:id"
        ),
        {"id": path["version"], "blob": blob, "checksum": checksum, "commit": commit},
    )
    session.execute(text("UPDATE connector_scopes SET external_scope_key='github:repository:123' WHERE id=:id"), {"id": path["scope"]})
    session.execute(text("""UPDATE source_items
        SET source_version=:blob,source_checksum=:checksum,
            metadata=jsonb_build_object(
                'provider','github','repository_identity','github:repository:123',
                'repository_path','file.md','blob_object_id',
                CAST(:blob_metadata AS varchar),
                'snapshot_commit_id',CAST(:commit_metadata AS varchar))
        WHERE id=:id"""), {
            "id": path["source"], "blob": blob, "checksum": checksum,
            "blob_metadata": blob, "commit_metadata": commit,
        })
    session.execute(text("""INSERT INTO connector_sync_jobs
        (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status,
         attempt_count,fencing_token,next_attempt_at,completed_at,created_at,updated_at)
        VALUES (:id,:org,:connector,:scope,'incremental','manual','succeeded',1,1,NULL,
                :now,:now,:now)"""),
        {"id": job, "org": org, "connector": path["connector"], "scope": path["scope"], "now": NOW})
    session.execute(text("""INSERT INTO connector_sync_generations
        (id,organization_id,connector_id,connector_scope_id,sync_job_id,provider_key,
         repository_identity,branch_name,commit_object_id,root_tree_object_id,
         profile_fingerprint,status,discovery_complete,discovery_completed_at,
         reconciliation_eligible,resync_required,items_discovered,items_registered,
         declared_bytes,created_at,updated_at,terminal_at)
        VALUES (:id,:org,:connector,:scope,:job,'github','github:repository:123','main',
                :commit,:tree,:profile,'completed',true,:now,false,false,1,1,10,:now,:now,:now)"""),
        {"id": generation, "org": org, "connector": path["connector"], "scope": path["scope"],
         "job": job, "commit": commit, "tree": tree, "profile": profile, "now": NOW})
    session.execute(text("""INSERT INTO connector_sync_file_work_items
        (id,organization_id,connector_id,connector_scope_id,generation_id,source_item_key,
         source_key_hash,repository_path,provider_blob_id,provider_revision_id,
         profile_fingerprint,status,attempt_count,max_attempts,fencing_token,
         downloaded_bytes,extracted_characters,chunk_count,embedding_batch_count,
         created_at,updated_at,terminal_at)
        VALUES (:id,:org,:connector,:scope,:generation,:key,:hash,'file.md',:blob,:commit,
                :profile,'succeeded',1,3,1,10,10,1,1,:now,:now,:now)"""),
        {"id": work, "org": org, "connector": path["connector"], "scope": path["scope"],
         "generation": generation, "key": source_key, "hash": "e" * 64,
         "blob": blob, "commit": commit, "profile": profile, "now": NOW})
    session.execute(text("""INSERT INTO connector_sync_file_materializations
        (id,organization_id,connector_id,connector_scope_id,generation_id,work_item_id,
         repository_identity,branch_name,root_tree_object_id,source_item_key,source_key_hash,
         repository_path,provider_blob_id,provider_revision_id,profile_fingerprint,
         content_checksum,title,mime_type,embedding_model,chunk_count,created_at)
        VALUES (:id,:org,:connector,:scope,:generation,:work,'github:repository:123','main',
                :tree,:key,:hash,'file.md',:blob,:commit,:profile,:checksum,'Staged',
                'text/markdown',:model,1,:now)"""),
        {"id": materialization, "org": org, "connector": path["connector"], "scope": path["scope"],
         "generation": generation, "work": work, "tree": tree, "key": source_key,
         "hash": "e" * 64, "blob": blob, "commit": commit, "profile": profile,
         "checksum": checksum, "model": MODEL, "now": NOW})
    session.execute(text("""INSERT INTO connector_sync_file_materialization_chunks
        (id,organization_id,generation_id,materialization_id,chunk_index,chunk_text,
         content_hash,embedding,embedding_model,created_at)
        VALUES (:id,:org,:generation,:materialization,0,'activated-ledger-chunk',:hash,
                CAST(:embedding AS vector),:model,:now)"""),
        {"id": chunk, "org": org, "generation": generation, "materialization": materialization,
         "hash": "f" * 64, "embedding": "[" + ",".join(str(v) for v in _vector(1.0)) + "]",
         "model": MODEL, "now": NOW})
    session.execute(text("""INSERT INTO connector_sync_generation_activations
        (id,organization_id,connector_id,connector_scope_id,generation_id,
         repository_identity,commit_object_id,profile_fingerprint,status,
         activated_at,created_at,updated_at)
        VALUES (:id,:org,:connector,:scope,:generation,'github:repository:123',:commit,
                :profile,'active',:now,:now,:now)"""),
        {"id": activation, "org": org, "connector": path["connector"], "scope": path["scope"],
         "generation": generation, "commit": commit, "profile": profile, "now": NOW})
    session.flush()
    return chunk, generation, activation


def _activate_shared_source_in_second_scope(
    session: Session, org: UUID, user: UUID, path: dict[str, UUID]
):
    space, scope, job, generation, work, materialization, chunk, activation = (
        uuid.uuid4() for _ in range(8)
    )
    version, state = uuid.uuid4(), uuid.uuid4()
    repository = "github:repository:456"
    commit, tree, blob, checksum = "7" * 40, "8" * 40, "9" * 40, "a" * 64
    source_key = session.execute(
        text("SELECT source_item_key FROM source_items WHERE id=:source"),
        {"source": path["source"]},
    ).scalar_one()
    profile = session.execute(
        text("SELECT profile_fingerprint FROM document_indexing_states WHERE document_version_id=:version"),
        {"version": path["version"]},
    ).scalar_one()
    _exec(session, "INSERT INTO knowledge_spaces (id,organization_id,name,slug,status) VALUES (:id,:org,'Second active ledger scope',:slug,'active')", id=space, org=org, slug=f"space-{space}")
    _exec(session, "INSERT INTO connector_scopes (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode,status) VALUES (:id,:org,:connector,:space,'Second scope',:slug,'repository',:repository,'platform_managed','active')", id=scope, org=org, connector=path["connector"], space=space, slug=f"scope-{scope}", repository=repository)
    _exec(session, "INSERT INTO source_item_scope_memberships (id,organization_id,connector_id,source_item_id,connector_scope_id,status,first_discovered_at,last_seen_at) VALUES (:id,:org,:connector,:source,:scope,'active',:now,:now)", id=uuid.uuid4(), org=org, connector=path["connector"], source=path["source"], scope=scope, now=NOW)
    _grant(session, org, user, space)
    _exec(session, "INSERT INTO document_versions (id,organization_id,connector_id,source_item_id,version_number,provider_version_id,content_checksum,checksum_algorithm,version_cause,lifecycle,is_current,discovered_at,metadata) VALUES (:id,:org,:connector,:source,2,:blob,:checksum,'sha256','content_changed','available',false,:now,jsonb_build_object('provider','github','commit_object_id',CAST(:commit AS varchar),'blob_object_id',CAST(:blob AS varchar)))", id=version, org=org, connector=path["connector"], source=path["source"], blob=blob, checksum=checksum, commit=commit, now=NOW)
    _exec(session, "INSERT INTO document_indexing_states (id,organization_id,document_version_id,extraction_profile,extraction_version,chunking_profile,chunking_version,embedding_provider,embedding_model,embedding_dimensions,profile_fingerprint,desired_generation,indexed_generation,status,reason,attempt_count,requested_at,started_at,completed_at) VALUES (:id,:org,:version,'default','v1','deterministic','v1','openai',:model,1536,:profile,1,1,'indexed','content_changed',0,:now,:now,:now)", id=state, org=org, version=version, model=MODEL, profile=profile, now=NOW)
    _exec(session, "INSERT INTO connector_sync_jobs (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status,attempt_count,fencing_token,next_attempt_at,completed_at,created_at,updated_at) VALUES (:id,:org,:connector,:scope,'incremental','manual','succeeded',1,1,NULL,:now,:now,:now)", id=job, org=org, connector=path["connector"], scope=scope, now=NOW)
    _exec(session, "INSERT INTO connector_sync_generations (id,organization_id,connector_id,connector_scope_id,sync_job_id,provider_key,repository_identity,branch_name,commit_object_id,root_tree_object_id,profile_fingerprint,status,discovery_complete,discovery_completed_at,reconciliation_eligible,resync_required,items_discovered,items_registered,declared_bytes,created_at,updated_at,terminal_at) VALUES (:id,:org,:connector,:scope,:job,'github',:repository,'main',:commit,:tree,:profile,'completed',true,:now,false,false,1,1,10,:now,:now,:now)", id=generation, org=org, connector=path["connector"], scope=scope, job=job, repository=repository, commit=commit, tree=tree, profile=profile, now=NOW)
    _exec(session, "INSERT INTO connector_sync_file_work_items (id,organization_id,connector_id,connector_scope_id,generation_id,source_item_key,source_key_hash,repository_path,provider_blob_id,provider_revision_id,profile_fingerprint,status,attempt_count,max_attempts,fencing_token,downloaded_bytes,extracted_characters,chunk_count,embedding_batch_count,created_at,updated_at,terminal_at) VALUES (:id,:org,:connector,:scope,:generation,:key,:hash,'file.md',:blob,:commit,:profile,'succeeded',1,3,1,10,10,1,1,:now,:now,:now)", id=work, org=org, connector=path["connector"], scope=scope, generation=generation, key=source_key, hash="b" * 64, blob=blob, commit=commit, profile=profile, now=NOW)
    _exec(session, "INSERT INTO connector_sync_file_materializations (id,organization_id,connector_id,connector_scope_id,generation_id,work_item_id,repository_identity,branch_name,root_tree_object_id,source_item_key,source_key_hash,repository_path,provider_blob_id,provider_revision_id,profile_fingerprint,content_checksum,title,mime_type,embedding_model,chunk_count,created_at) VALUES (:id,:org,:connector,:scope,:generation,:work,:repository,'main',:tree,:key,:hash,'file.md',:blob,:commit,:profile,:checksum,'Second staged','text/markdown',:model,1,:now)", id=materialization, org=org, connector=path["connector"], scope=scope, generation=generation, work=work, repository=repository, tree=tree, key=source_key, hash="b" * 64, blob=blob, commit=commit, profile=profile, checksum=checksum, model=MODEL, now=NOW)
    _exec(session, "INSERT INTO connector_sync_file_materialization_chunks (id,organization_id,generation_id,materialization_id,chunk_index,chunk_text,content_hash,embedding,embedding_model,created_at) VALUES (:id,:org,:generation,:materialization,0,'second-activated-ledger-chunk',:hash,CAST(:embedding AS vector),:model,:now)", id=chunk, org=org, generation=generation, materialization=materialization, hash="c" * 64, embedding="[" + ",".join(str(v) for v in _vector(0.8, 0.2)) + "]", model=MODEL, now=NOW)
    _exec(session, "INSERT INTO connector_sync_generation_activations (id,organization_id,connector_id,connector_scope_id,generation_id,repository_identity,commit_object_id,profile_fingerprint,status,activated_at,created_at,updated_at) VALUES (:id,:org,:connector,:scope,:generation,:repository,:commit,:profile,'active',:now,:now,:now)", id=activation, org=org, connector=path["connector"], scope=scope, generation=generation, repository=repository, commit=commit, profile=profile, now=NOW)
    session.flush()
    return {"space": space, "scope": scope, "version": version, "chunk": chunk, "activation": activation}


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


def test_atomic_activation_switches_one_scope_without_mixing_legacy_chunks(session: Session):
    org, user = _tenant(session, "LedgerActivation")
    activated = _content_path(session, org, mode="platform_managed", chunk_vector=_vector(1.0))
    unaffected = _content_path(session, org, mode="platform_managed", chunk_vector=_vector(0.8, 0.2))
    _grant(session, org, user, activated["space"])
    _grant(session, org, user, unaffected["space"])
    session.flush()
    before = _search(session, org, user)
    assert activated["chunk"] in {row.chunk_id for row in before}
    staged_chunk, _generation, _activation = _activate_staged_generation(
        session, org, activated
    )
    after = _search(session, org, user)
    ids = {row.chunk_id for row in after}
    assert staged_chunk in ids
    assert activated["chunk"] not in ids
    assert unaffected["chunk"] in ids
    assert len([row for row in after if row.connector_scope_id == activated["scope"]]) == 1
    denied_user = _user_without_grant(session, org)
    assert _search(session, org, denied_user) == ()


def test_activated_retrieval_honors_real_grant_revocation_and_lifecycle(session: Session):
    org, user = _tenant(session, "LedgerRevocation")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, _generation, activation = _activate_staged_generation(
        session, org, path
    )
    session.flush()
    assert [row.chunk_id for row in _search(session, org, user)] == [staged_chunk]

    session.execute(
        text(
            "UPDATE knowledge_space_user_grants SET revoked_at=:now "
            "WHERE organization_id=:org AND knowledge_space_id=:space AND user_id=:user"
        ),
        {"now": NOW, "org": org, "space": path["space"], "user": user},
    )
    session.flush()
    assert _search(session, org, user) == ()

    session.execute(
        text(
            "UPDATE knowledge_space_user_grants SET revoked_at=NULL "
            "WHERE organization_id=:org AND knowledge_space_id=:space AND user_id=:user"
        ),
        {"org": org, "space": path["space"], "user": user},
    )
    lifecycle_changes = (
        ("UPDATE source_item_scope_memberships SET status='removed',removed_at=:now WHERE connector_scope_id=:id", path["scope"]),
        ("UPDATE source_items SET status='unavailable' WHERE id=:id", path["source"]),
        ("UPDATE connectors SET status='paused' WHERE id=:id", path["connector"]),
        ("UPDATE knowledge_spaces SET status='inactive' WHERE id=:id", path["space"]),
        ("UPDATE documents SET status='failed' WHERE id=:id", path["document"]),
    )
    for statement, identity in lifecycle_changes:
        with session.begin_nested() as savepoint:
            session.execute(text(statement), {"id": identity, "now": NOW})
            session.flush()
            assert _search(session, org, user) == ()
            savepoint.rollback()
        assert [row.chunk_id for row in _search(session, org, user)] == [staged_chunk]

    session.execute(
        text(
            "UPDATE connector_sync_generation_activations "
            "SET status='retired',retired_at=:now WHERE id=:id"
        ),
        {"id": activation, "now": NOW},
    )
    session.flush()
    retired_results = _search(session, org, user)
    assert [row.chunk_id for row in retired_results] == [path["chunk"]]
    assert staged_chunk not in {row.chunk_id for row in retired_results}


@pytest.mark.parametrize("historical_fallback", [False, True])
def test_activated_retrieval_rejects_exact_and_historical_duplicate_versions(
    session: Session, historical_fallback: bool
):
    org, user = _tenant(session, "LedgerHistoricalVersion")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, generation, _activation = _activate_staged_generation(
        session, org, path
    )
    if historical_fallback:
        session.execute(
            text(
                "UPDATE document_versions SET is_current=false,"
                "metadata=jsonb_set(metadata,'{commit_object_id}',"
                "to_jsonb(CAST(:commit AS text))) WHERE id=:id"
            ),
            {"id": path["version"], "commit": "0" * 40},
        )
    session.flush()
    result = _search(session, org, user)
    assert [(row.chunk_id, row.document_version_id) for row in result] == [
        (staged_chunk, path["version"])
    ]

    identity = session.execute(
        text(
            "SELECT provider_version_id,content_checksum,metadata,connector_id,source_item_id "
            "FROM document_versions WHERE id=:id"
        ),
        {"id": path["version"]},
    ).mappings().one()
    duplicate = uuid.uuid4()
    session.execute(
        text(
            "INSERT INTO document_versions "
            "(id,organization_id,connector_id,source_item_id,version_number,"
            "provider_version_id,content_checksum,checksum_algorithm,version_cause,"
            "lifecycle,is_current,discovered_at,metadata) VALUES "
            "(:id,:org,:connector,:source,2,:blob,:checksum,'sha256',"
            "'content_changed','available',false,:now,CAST(:metadata AS jsonb))"
        ),
        {
            "id": duplicate,
            "org": org,
            "connector": identity["connector_id"],
            "source": identity["source_item_id"],
            "blob": identity["provider_version_id"],
            "checksum": identity["content_checksum"],
            "now": NOW,
            "metadata": __import__("json").dumps(identity["metadata"]),
        },
    )
    profile = session.execute(
        text("SELECT profile_fingerprint FROM connector_sync_generations WHERE id=:id"),
        {"id": generation},
    ).scalar_one()
    session.execute(
        text(
            "INSERT INTO document_indexing_states "
            "(id,organization_id,document_version_id,extraction_profile,extraction_version,"
            "chunking_profile,chunking_version,embedding_provider,embedding_model,"
            "embedding_dimensions,profile_fingerprint,desired_generation,indexed_generation,"
            "status,reason,attempt_count,requested_at,started_at,completed_at) VALUES "
            "(:id,:org,:version,'default','v1','deterministic','v1','openai',:model,"
            "1536,:profile,1,1,'indexed','content_changed',0,:now,:now,:now)"
        ),
        {
            "id": uuid.uuid4(), "org": org, "version": duplicate,
            "model": MODEL, "profile": profile, "now": NOW,
        },
    )
    session.flush()
    assert _search(session, org, user) == ()


def test_manifest_v1_prefers_one_exact_commit_version_over_historical_fallback(
    session: Session,
):
    org, user = _tenant(session, "LedgerHistoricalPreference")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, generation, _activation = _activate_staged_generation(
        session, org, path
    )
    generation_commit = session.execute(
        text("SELECT commit_object_id FROM connector_sync_generations WHERE id=:id"),
        {"id": generation},
    ).scalar_one()
    session.execute(
        text(
            "UPDATE document_versions SET is_current=false,"
            "metadata=jsonb_set(metadata,'{commit_object_id}',to_jsonb(CAST(:commit AS text))) "
            "WHERE id=:id"
        ),
        {"id": path["version"], "commit": "0" * 40},
    )
    identity = session.execute(
        text(
            "SELECT provider_version_id,content_checksum,metadata,connector_id,source_item_id "
            "FROM document_versions WHERE id=:id"
        ),
        {"id": path["version"]},
    ).mappings().one()
    exact = uuid.uuid4()
    exact_metadata = dict(identity["metadata"])
    exact_metadata["commit_object_id"] = generation_commit
    session.execute(
        text(
            "INSERT INTO document_versions "
            "(id,organization_id,connector_id,source_item_id,version_number,"
            "provider_version_id,content_checksum,checksum_algorithm,version_cause,"
            "lifecycle,is_current,discovered_at,metadata) VALUES "
            "(:id,:org,:connector,:source,2,:blob,:checksum,'sha256',"
            "'content_changed','available',false,:now,CAST(:metadata AS jsonb))"
        ),
        {
            "id": exact,
            "org": org,
            "connector": identity["connector_id"],
            "source": identity["source_item_id"],
            "blob": identity["provider_version_id"],
            "checksum": identity["content_checksum"],
            "now": NOW,
            "metadata": __import__("json").dumps(exact_metadata),
        },
    )
    profile = session.execute(
        text(
            "SELECT profile_fingerprint FROM connector_sync_generations WHERE id=:id"
        ),
        {"id": generation},
    ).scalar_one()
    session.execute(
        text(
            "INSERT INTO document_indexing_states "
            "(id,organization_id,document_version_id,extraction_profile,extraction_version,"
            "chunking_profile,chunking_version,embedding_provider,embedding_model,"
            "embedding_dimensions,profile_fingerprint,desired_generation,indexed_generation,"
            "status,reason,attempt_count,requested_at,started_at,completed_at) VALUES "
            "(:id,:org,:version,'default','v1','deterministic','v1','openai',:model,"
            "1536,:profile,1,1,'indexed','content_changed',0,:now,:now,:now)"
        ),
        {
            "id": uuid.uuid4(),
            "org": org,
            "version": exact,
            "model": MODEL,
            "profile": profile,
            "now": NOW,
        },
    )
    session.flush()

    assert [
        (row.chunk_id, row.document_version_id) for row in _search(session, org, user)
    ] == [(staged_chunk, exact)]


def test_manifest_v2_rejects_historical_commit_fallback(session: Session):
    org, user = _tenant(session, "LedgerV2HistoricalFallback")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    _staged_chunk, generation, _activation = _activate_staged_generation(
        session, org, path
    )
    session.execute(
        text(
            "UPDATE connector_sync_generations SET manifest_schema_version=2 WHERE id=:id"
        ),
        {"id": generation},
    )
    session.execute(
        text(
            "UPDATE document_versions SET "
            "metadata=jsonb_set(metadata,'{commit_object_id}',to_jsonb(CAST(:commit AS text))) "
            "WHERE id=:id"
        ),
        {"id": path["version"], "commit": "0" * 40},
    )
    session.flush()

    assert _search(session, org, user) == ()


@pytest.mark.parametrize(
    "drift",
    (
        "provider",
        "metadata_blob",
        "provider_blob",
        "checksum",
        "unavailable_version",
        "missing_index",
        "index_profile",
        "index_model",
        "index_dimensions",
        "document_identity",
    ),
)
def test_manifest_v1_historical_fallback_rejects_other_attribution_drift(
    session: Session, drift: str
):
    org, user = _tenant(session, f"LedgerHistoricalDrift-{drift}")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, generation, _activation = _activate_staged_generation(
        session, org, path
    )
    session.execute(
        text(
            "UPDATE connector_sync_generations SET manifest_schema_version=1 "
            "WHERE id=:id"
        ),
        {"id": generation},
    )
    session.execute(
        text(
            "UPDATE document_versions SET is_current=false,"
            "metadata=jsonb_set(metadata,'{commit_object_id}',"
            "to_jsonb(CAST(:commit AS text))) WHERE id=:id"
        ),
        {"id": path["version"], "commit": "0" * 40},
    )
    session.flush()
    assert [
        (row.chunk_id, row.document_version_id)
        for row in _search(session, org, user)
    ] == [(staged_chunk, path["version"])]

    if drift == "provider":
        statement = (
            "UPDATE document_versions SET metadata=jsonb_set(metadata,'{provider}',"
            "to_jsonb(CAST('gitlab' AS text))) WHERE id=:id"
        )
        params = {"id": path["version"]}
    elif drift == "metadata_blob":
        statement = (
            "UPDATE document_versions SET metadata=jsonb_set(metadata,'{blob_object_id}',"
            "to_jsonb(CAST(:value AS text))) WHERE id=:id"
        )
        params = {"id": path["version"], "value": "e" * 40}
    elif drift == "provider_blob":
        statement = "UPDATE document_versions SET provider_version_id=:value WHERE id=:id"
        params = {"id": path["version"], "value": "e" * 40}
    elif drift == "checksum":
        statement = "UPDATE document_versions SET content_checksum=:value WHERE id=:id"
        params = {"id": path["version"], "value": "0" * 64}
    elif drift == "unavailable_version":
        statement = "UPDATE document_versions SET lifecycle='unavailable' WHERE id=:id"
        params = {"id": path["version"]}
    elif drift == "missing_index":
        statement = "DELETE FROM document_indexing_states WHERE document_version_id=:id"
        params = {"id": path["version"]}
    elif drift == "index_profile":
        statement = (
            "UPDATE document_indexing_states SET profile_fingerprint='other:profile' "
            "WHERE document_version_id=:id"
        )
        params = {"id": path["version"]}
    elif drift == "index_model":
        statement = (
            "UPDATE document_indexing_states SET embedding_model='other:model' "
            "WHERE document_version_id=:id"
        )
        params = {"id": path["version"]}
    elif drift == "index_dimensions":
        statement = (
            "UPDATE document_indexing_states SET embedding_dimensions=1024 "
            "WHERE document_version_id=:id"
        )
        params = {"id": path["version"]}
    else:
        statement = "UPDATE documents SET source_document_key=:value WHERE id=:id"
        params = {"id": path["document"], "value": f"wrong-{path['document']}"}

    session.execute(text(statement), params)
    session.flush()
    assert _search(session, org, user) == ()


def test_activated_historical_citations_are_tenant_isolated(session: Session):
    first_org, first_user = _tenant(session, "LedgerHistoricalTenantOne")
    first = _content_path(
        session, first_org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, first_org, first_user, first["space"])
    first_chunk, _first_generation, _first_activation = _activate_staged_generation(
        session, first_org, first
    )
    second_org, second_user = _tenant(session, "LedgerHistoricalTenantTwo")
    second = _content_path(
        session, second_org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, second_org, second_user, second["space"])
    second_chunk, _second_generation, _second_activation = _activate_staged_generation(
        session, second_org, second
    )
    session.flush()

    assert [row.chunk_id for row in _search(session, first_org, first_user)] == [
        first_chunk
    ]
    assert [row.chunk_id for row in _search(session, second_org, second_user)] == [
        second_chunk
    ]

    session.execute(
        text(
            "UPDATE document_versions SET metadata=jsonb_set(metadata,'{provider}',"
            "to_jsonb(CAST('gitlab' AS text))) WHERE id=:id"
        ),
        {"id": first["version"]},
    )
    session.flush()
    assert _search(session, first_org, first_user) == ()
    assert [row.chunk_id for row in _search(session, second_org, second_user)] == [
        second_chunk
    ]


def test_unsupported_manifest_version_cannot_enter_activated_retrieval(
    session: Session,
):
    org, user = _tenant(session, "LedgerUnsupportedManifest")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, generation, _activation = _activate_staged_generation(
        session, org, path
    )
    session.flush()
    assert [row.chunk_id for row in _search(session, org, user)] == [staged_chunk]

    # The database constraint is the primary invariant.  Dropping it only
    # inside this rolled-back test transaction proves retrieval independently
    # fails closed if an unsupported value is ever present.
    session.execute(
        text(
            "ALTER TABLE connector_sync_generations DROP CONSTRAINT "
            "ck_connector_sync_generations_ck_connector_sync_generat_736f"
        )
    )
    session.execute(
        text(
            "UPDATE connector_sync_generations SET manifest_schema_version=3 "
            "WHERE id=:id"
        ),
        {"id": generation},
    )
    session.flush()
    assert _search(session, org, user) == ()


def test_activated_retrieval_ignores_fabricated_mutable_version_document_link(
    session: Session,
):
    org, user = _tenant(session, "LedgerCitationLink")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    unrelated = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(0.8, 0.2)
    )
    _grant(session, org, user, path["space"])
    staged_chunk, _generation, _activation = _activate_staged_generation(
        session, org, path
    )
    session.flush()
    assert [row.chunk_id for row in _search(session, org, user)] == [staged_chunk]

    session.execute(
        text(
            "DELETE FROM document_version_documents "
            "WHERE organization_id=:org "
            "AND (document_version_id IN (:version,:other_version) "
            "OR document_id=:other_document)"
        ),
        {
            "org": org,
            "version": path["version"],
            "other_version": unrelated["version"],
            "other_document": unrelated["document"],
        },
    )
    _exec(
        session,
        "INSERT INTO document_version_documents "
        "(id,organization_id,document_version_id,document_id) "
        "VALUES (:id,:org,:version,:document)",
        id=uuid.uuid4(),
        org=org,
        version=path["version"],
        document=unrelated["document"],
    )
    session.flush()
    result = _search(session, org, user)
    assert [(row.chunk_id, row.document_id, row.document_version_id) for row in result] == [
        (staged_chunk, path["document"], path["version"])
    ]
    assert result[0].document_id != unrelated["document"]


def test_two_activated_scopes_sharing_source_keep_distinct_immutable_citations(
    session: Session,
):
    org, user = _tenant(session, "TwoActivatedScopes")
    first = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, first["space"])
    first_chunk, _generation, _activation = _activate_staged_generation(
        session, org, first
    )
    second = _activate_shared_source_in_second_scope(session, org, user, first)

    results = _search(session, org, user, limit=10)
    identities = {
        (row.connector_scope_id, row.document_version_id, row.chunk_id)
        for row in results
    }
    assert identities == {
        (first["scope"], first["version"], first_chunk),
        (second["scope"], second["version"], second["chunk"]),
    }
    assert first["chunk"] not in {row.chunk_id for row in results}

    session.execute(
        text(
            "UPDATE knowledge_space_user_grants SET revoked_at=:now "
            "WHERE organization_id=:org AND knowledge_space_id=:space AND user_id=:user"
        ),
        {"now": NOW, "org": org, "space": first["space"], "user": user},
    )
    session.flush()
    after_revoke = _search(session, org, user, limit=10)
    assert [row.connector_scope_id for row in after_revoke] == [second["scope"]]
    assert after_revoke[0].chunk_id == second["chunk"]


def test_activation_attribution_drift_fails_closed_without_legacy_fallback(
    session: Session,
):
    org, user = _tenant(session, "LedgerActivationDrift")
    path = _content_path(
        session, org, mode="platform_managed", chunk_vector=_vector(1.0)
    )
    _grant(session, org, user, path["space"])
    _staged_chunk, _generation, activation = _activate_staged_generation(
        session, org, path
    )
    assert len(_search(session, org, user)) == 1
    session.execute(
        text(
            "UPDATE connector_sync_generation_activations "
            "SET commit_object_id=:commit WHERE id=:id"
        ),
        {"commit": "9" * 40, "id": activation},
    )
    session.flush()
    assert _search(session, org, user) == ()


def test_generated_plan_contains_authorization_relations(session: Session):
    org,user=_tenant(session,"Plan")
    params={"organization_id":org,"user_id":user,"query_embedding":"["+",".join("0" for _ in range(EMBEDDING_DIMENSION))+"]","embedding_model":MODEL,"embedding_dimension":EMBEDDING_DIMENSION,"result_limit":1,"max_group_depth":MAX_GROUP_DEPTH,"knowledge_space_ids":None,"connector_ids":None,"source_item_types":None}
    plan=session.execute(text("EXPLAIN "+SEARCH_SQL),params).scalars().all(); rendered="\n".join(plan).lower()
    # PostgreSQL may inline CTEs, but secure authorization relations remain in the plan.
    assert "document_chunks" in rendered
    assert any(name in rendered for name in ("source_acl_entries","knowledge_space_user_grants","connector_scopes"))
