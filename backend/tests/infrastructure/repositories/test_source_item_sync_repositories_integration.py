from __future__ import annotations
import os, subprocess, uuid
from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker
from infrastructure.db.models import SourceItem, SourceItemScopeMembership, ConnectorSyncRun, ConnectorSyncItem
from infrastructure.repositories.connector_repository import ConnectorRepositoryConflict, InvalidConnectorRepositoryRequest
from infrastructure.repositories.source_item_repository import SourceItemRepository
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository, SafeSyncError

ROOT=Path(__file__).resolve().parents[3];PYTHON=ROOT/".venv"/"Scripts"/"python.exe";INI=ROOT/"alembic.ini";TEST_URL="TEST_DATABASE_URL";DEV_URL="DATABASE_URL";NOW=datetime(2026,8,24,tzinfo=timezone.utc)
def _identity(url):v=make_url(url);return v.drivername,v.host,v.port,v.database
@pytest.fixture(scope="module")
def engine():
    url=os.environ[TEST_URL];dev=os.environ.get(DEV_URL)
    if dev and _identity(dev)==_identity(url):raise RuntimeError("test DB must differ")
    reset=create_engine(url,future=True)
    with reset.begin() as c:c.execute(text("DROP SCHEMA IF EXISTS public CASCADE"));c.execute(text("CREATE SCHEMA public"))
    reset.dispose();env=os.environ.copy();env[DEV_URL]=url;subprocess.run([str(PYTHON),"-m","alembic","-c",str(INI),"upgrade","head"],check=True,cwd=str(ROOT),env=env);value=create_engine(url,future=True)
    try:yield value
    finally:value.dispose()
@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as c:
        for table in ("source_acl_entries","source_acl_snapshots","external_group_memberships","external_directory_states","user_external_identity_links","external_principals","document_indexing_attempts","document_indexing_states","document_version_documents","document_versions","connector_sync_cursors","connector_sync_errors","connector_sync_items","connector_sync_runs","source_item_scope_memberships","source_items","connector_scopes","connectors","audit_events","knowledge_space_user_grants","knowledge_space_team_grants","knowledge_space_department_grants","knowledge_space_organization_grants","knowledge_spaces","team_memberships","department_memberships","teams","departments","document_chunks","documents","authentication_sessions","user_roles","users","organization_settings","organizations","industries"):c.execute(text(f"DELETE FROM {table}"))
@pytest.fixture
def session(engine):
    value=sessionmaker(bind=engine,class_=Session,autoflush=False,expire_on_commit=False)()
    try:yield value
    finally:value.rollback();value.close()
def _setup(session,name="Alpha"):
    org,connector,space,scope=(uuid.uuid4() for _ in range(4));session.execute(text("INSERT INTO organizations(id,name,slug)VALUES(:id,:name,:slug)"),{"id":org,"name":name,"slug":f"{name.lower()}-{org}"});session.execute(text("INSERT INTO connectors(id,organization_id,connector_type,display_name,slug)VALUES(:id,:org,'local_folder',:name,:slug)"),{"id":connector,"org":org,"name":name,"slug":f"connector-{str(connector)[:8]}"});session.execute(text("INSERT INTO knowledge_spaces(id,organization_id,name,slug)VALUES(:id,:org,:name,:slug)"),{"id":space,"org":org,"name":name,"slug":f"space-{str(space)[:8]}"});session.execute(text("INSERT INTO connector_scopes(id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode)VALUES(:id,:org,:connector,:space,:name,:slug,'folder',:key,'platform_managed')"),{"id":scope,"org":org,"connector":connector,"space":space,"name":name,"slug":f"scope-{str(scope)[:8]}","key":f"/{scope}"});return org,connector,scope
def _item(org,connector,key="Case/Key",status="active"):
    return SourceItem(id=uuid.uuid4(),organization_id=org,connector_id=connector,source_item_key=key,source_item_type="file",title=key,first_seen_at=NOW,last_seen_at=NOW,status=status,source_metadata={},metadata_schema_version=1)
def _membership(org,connector,scope,item):return SourceItemScopeMembership(id=uuid.uuid4(),organization_id=org,connector_id=connector,source_item_id=item,connector_scope_id=scope,status="active",first_discovered_at=NOW,last_seen_at=NOW)
def _run(org,connector,scope,status="queued"):return ConnectorSyncRun(id=uuid.uuid4(),organization_id=org,connector_id=connector,connector_scope_id=scope,mode="incremental",trigger_type="manual",status=status,started_at=NOW if status=="running" else None,run_metadata={})
def _sync_item(org,connector,scope,run,key="Case/Key",source=None):return ConnectorSyncItem(id=uuid.uuid4(),organization_id=org,connector_id=connector,connector_scope_id=scope,sync_run_id=run,source_item_id=source,source_item_key=key,change_type="new",processing_status="pending",attempt_count=0)

def test_source_identity_commit_rollback_case_and_tenant(session):
    org,connector,_=_setup(session);other_org,other_connector,_=_setup(session,"Beta");repo=SourceItemRepository(session);first=_item(org,connector);repo.add(org,connector,first);session.commit();assert repo.get_by_id(org,connector,first.id).id==first.id and repo.get_by_key(org,connector,"Case/Key").id==first.id;assert repo.get_by_id(other_org,other_connector,first.id) is None
    with pytest.raises(ConnectorRepositoryConflict):repo.add(org,connector,_item(org,connector))
    session.rollback()
    lower=_item(org,connector,"case/key");repo.add(org,connector,lower);session.commit();assert repo.get_by_key(org,connector,"case/key").id==lower.id
    transient=_item(org,connector,"rollback");repo.add(org,connector,transient);session.rollback();assert repo.get_by_id(org,connector,transient.id) is None

def test_source_keyset_filters_locks_and_controlled_updates(session):
    org,connector,_=_setup(session);repo=SourceItemRepository(session);items=[]
    for i in range(5):
        row=_item(org,connector,f"key-{i}","active" if i%2==0 else "unavailable");row.created_at=NOW;repo.add(org,connector,row);items.append(row)
    session.commit();first=repo.list_page(org,connector,limit=2);second=repo.list_page(org,connector,limit=2,cursor=first.next_cursor);final=repo.list_page(org,connector,limit=2,cursor=second.next_cursor);assert len({x.id for x in first.items+second.items+final.items})==5 and final.next_cursor is None;assert all(x.status=="active" for x in repo.list_page(org,connector,status="active").items);assert repo.lock_by_key(org,connector,"key-0").id==items[0].id
    repo.update_provider_state(org,connector,items[0].id,source_metadata={"size":1},metadata_schema_version=2,last_seen_at=NOW,source_checksum="abc",source_version="v2",size_bytes=1);repo.set_lifecycle(org,connector,items[0].id,"unavailable");session.commit();changed=repo.get_by_id(org,connector,items[0].id);assert changed.source_metadata=={"size":1} and changed.status=="unavailable"

def test_membership_add_remove_reactivate_and_scope_page(session):
    org,connector,scope=_setup(session);repo=SourceItemRepository(session);item=_item(org,connector);repo.add(org,connector,item);membership=_membership(org,connector,scope,item.id);repo.add_membership(org,connector,membership);session.commit();assert repo.get_membership(org,connector,scope,item.id).id==membership.id;assert repo.list_for_scope(org,connector,scope).items[0].id==item.id
    repo.remove_membership(org,connector,scope,item.id,NOW);session.commit();assert repo.list_for_scope(org,connector,scope).items==();repo.reactivate_membership(org,connector,scope,item.id,NOW);session.commit();assert repo.get_membership(org,connector,scope,item.id).id==membership.id
    with pytest.raises(ConnectorRepositoryConflict):repo.add_membership(org,connector,_membership(org,connector,scope,item.id));session.rollback()

def test_membership_cross_tenant_and_connector_conflicts(session):
    org,connector,scope=_setup(session);other_org,other_connector,other_scope=_setup(session,"Gamma");repo=SourceItemRepository(session);item=_item(org,connector);repo.add(org,connector,item);session.commit()
    for membership in (_membership(org,other_connector,other_scope,item.id),_membership(other_org,other_connector,scope,item.id)):
        with pytest.raises(ConnectorRepositoryConflict):repo.add_membership(membership.organization_id,membership.connector_id,membership)
        session.rollback()

def test_runs_active_conflict_get_lock_pages_state_and_counters(session):
    org,connector,scope=_setup(session);repo=ConnectorSyncRepository(session);queued=_run(org,connector,scope);repo.add_run(org,connector,scope,queued);running=_run(org,connector,scope,"running");repo.add_run(org,connector,scope,running);session.commit();queued_id,running_id=queued.id,running.id;assert repo.get_run(org,connector,scope,queued_id).id==queued_id;assert repo.lock_run(org,connector,scope,queued_id).id==queued_id
    with pytest.raises(ConnectorRepositoryConflict):repo.add_run(org,connector,scope,_run(org,connector,scope,"running"))
    session.rollback()
    repo.set_run_state(org,connector,scope,running_id,status="completed",started_at=NOW,finished_at=NOW);session.commit()
    repo.increment_counters(org,connector,scope,queued_id,items_discovered=2,items_new=1);repo.increment_counters(org,connector,scope,queued_id,items_discovered=3);session.commit();assert repo.get_run(org,connector,scope,queued_id).items_discovered==5
    repo.set_run_state(org,connector,scope,queued_id,status="running",started_at=NOW);session.rollback();assert repo.get_run(org,connector,scope,queued_id).status=="queued";assert len(repo.list_runs(org,connector_id=connector,scope_id=scope,limit=1).items)==1

def test_concurrent_counter_increments_do_not_lose_updates(engine,session):
    org,connector,scope=_setup(session,"Counters");repo=ConnectorSyncRepository(session);run=_run(org,connector,scope);repo.add_run(org,connector,scope,run);session.commit();factory=sessionmaker(bind=engine,class_=Session,expire_on_commit=False);first,second=factory(),factory()
    try:
        ConnectorSyncRepository(first).increment_counters(org,connector,scope,run.id,items_succeeded=2);first.commit();ConnectorSyncRepository(second).increment_counters(org,connector,scope,run.id,items_succeeded=3);second.commit()
    finally:first.close();second.close()
    session.expire_all();assert repo.get_run(org,connector,scope,run.id).items_succeeded==5

def test_sync_items_identity_state_and_pagination(session):
    org,connector,scope=_setup(session);source_repo=SourceItemRepository(session);sync=ConnectorSyncRepository(session);source=_item(org,connector);source_repo.add(org,connector,source);run=_run(org,connector,scope);sync.add_run(org,connector,scope,run);item=_sync_item(org,connector,scope,run.id,source=source.id);sync.add_item(org,connector,scope,run.id,item);session.commit();assert sync.get_item(org,connector,run.id,item.id).id==item.id and sync.get_item_by_key(org,connector,run.id,"Case/Key").id==item.id
    run_id,item_id,source_id=run.id,item.id,source.id
    with pytest.raises(ConnectorRepositoryConflict):sync.add_item(org,connector,scope,run_id,_sync_item(org,connector,scope,run_id,source=source_id))
    session.rollback()
    other=_run(org,connector,scope);sync.add_run(org,connector,scope,other);sync.add_item(org,connector,scope,other.id,_sync_item(org,connector,scope,other.id,source=source_id));session.commit();sync.set_item_state(org,connector,run_id,item_id,status="processing",attempt_count=1,started_at=NOW,source_item_id=source_id);session.commit();assert sync.lock_item(org,connector,run_id,item_id).processing_status=="processing";assert sync.list_items(org,connector,run_id,status="processing").items[0].id==item_id

def test_append_errors_safe_filters_and_no_update_delete(session):
    org,connector,scope=_setup(session);repo=ConnectorSyncRepository(session);run=_run(org,connector,scope);repo.add_run(org,connector,scope,run);item=_sync_item(org,connector,scope,run.id);repo.add_item(org,connector,scope,run.id,item);error=SafeSyncError("source_read","read_failed","Safe summary",True,1,{"operation":"read"},NOW);row=repo.add_error(org,connector,scope,run.id,error,item_id=item.id);repo.add_error(org,connector,scope,run.id,error);session.commit();page=repo.list_errors(org,connector,run.id,category="source_read",retryable=True,item_id=item.id);assert page.items[0].id==row.id and page.items[0].details=={"operation":"read"};assert not hasattr(repo,"update_error") and not hasattr(repo,"delete_error")

def test_cursor_atomic_promotion_history_rollback_and_retention(session):
    org,connector,scope=_setup(session);repo=ConnectorSyncRepository(session);run=_run(org,connector,scope);repo.add_run(org,connector,scope,run);session.commit();assert repo.get_active_cursor(org,connector,scope) is None
    first=repo.replace_active_cursor(org,connector,scope,run.id,version=1,cursor_type="page_token",activated_at=NOW,safe_cursor={"page":1});session.commit();second=repo.replace_active_cursor(org,connector,scope,run.id,version=2,cursor_type="page_token",activated_at=NOW,secret_reference="vault://cursor/2");session.commit();assert repo.get_active_cursor(org,connector,scope).id==second.id;history=repo.list_cursors(org,connector,scope,limit=10);assert [x.cursor_version for x in history.items]==[1,2] and history.items[0].state=="superseded"
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.replace_active_cursor(org,connector,scope,run.id,version=2,cursor_type="page_token",activated_at=NOW,safe_cursor={"page":2})
    session.rollback();assert repo.get_active_cursor(org,connector,scope).id==second.id
    with pytest.raises(Exception):session.execute(text("DELETE FROM connector_sync_runs WHERE id=:id"),{"id":run.id});session.rollback()

def test_atomic_operational_transaction_rolls_back_all(session):
    org,connector,scope=_setup(session,"Atomic");source_repo=SourceItemRepository(session);sync=ConnectorSyncRepository(session);source=_item(org,connector);source_repo.add(org,connector,source);source_repo.add_membership(org,connector,_membership(org,connector,scope,source.id));run=_run(org,connector,scope);sync.add_run(org,connector,scope,run);sync.add_item(org,connector,scope,run.id,_sync_item(org,connector,scope,run.id,source=source.id));sync.add_error(org,connector,scope,run.id,SafeSyncError("source_read","failed","Safe",False,1,{},NOW));sync.replace_active_cursor(org,connector,scope,run.id,version=1,cursor_type="page",activated_at=NOW,safe_cursor={"page":1})
    with pytest.raises(ConnectorRepositoryConflict):source_repo.add(org,connector,_item(org,connector))
    session.rollback();assert source_repo.get_by_id(org,connector,source.id) is None and sync.get_run(org,connector,scope,run.id) is None
