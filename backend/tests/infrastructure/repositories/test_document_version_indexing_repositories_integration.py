from __future__ import annotations
import os,subprocess,threading,uuid
from datetime import datetime,timedelta,timezone
from pathlib import Path
import pytest
from sqlalchemy import create_engine,func,select,text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session,sessionmaker
from infrastructure.db.models import DocumentIndexingState
from infrastructure.repositories.connector_repository import ConnectorRepositoryConflict,InvalidConnectorRepositoryRequest
from infrastructure.repositories.document_version_repository import DocumentVersionRepository
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository
ROOT=Path(__file__).resolve().parents[3];PYTHON=ROOT/".venv"/"Scripts"/"python.exe";INI=ROOT/"alembic.ini";NOW=datetime(2026,8,25,tzinfo=timezone.utc)
def _identity(url):value=make_url(url);return value.drivername,value.host,value.port,value.database
@pytest.fixture(scope="module")
def engine():
    url=os.environ["TEST_DATABASE_URL"];dev=os.environ.get("DATABASE_URL")
    if dev and _identity(dev)==_identity(url):raise RuntimeError("test DB must differ")
    reset=create_engine(url,future=True)
    with reset.begin() as connection:connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"));connection.execute(text("CREATE SCHEMA public"))
    reset.dispose();environment=os.environ.copy();environment["DATABASE_URL"]=url;subprocess.run([str(PYTHON),"-m","alembic","-c",str(INI),"upgrade","head"],check=True,cwd=str(ROOT),env=environment);value=create_engine(url,future=True)
    try:yield value
    finally:value.dispose()
@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in ("source_acl_entries","source_acl_snapshots","external_group_memberships","external_directory_states","user_external_identity_links","external_principals","document_indexing_attempts","document_indexing_states","document_version_documents","document_versions","connector_sync_cursors","connector_sync_errors","connector_sync_items","connector_sync_runs","source_item_scope_memberships","source_items","connector_scopes","connectors","audit_events","knowledge_space_user_grants","knowledge_space_team_grants","knowledge_space_department_grants","knowledge_space_organization_grants","knowledge_spaces","team_memberships","department_memberships","teams","departments","document_chunks","documents","authentication_sessions","user_roles","users","organization_settings","organizations","industries"):connection.execute(text(f"DELETE FROM {table}"))
@pytest.fixture
def session(engine):
    value=sessionmaker(bind=engine,class_=Session,autoflush=False,expire_on_commit=False)()
    try:yield value
    finally:value.rollback();value.close()
def _setup(session,name):
    org,connector,space,scope,source=(uuid.uuid4() for _ in range(5));session.execute(text("INSERT INTO organizations(id,name,slug)VALUES(:id,:name,:slug)"),{"id":org,"name":name,"slug":f"{name.lower()}-{org}"});session.execute(text("INSERT INTO connectors(id,organization_id,connector_type,display_name,slug)VALUES(:id,:org,'local_folder',:name,:slug)"),{"id":connector,"org":org,"name":name,"slug":f"connector-{str(connector)[:8]}"});session.execute(text("INSERT INTO knowledge_spaces(id,organization_id,name,slug)VALUES(:id,:org,:name,:slug)"),{"id":space,"org":org,"name":name,"slug":f"space-{str(space)[:8]}"});session.execute(text("INSERT INTO connector_scopes(id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode)VALUES(:id,:org,:connector,:space,:name,:slug,'folder',:key,'platform_managed')"),{"id":scope,"org":org,"connector":connector,"space":space,"name":name,"slug":f"scope-{str(scope)[:8]}","key":f"/{scope}"});session.execute(text("INSERT INTO source_items(id,organization_id,connector_id,source_item_key,source_item_type,title,first_seen_at,last_seen_at)VALUES(:id,:org,:connector,:key,'file',:key,:now,:now)"),{"id":source,"org":org,"connector":connector,"key":f"file-{source}","now":NOW});return org,connector,scope,source
def _document(session,org,key):
    value=uuid.uuid4();session.execute(text("INSERT INTO documents(id,organization_id,source_type,source_document_key,title)VALUES(:id,:org,'manual_upload',:key,:key)"),{"id":value,"org":org,"key":key});return value
def _state(org,version,fingerprint="profile-a",requested=NOW,next_retry=None):return DocumentIndexingState(id=uuid.uuid4(),organization_id=org,document_version_id=version,extraction_profile="default",extraction_version="v1",chunking_profile="deterministic_text",chunking_version="v1",embedding_provider="openai",embedding_model="text-embedding-3-small",embedding_dimensions=1536,profile_fingerprint=fingerprint,desired_generation=1,indexed_generation=None,status="pending",reason="new_version",attempt_count=0,requested_at=requested,next_retry_at=next_retry)
def _sync(session,org,connector,scope):
    run,item=uuid.uuid4(),uuid.uuid4();session.execute(text("INSERT INTO connector_sync_runs(id,organization_id,connector_id,connector_scope_id,mode,trigger_type)VALUES(:id,:org,:connector,:scope,'incremental','manual')"),{"id":run,"org":org,"connector":connector,"scope":scope});session.execute(text("INSERT INTO connector_sync_items(id,organization_id,connector_id,connector_scope_id,sync_run_id,source_item_key,change_type)VALUES(:id,:org,:connector,:scope,:run,:key,'changed')"),{"id":item,"org":org,"connector":connector,"scope":scope,"run":run,"key":str(item)});return run,item
def _create_version(repo,org,source,cause="discovered"):return repo.create_current_version(org,source,version_cause=cause,lifecycle="available",discovered_at=NOW,content_checksum=str(uuid.uuid4()),checksum_algorithm="sha256",source_size_bytes=10,metadata={"schema_version":1})

def test_versions_tenant_history_pagination_and_rollback(session):
    org,_,_,source=_setup(session,"Alpha");other_org,_,_,other_source=_setup(session,"Beta");repo=DocumentVersionRepository(session);first=_create_version(repo,org,source);session.commit();second=_create_version(repo,org,source,"content_changed");session.commit();assert [row.version_number for row in repo.list_history(org,source).items]==[1,2];assert repo.get_by_id(org,source,first.id).is_current is False and repo.get_current(org,source).id==second.id and repo.get_by_number(org,source,2).id==second.id;assert repo.get_by_id(other_org,other_source,second.id) is None
    third=_create_version(repo,org,source,"metadata_changed");assert third.version_number==3;session.rollback();session.expire_all();assert repo.get_current(org,source).id==second.id and repo.get_by_number(org,source,3) is None
    page1=repo.list_history(org,source,limit=1);page2=repo.list_history(org,source,limit=1,cursor=page1.next_cursor);assert {page1.items[0].id,page2.items[0].id}=={first.id,second.id}

def test_concurrent_version_creation_is_serialized(engine,session):
    org,_,_,source=_setup(session,"Versions");session.commit();factory=sessionmaker(bind=engine,class_=Session,autoflush=False,expire_on_commit=False);first=factory();second=factory();started=threading.Event();outcome=[]
    try:
        one=_create_version(DocumentVersionRepository(first),org,source);thread=threading.Thread(target=lambda:(started.set(),outcome.append(_create_version(DocumentVersionRepository(second),org,source,"content_changed")),second.commit()));thread.start();started.wait(5);first.commit();thread.join(10);assert not thread.is_alive();assert one.version_number==1 and outcome[0].version_number==2
    finally:first.rollback();second.rollback();first.close();second.close()
    rows=DocumentVersionRepository(session).list_history(org,source).items;assert [row.version_number for row in rows]==[1,2] and sum(row.is_current for row in rows)==1

def test_materialization_replace_rollback_remove_preserves_content(session):
    org,_,_,source=_setup(session,"Mapping");other_org,_,_,other_source=_setup(session,"Other");repo=DocumentVersionRepository(session);first=_create_version(repo,org,source);document=_document(session,org,"logical");other_document=_document(session,other_org,"foreign");mapping=repo.replace_materialization(org,source,first.id,document);session.commit();second=_create_version(repo,org,source,"content_changed");repo.replace_materialization(org,source,second.id,document);session.commit();assert repo.get_current_materialization(org,source).document_version_id==second.id and repo.get_by_id(org,source,first.id) is not None
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.replace_materialization(org,source,second.id,other_document)
    session.rollback();repo.replace_materialization(org,source,first.id,document);session.rollback();assert repo.get_current_materialization(org,source).document_version_id==second.id
    assert repo.remove_materialization(org,source,second.id);session.commit();assert repo.get_materialization(org,second.id) is None and session.execute(text("SELECT count(*) FROM documents WHERE id=:id"),{"id":document}).scalar_one()==1
    assert repo.get_current_materialization(other_org,other_source) is None and mapping.id

def test_indexing_state_profiles_work_filters_generation_and_rollback(session):
    org,_,_,source=_setup(session,"States");other_org,_,_,_=_setup(session,"Foreign");version=_create_version(DocumentVersionRepository(session),org,source);repo=DocumentIndexingRepository(session);state=repo.add_state(org,version.id,_state(org,version.id,next_retry=NOW));repo.add_state(org,version.id,_state(org,version.id,"profile-b",NOW+timedelta(seconds=1),NOW+timedelta(hours=1)));session.commit();assert repo.get_state(org,version.id,"profile-a").id==state.id and repo.get_state(other_org,version.id,"profile-a") is None;assert repo.get_or_create_state(org,version.id,_state(org,version.id)).id==state.id
    with pytest.raises(ConnectorRepositoryConflict):repo.add_state(org,version.id,_state(org,version.id))
    session.rollback();page=repo.list_work(org,status="pending",embedding_model="text-embedding-3-small",embedding_dimensions=1536,retry_ready_at=NOW+timedelta(minutes=1));assert [row.profile_fingerprint for row in page.items]==["profile-a"]
    changed=repo.request_generation(org,version.id,"profile-a",desired_generation=2,status="stale",reason="repair",requested_at=NOW+timedelta(minutes=2));assert changed.desired_generation==2;session.rollback();session.expire_all();assert repo.get_state(org,version.id,"profile-a").desired_generation==1
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.request_generation(org,version.id,"profile-a",desired_generation=1,status="stale",reason="repair",requested_at=NOW)

def test_concurrent_state_get_or_create_is_serialized(engine,session):
    org,_,_,source=_setup(session,"StateRace");version=_create_version(DocumentVersionRepository(session),org,source);session.commit();factory=sessionmaker(bind=engine,class_=Session,autoflush=False,expire_on_commit=False);first=factory();second=factory();started=threading.Event();rows=[]
    try:
        initial=DocumentIndexingRepository(first).get_or_create_state(org,version.id,_state(org,version.id));thread=threading.Thread(target=lambda:(started.set(),rows.append(DocumentIndexingRepository(second).get_or_create_state(org,version.id,_state(org,version.id))),second.commit()));thread.start();started.wait(5);first.commit();thread.join(10);assert not thread.is_alive();assert rows[0].id==initial.id
    finally:first.rollback();second.rollback();first.close();second.close()
    assert session.execute(select(func.count()).select_from(DocumentIndexingState).where(DocumentIndexingState.organization_id==org)).scalar_one()==1

def test_attempt_allocation_completion_attribution_and_rollback(session):
    org,connector,scope,source=_setup(session,"Attempts");other_org,other_connector,other_scope,_=_setup(session,"AttemptsOther");version=_create_version(DocumentVersionRepository(session),org,source);repo=DocumentIndexingRepository(session);state=repo.add_state(org,version.id,_state(org,version.id));run,item=_sync(session,org,connector,scope);other_run,other_item=_sync(session,other_org,other_connector,other_scope);session.commit();first=repo.allocate_attempt(org,version.id,"profile-a",trigger_type="sync",started_at=NOW,sync_run_id=run,sync_item_id=item);session.commit();second=repo.allocate_attempt(org,version.id,"profile-a",trigger_type="retry",started_at=NOW+timedelta(minutes=1));session.commit();assert [row.attempt_number for row in repo.list_attempts(org,state.id).items]==[1,2];assert repo.get_state(org,version.id,"profile-a").attempt_count==2
    repo.complete_attempt(org,state.id,first.id,status="succeeded",completed_at=NOW+timedelta(minutes=2),retryable=False,summary={"chunks_indexed":2});repo.complete_attempt(org,state.id,second.id,status="failed",completed_at=NOW+timedelta(minutes=3),retryable=True,error_category="embedding",error_code="provider_timeout",summary={"retry_count":1});session.commit();assert repo.get_attempt(org,state.id,first.id).error_code is None and repo.get_attempt(org,state.id,second.id).error_code=="provider_timeout"
    with pytest.raises(ConnectorRepositoryConflict):repo.allocate_attempt(org,version.id,"profile-a",trigger_type="sync",started_at=NOW,sync_run_id=other_run,sync_item_id=other_item)
    session.rollback();before=repo.get_state(org,version.id,"profile-a").attempt_count;transient=repo.allocate_attempt(org,version.id,"profile-a",trigger_type="retry",started_at=NOW);session.rollback();assert repo.get_attempt(org,state.id,transient.id) is None and repo.get_state(org,version.id,"profile-a").attempt_count==before

def test_concurrent_attempt_allocation_is_serialized(engine,session):
    org,_,_,source=_setup(session,"AttemptRace");version=_create_version(DocumentVersionRepository(session),org,source);state=DocumentIndexingRepository(session).add_state(org,version.id,_state(org,version.id));session.commit();factory=sessionmaker(bind=engine,class_=Session,autoflush=False,expire_on_commit=False);first=factory();second=factory();started=threading.Event();rows=[]
    try:
        one=DocumentIndexingRepository(first).allocate_attempt(org,version.id,"profile-a",trigger_type="retry",started_at=NOW);thread=threading.Thread(target=lambda:(started.set(),rows.append(DocumentIndexingRepository(second).allocate_attempt(org,version.id,"profile-a",trigger_type="retry",started_at=NOW)),second.commit()));thread.start();started.wait(5);first.commit();thread.join(10);assert not thread.is_alive();assert {one.attempt_number,rows[0].attempt_number}=={1,2}
    finally:first.rollback();second.rollback();first.close();second.close()
    assert [row.attempt_number for row in DocumentIndexingRepository(session).list_attempts(org,state.id).items]==[1,2]

def test_full_atomic_rollback_restores_version_mapping_and_state(session):
    org,_,_,source=_setup(session,"Atomic");versions=DocumentVersionRepository(session);indexing=DocumentIndexingRepository(session);first=_create_version(versions,org,source);document=_document(session,org,"atomic");versions.replace_materialization(org,source,first.id,document);state=indexing.add_state(org,first.id,_state(org,first.id));session.commit();second=_create_version(versions,org,source,"content_changed");versions.replace_materialization(org,source,second.id,document);attempt=indexing.allocate_attempt(org,first.id,"profile-a",trigger_type="repair",started_at=NOW)
    with pytest.raises(ConnectorRepositoryConflict):indexing.add_state(org,first.id,_state(org,first.id))
    session.rollback();session.expire_all();assert versions.get_current(org,source).id==first.id;assert versions.get_current_materialization(org,source).document_version_id==first.id;assert indexing.get_state(org,first.id,"profile-a").attempt_count==0;assert indexing.get_attempt(org,state.id,attempt.id) is None

def test_required_query_shapes_have_index_paths(session):
    org,source,version,state=(uuid.uuid4() for _ in range(4));session.execute(text("SET LOCAL enable_seqscan = off"));queries=(
        ("SELECT id FROM document_versions WHERE organization_id=:org AND source_item_id=:source AND is_current",{"org":org,"source":source}),
        ("SELECT id FROM document_versions WHERE organization_id=:org AND source_item_id=:source ORDER BY version_number,id LIMIT 10",{"org":org,"source":source}),
        ("SELECT m.id FROM document_versions v JOIN document_version_documents m ON m.organization_id=v.organization_id AND m.document_version_id=v.id WHERE v.organization_id=:org AND v.source_item_id=:source AND v.is_current",{"org":org,"source":source}),
        ("SELECT id FROM document_indexing_states WHERE organization_id=:org AND status='pending' ORDER BY requested_at,id LIMIT 10",{"org":org}),
        ("SELECT id FROM document_indexing_attempts WHERE organization_id=:org AND indexing_state_id=:state ORDER BY attempt_number,id LIMIT 10",{"org":org,"state":state}),
    )
    for sql,parameters in queries:
        plan="\n".join(row[0] for row in session.execute(text(f"EXPLAIN (COSTS OFF) {sql}"),parameters));assert "Index" in plan
