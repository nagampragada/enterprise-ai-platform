from __future__ import annotations
from dataclasses import FrozenInstanceError
from datetime import datetime,timezone
from unittest.mock import Mock
from uuid import uuid4
import pytest
from infrastructure.db.models import DocumentIndexingAttempt,DocumentIndexingState,DocumentVersion,SourceItem
from infrastructure.repositories.connector_repository import InvalidConnectorRepositoryRequest
from infrastructure.repositories.document_version_repository import DocumentVersionPageCursor,DocumentVersionRepository
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository,IndexingAttemptPageCursor,IndexingWorkPageCursor
NOW=datetime(2026,8,25,tzinfo=timezone.utc)

class Result:
    def __init__(self,one=None,rows=()):self.one=one;self.rows=rows
    def scalar_one_or_none(self):return self.one
    def scalar_one(self):return self.one
    def scalars(self):return self
    def all(self):return list(self.rows)

def _source(org,connector,source):return SourceItem(id=source,organization_id=org,connector_id=connector,source_item_key="file",source_item_type="file",title="File",first_seen_at=NOW,last_seen_at=NOW,status="active",source_metadata={},metadata_schema_version=1)
def _version(org,connector,source,number=1):return DocumentVersion(id=uuid4(),organization_id=org,connector_id=connector,source_item_id=source,version_number=number,version_cause="discovered",lifecycle="available",is_current=True,discovered_at=NOW,version_metadata={},metadata_schema_version=1,created_at=NOW)
def _state(org,version,fingerprint="profile-a"):
    return DocumentIndexingState(id=uuid4(),organization_id=org,document_version_id=version,extraction_profile="default",extraction_version="v1",chunking_profile="text",chunking_version="v1",embedding_provider="openai",embedding_model="text-embedding-3-small",embedding_dimensions=1536,profile_fingerprint=fingerprint,desired_generation=1,indexed_generation=None,status="pending",reason="new_version",attempt_count=0,requested_at=NOW,created_at=NOW,updated_at=NOW)
def _attempt(org,state,number=1):return DocumentIndexingAttempt(id=uuid4(),organization_id=org,indexing_state_id=state,attempt_number=number,trigger_type="sync",status="running",started_at=NOW,retryable=False,summary={},summary_schema_version=1,created_at=NOW)

def test_version_invalid_inputs_fail_before_sql():
    session=Mock();repo=DocumentVersionRepository(session);org,source=uuid4(),uuid4()
    calls=(("get_by_id",("bad",source,uuid4()),{}),("get_by_number",(org,source,True),{}),("list_history",(org,source),{"limit":True}),("list_history",(org,source),{"lifecycle":"bad"}),("list_history",(org,source),{"cursor":DocumentVersionPageCursor(1,"bad")}))
    for name,args,kwargs in calls:
        with pytest.raises(InvalidConnectorRepositoryRequest):getattr(repo,name)(*args,**kwargs)
    session.execute.assert_not_called()

def test_version_observation_validation_precedes_parent_lock():
    session=Mock();repo=DocumentVersionRepository(session);org,source=uuid4(),uuid4();base=dict(version_cause="discovered",lifecycle="available",discovered_at=NOW)
    invalid=({"content_checksum":"abc"},{"source_size_bytes":-1},{"discovered_at":datetime.now()},{"version_cause":"bad"},{"lifecycle":"bad"},{"metadata":[]},{"metadata":{"content":"raw"}},{"metadata":{"value":float("nan")}})
    for values in invalid:
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.create_current_version(org,source,**(base|values))
    session.execute.assert_not_called()

def test_version_creation_locks_parent_demotes_and_never_owns_transaction():
    org,connector,source=uuid4(),uuid4(),uuid4();parent=_source(org,connector,source);current=_version(org,connector,source);session=Mock();session.execute.side_effect=[Result(parent),Result(current),Result(2)];repo=DocumentVersionRepository(session)
    created=repo.create_current_version(org,source,version_cause="content_changed",lifecycle="available",discovered_at=NOW,content_checksum="abc",checksum_algorithm="sha256",metadata={"schema":1})
    assert created.version_number==2 and created.is_current and current.is_current is False
    assert "FOR UPDATE" in str(session.execute.call_args_list[0].args[0]).upper()
    assert session.flush.call_count==2;session.commit.assert_not_called();session.rollback.assert_not_called()
    assert not hasattr(repo,"update") and not hasattr(repo,"patch")

def test_version_page_is_immutable_and_uses_limit_plus_one():
    org,connector,source=uuid4(),uuid4(),uuid4();rows=[_version(org,connector,source,index) for index in range(1,4)];session=Mock();session.execute.return_value=Result(rows=rows);page=DocumentVersionRepository(session).list_history(org,source,limit=2)
    assert len(page.items)==2 and page.has_more and session.execute.call_args.args[0]._limit_clause.value==3
    with pytest.raises(FrozenInstanceError):page.limit=3 # type: ignore[misc]

def test_materialization_validation_and_transaction_ownership():
    session=Mock();repo=DocumentVersionRepository(session);org,source,version,document=uuid4(),uuid4(),uuid4(),uuid4()
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.replace_materialization(org,source,"bad",document)
    session.execute.assert_not_called()
    assert not hasattr(repo,"move_document") and not hasattr(repo,"delete_version")

def test_indexing_state_validation_and_generation_before_sql():
    org,version=uuid4(),uuid4();session=Mock();repo=DocumentIndexingRepository(session)
    invalid=[_state(org,version) for _ in range(5)];invalid[0].embedding_dimensions=0;invalid[1].profile_fingerprint="Bad Profile";invalid[2].desired_generation=True;invalid[3].status="bad";invalid[4].reason="bad"
    for state in invalid:
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.add_state(org,version,state)
    for values in ({"desired_generation":True,"status":"pending","reason":"repair","requested_at":NOW},{"desired_generation":2,"status":"bad","reason":"repair","requested_at":NOW},{"desired_generation":2,"status":"pending","reason":"repair","requested_at":datetime.now()}):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.request_generation(org,version,"profile-a",**values)
    session.execute.assert_not_called()
    assert not hasattr(repo,"patch_state") and not hasattr(repo,"claim_work")

def test_indexing_pages_are_bounded_immutable_and_filtered():
    org,version=uuid4(),uuid4();states=[_state(org,version,f"profile-{index}") for index in range(3)];session=Mock();session.execute.return_value=Result(rows=states);repo=DocumentIndexingRepository(session);page=repo.list_work(org,limit=2,status="pending",embedding_dimensions=1536)
    assert page.has_more and len(page.items)==2 and session.execute.call_args.args[0]._limit_clause.value==3
    with pytest.raises(FrozenInstanceError):page.next_cursor=IndexingWorkPageCursor(NOW,uuid4()) # type: ignore[misc]
    session.execute.return_value=Result(rows=[_attempt(org,uuid4(),1),_attempt(org,uuid4(),2)]);attempts=repo.list_attempts(org,uuid4(),limit=1)
    assert attempts.has_more and session.execute.call_args.args[0]._limit_clause.value==2
    with pytest.raises(FrozenInstanceError):attempts.next_cursor=IndexingAttemptPageCursor(1,uuid4()) # type: ignore[misc]

def test_attempt_validation_lock_completion_and_append_only_contract():
    org,version=uuid4(),uuid4();session=Mock();repo=DocumentIndexingRepository(session)
    for kwargs in ({"trigger_type":"bad","started_at":NOW},{"trigger_type":"sync","started_at":datetime.now()},{"trigger_type":"sync","started_at":NOW,"sync_item_id":uuid4()}):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.allocate_attempt(org,version,"profile-a",**kwargs)
    for summary in ([],{"content":"raw"},{"trace":"Traceback (most recent call last)"},{"value":float("nan")}):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.complete_attempt(org,uuid4(),uuid4(),status="failed",completed_at=NOW,retryable=True,error_category="embedding",error_code="failed",summary=summary)
    session.execute.assert_not_called()
    attempt=_attempt(org,uuid4());session.execute.return_value=Result(attempt);completed=repo.complete_attempt(org,attempt.indexing_state_id,attempt.id,status="succeeded",completed_at=NOW,retryable=False,summary={"chunks_indexed":2})
    assert completed.status=="succeeded" and "FOR UPDATE" in str(session.execute.call_args.args[0]).upper();session.commit.assert_not_called();session.rollback.assert_not_called()
    assert not hasattr(repo,"delete_attempt") and not hasattr(repo,"update_attempt")
