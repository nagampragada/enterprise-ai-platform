from __future__ import annotations
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4
import pytest
from infrastructure.db.models import SourceItem, SourceItemScopeMembership, ConnectorSyncRun, ConnectorSyncItem
from infrastructure.repositories.connector_repository import InvalidConnectorRepositoryRequest
from infrastructure.repositories.source_item_repository import MembershipReconciliationCursor, SourceItemRepository, SourceItemPageCursor
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository, SafeSyncError, SyncPageCursor
NOW=datetime(2026,8,24,tzinfo=timezone.utc)

def _item(org=None,connector=None,key="Case/Key"):
    return SourceItem(id=uuid4(),organization_id=org or uuid4(),connector_id=connector or uuid4(),source_item_key=key,source_item_type="file",title="File",first_seen_at=NOW,last_seen_at=NOW,status="active",source_metadata={},metadata_schema_version=1,created_at=NOW)
def _membership(org,connector):return SourceItemScopeMembership(id=uuid4(),organization_id=org,connector_id=connector,source_item_id=uuid4(),connector_scope_id=uuid4(),status="active",first_discovered_at=NOW,last_seen_at=NOW)
def _run(org,connector,scope):return ConnectorSyncRun(id=uuid4(),organization_id=org,connector_id=connector,connector_scope_id=scope,mode="incremental",trigger_type="manual",status="queued",run_metadata={},created_at=NOW)
def _sync_item(org,connector,scope,run,key="Case/Key"):return ConnectorSyncItem(id=uuid4(),organization_id=org,connector_id=connector,connector_scope_id=scope,sync_run_id=run,source_item_key=key,change_type="new",processing_status="pending",attempt_count=0,created_at=NOW)

def test_source_invalid_inputs_before_sql_and_case_preserved():
    session=Mock();repo=SourceItemRepository(session);org,connector=uuid4(),uuid4()
    for args in (("bad",connector,uuid4()),(org,"bad",uuid4()),(org,connector,"bad")):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.get_by_id(*args)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.get_by_key(org,connector," ")
    item=_item(org,connector);session.flush.return_value=None;repo.add(org,connector,item);assert item.source_item_key=="Case/Key"
    session.execute.assert_not_called()

def test_source_json_size_time_limit_cursor_and_lock_contracts():
    session=Mock();repo=SourceItemRepository(session);org,connector=uuid4(),uuid4()
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.update_provider_state(org,connector,uuid4(),source_metadata=[],metadata_schema_version=1,last_seen_at=NOW) # type: ignore[arg-type]
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.update_provider_state(org,connector,uuid4(),source_metadata={"x":float("nan")},metadata_schema_version=1,last_seen_at=NOW)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.list_page(org,connector,limit=True)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.list_page(org,connector,cursor=SourceItemPageCursor(datetime.now(),uuid4()))
    session.execute.return_value.scalar_one_or_none.return_value=None;repo.lock_by_id(org,connector,uuid4());sql=str(session.execute.call_args.args[0]).upper();assert "FOR UPDATE" in sql and "ORGANIZATION_ID" in sql and "CONNECTOR_ID" in sql

def test_source_page_immutable_limit_plus_one_and_no_patch():
    org,connector=uuid4(),uuid4();rows=[_item(org,connector,str(i)) for i in range(3)];session=Mock();session.execute.return_value.scalars.return_value.all.return_value=rows;page=SourceItemRepository(session).list_page(org,connector,limit=2);assert len(page.items)==2 and page.has_more and page.next_cursor;assert session.execute.call_args.args[0]._limit_clause.value==3
    with pytest.raises(FrozenInstanceError):page.limit=3 # type: ignore[misc]
    assert not hasattr(SourceItemRepository(Mock()),"update")

def test_membership_validation_and_methods_never_commit():
    session=Mock();repo=SourceItemRepository(session);org,connector=uuid4(),uuid4();membership=_membership(org,connector);repo.add_membership(org,connector,membership);repo.remove_membership(org,connector,membership.connector_scope_id,membership.source_item_id,NOW);repo.reactivate_membership(org,connector,membership.connector_scope_id,membership.source_item_id,NOW);session.commit.assert_not_called();session.rollback.assert_not_called()

def test_membership_reconciliation_page_is_bounded_immutable_and_tenant_scoped():
    org,connector,scope=uuid4(),uuid4(),uuid4();memberships=[_membership(org,connector) for _ in range(3)]
    for membership in memberships:membership.connector_scope_id=scope;membership.last_seen_at=NOW
    session=Mock();session.execute.return_value.scalars.return_value.all.return_value=memberships;page=SourceItemRepository(session).list_active_memberships_before(org,connector,scope,NOW,limit=2)
    statement=session.execute.call_args.args[0];sql=str(statement).upper();assert page.has_more and len(page.items)==2 and statement._limit_clause.value==3;assert "ORGANIZATION_ID" in sql and "CONNECTOR_ID" in sql and "CONNECTOR_SCOPE_ID" in sql and "LAST_SEEN_AT" in sql
    with pytest.raises(FrozenInstanceError):page.next_cursor=MembershipReconciliationCursor(NOW,uuid4()) # type: ignore[misc]

def test_membership_reconciliation_validation_and_active_existence():
    org,connector,scope,source=uuid4(),uuid4(),uuid4(),uuid4();session=Mock();repo=SourceItemRepository(session)
    for args,kwargs in ((("bad",connector,scope,NOW),{}),((org,connector,scope,datetime.now()),{}),((org,connector,scope,NOW),{"limit":True}),((org,connector,scope,NOW),{"cursor":MembershipReconciliationCursor(NOW,"bad")})):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.list_active_memberships_before(*args,**kwargs)
    session.execute.assert_not_called();session.execute.return_value.scalar_one_or_none.return_value=uuid4();assert repo.has_active_membership(org,connector,source)

def test_github_reconciliation_page_is_repository_bounded_and_allows_hard_maximum():
    org,connector,scope=uuid4(),uuid4(),uuid4();memberships=[_membership(org,connector) for _ in range(2)]
    for membership in memberships:membership.connector_scope_id=scope;membership.last_seen_at=NOW
    session=Mock();session.execute.return_value.scalars.return_value.all.return_value=memberships
    page=SourceItemRepository(session).list_active_github_memberships_before(org,connector,scope,501,NOW,limit=500)
    statement=session.execute.call_args.args[0];sql=str(statement).upper()
    assert page.items==tuple(memberships) and not page.has_more and statement._limit_clause.value==501
    for term in ("SOURCE_ITEMS", "ORGANIZATION_ID", "CONNECTOR_ID", "CONNECTOR_SCOPE_ID", "LAST_SEEN_AT"):
        assert term in sql
    parameter_values=list(statement.compile().params.values())
    assert "github" in parameter_values and "501" in parameter_values
    session.reset_mock()
    for repository_id,limit in ((True,100),(0,100),(501,501)):
        with pytest.raises(InvalidConnectorRepositoryRequest):
            SourceItemRepository(session).list_active_github_memberships_before(org,connector,scope,repository_id,NOW,limit=limit)
    session.execute.assert_not_called()

def test_sync_validation_locks_counters_and_pages():
    session=Mock();repo=ConnectorSyncRepository(session);org,connector,scope=uuid4(),uuid4(),uuid4();run=_run(org,connector,scope);repo.add_run(org,connector,scope,run)
    bad=_run(org,connector,scope);bad.mode="bad"
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.add_run(org,connector,scope,bad)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.increment_counters(org,connector,scope,run.id,items_failed=True)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.increment_counters(org,connector,scope,run.id,unknown=1)
    session.execute.return_value.scalar_one_or_none.return_value=None;repo.lock_run(org,connector,scope,run.id);assert "FOR UPDATE" in str(session.execute.call_args.args[0]).upper()
    session.execute.return_value.scalars.return_value.all.return_value=[run,run,run];page=repo.list_runs(org,limit=2);assert page.has_more and session.execute.call_args.args[0]._limit_clause.value==3

def test_sync_item_key_status_and_lock_contract():
    session=Mock();repo=ConnectorSyncRepository(session);org,connector,scope,run=uuid4(),uuid4(),uuid4(),uuid4();item=_sync_item(org,connector,scope,run);repo.add_item(org,connector,scope,run,item);assert item.source_item_key=="Case/Key"
    bad=_sync_item(org,connector,scope,run," ")
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.add_item(org,connector,scope,run,bad)
    session.execute.return_value.scalar_one_or_none.return_value=None;repo.lock_item(org,connector,run,item.id);sql=str(session.execute.call_args.args[0]).upper();assert "FOR UPDATE" in sql and "SYNC_RUN_ID" in sql

def test_safe_error_boundary_rejects_stack_and_exception_objects():
    repo=ConnectorSyncRepository(Mock());org,connector,scope,run=uuid4(),uuid4(),uuid4(),uuid4()
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.add_error(org,connector,scope,run,Exception("bad")) # type: ignore[arg-type]
    error=SafeSyncError("source_read","read_failed","Traceback (most recent call last)\nsecret",True,1,{},NOW)
    with pytest.raises(InvalidConnectorRepositoryRequest):repo.add_error(org,connector,scope,run,error)

def test_cursor_storage_version_and_transaction_ownership():
    session=Mock();repo=ConnectorSyncRepository(session);org,connector,scope,run=uuid4(),uuid4(),uuid4(),uuid4();session.execute.return_value.scalar_one_or_none.return_value=None
    for kwargs in ({},{"safe_cursor":{},"secret_reference":"vault"},{"safe_cursor":[]},{"secret_reference":" "}):
        with pytest.raises(InvalidConnectorRepositoryRequest):repo.replace_active_cursor(org,connector,scope,run,version=1,cursor_type="page",activated_at=NOW,**kwargs)
    repo.replace_active_cursor(org,connector,scope,run,version=1,cursor_type="page",activated_at=NOW,safe_cursor={"page":1});session.flush.assert_called();session.commit.assert_not_called();session.rollback.assert_not_called()
