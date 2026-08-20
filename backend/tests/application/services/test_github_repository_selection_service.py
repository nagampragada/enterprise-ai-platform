from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.ports.github_app import (
    GitHubInstallationAccessToken,
    GitHubRepository,
    GitHubRepositoryAccessGrant,
    GitHubRepositoryPage,
)
from application.services.github_repository_selection_service import (
    GitHubRepositorySelectionConflict,
    GitHubRepositorySelectionContext,
    GitHubRepositorySelectionNotFound,
    GitHubRepositorySelectionRejected,
    GitHubRepositorySelectionService,
)
from infrastructure.db.models import ConnectorScope
from infrastructure.repositories.connector_credential_repository import CredentialMetadata
from infrastructure.repositories.connector_scope_repository import ConnectorScopePage
from infrastructure.repositories.github_app_installation_repository import GitHubInstallationView


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)


def repository(identifier=501):
    return GitHubRepository(
        identifier, "docs", "fake-org/docs", "fake-org", True, "private",
        False, False, "main", "https://github.com/fake-org/docs", NOW,
    )


def _service():
    session = Mock()
    session.execute.return_value.scalar_one_or_none.return_value = uuid4()
    client = Mock(app_id=123)
    value = GitHubRepositorySelectionService(session, client, clock=lambda: NOW)
    value._connectors = Mock()
    value._credentials = Mock()
    value._installations = Mock()
    value._scopes = Mock()
    organization_id, connector_id, space_id, user_id, credential_id = (
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    )
    connector = SimpleNamespace(connector_type="github", status="active")
    credential = CredentialMetadata(
        credential_id, connector_id, "github", "app_installation", "active", "77",
        "fake-org", ("contents:read", "metadata:read"), None, NOW, None, NOW, NOW,
    )
    installation = GitHubInstallationView(
        connector_id, credential_id, 123, 77, 99, "fake-org", "Organization",
        "selected", "connected", NOW, NOW, NOW, None, NOW, NOW,
    )
    value._connectors.get_by_id.return_value = connector
    value._connectors.lock_by_id.return_value = connector
    value._credentials.get.return_value = credential
    value._credentials.lock.return_value = SimpleNamespace(
        id=credential_id, status="active", provider_key="github",
        auth_scheme="app_installation", external_subject="77",
        granted_scopes=["contents:read", "metadata:read"], expires_at=None,
    )
    value._installations.get.return_value = installation
    value._installations.lock.return_value = SimpleNamespace(**installation.__dict__)
    context = GitHubRepositorySelectionContext(
        organization_id, connector_id, space_id, credential_id, 77, 123, 99,
        "fake-org", 501,
    )
    return value, client, context, user_id


def _scope(context, *, space_id=None, status="active"):
    removed_at = NOW if status == "removed" else None
    return ConnectorScope(
        id=uuid4(), organization_id=context.organization_id,
        connector_id=context.connector_id,
        knowledge_space_id=space_id or context.knowledge_space_id,
        display_name="fake-org/docs", slug="github-repository-501",
        scope_type="repository", external_scope_key="github:repository:501",
        access_mode="platform_managed", status=status,
        safe_config={
            "repository_id":501,"repository_name":"docs",
            "repository_full_name":"fake-org/docs","owner_login":"fake-org",
            "private":True,"visibility":"private","archived":False,
            "disabled":False,"default_branch":"main",
        },
        config_schema_version=1, created_by_user_id=uuid4(),
        last_validated_at=NOW, created_at=NOW, updated_at=NOW, removed_at=removed_at,
    )


def test_prepare_copies_only_verified_tenant_boundary_identifiers():
    value, _, context, _ = _service()
    result = value.prepare(
        context.organization_id, context.connector_id, context.knowledge_space_id, 501
    )
    assert result == context
    assert "fake-org" not in repr(result) and "77" not in repr(result)
    value._connectors.get_by_id.assert_called_once_with(
        context.organization_id, context.connector_id
    )


@pytest.mark.parametrize(("target","error"),(
    ("connector",GitHubRepositorySelectionNotFound),
    ("wrong_type",GitHubRepositorySelectionRejected),
    ("inactive",GitHubRepositorySelectionConflict),
    ("credential",GitHubRepositorySelectionNotFound),
    ("installation",GitHubRepositorySelectionNotFound),
    ("personal",GitHubRepositorySelectionConflict),
))
def test_prepare_rejects_missing_or_invalid_boundaries(target,error):
    value, _, context, _ = _service()
    if target == "connector": value._connectors.get_by_id.return_value=None
    elif target == "wrong_type": value._connectors.get_by_id.return_value.connector_type="local_folder"
    elif target == "inactive": value._connectors.get_by_id.return_value.status="draft"
    elif target == "credential": value._credentials.get.return_value=None
    elif target == "installation": value._installations.get.return_value=None
    elif target == "personal":
        current=value._installations.get.return_value
        value._installations.get.return_value=SimpleNamespace(**{**current.__dict__,"account_type":"User"})
    with pytest.raises(error):
        value.prepare(context.organization_id,context.connector_id,context.knowledge_space_id,501)


def test_verify_uses_exact_installation_repository_and_one_metadata_token():
    value, client, context, _ = _service()
    token=GitHubInstallationAccessToken("ghs_temporary",NOW+timedelta(hours=1))
    repo=repository()
    client.create_repository_access_token.return_value=GitHubRepositoryAccessGrant(token,repo)
    client.list_installation_repositories.return_value=GitHubRepositoryPage((repo,),1,1,False,1)
    assert value.verify(context) is repo
    client.create_repository_access_token.assert_called_once_with(
        77,501,account_id=99,account_login="fake-org"
    )
    client.list_installation_repositories.assert_called_once_with(
        token,page=1,page_size=1,account_id=99,account_login="fake-org"
    )


@pytest.mark.parametrize("page",(
    GitHubRepositoryPage((),1,1,False,0),
    GitHubRepositoryPage((repository(502),),1,1,False,1),
    GitHubRepositoryPage((repository(),),1,1,True,2),
    GitHubRepositoryPage((repository(),),1,1,False,None),
))
def test_verify_rejects_inaccessible_or_disagreeing_repository(page):
    value, client, context, _ = _service()
    token=GitHubInstallationAccessToken("ghs_temporary",NOW+timedelta(hours=1))
    client.create_repository_access_token.return_value=GitHubRepositoryAccessGrant(token,repository())
    client.list_installation_repositories.return_value=page
    with pytest.raises(GitHubRepositorySelectionRejected): value.verify(context)
    client.create_repository_access_token.assert_called_once()


def test_persist_creates_canonical_allowlisted_scope_without_starting_sync():
    value, _, context, user_id = _service()
    value._scopes.lock_by_external_scope_key.return_value=None
    captured=[]
    def add(_,scope):
        scope.created_at=NOW;scope.updated_at=NOW;captured.append(scope);return scope
    value._scopes.add.side_effect=add
    result=value.persist(context,repository(),user_id)
    scope=captured[0]
    assert result.repository_id==501 and result.status=="active"
    assert scope.external_scope_key=="github:repository:501"
    assert scope.knowledge_space_id==context.knowledge_space_id
    assert set(scope.safe_config)=={
        "repository_id","repository_name","repository_full_name","owner_login",
        "private","visibility","archived","disabled","default_branch",
    }
    assert "token" not in repr(scope.safe_config).lower()


def test_exact_duplicate_is_idempotent_and_removed_same_space_reactivates():
    value, _, context, user_id = _service()
    scope=_scope(context,status="removed")
    value._scopes.lock_by_external_scope_key.return_value=scope
    result=value.persist(context,repository(),user_id)
    assert result.scope_id==scope.id and scope.status=="active" and scope.removed_at is None
    value._scopes.add.assert_not_called();value._scopes.flush.assert_called_once()


def test_existing_repository_in_another_space_never_moves():
    value, _, context, user_id = _service()
    scope=_scope(context,space_id=uuid4(),status="removed")
    value._scopes.lock_by_external_scope_key.return_value=scope
    with pytest.raises(GitHubRepositorySelectionConflict):
        value.persist(context,repository(),user_id)
    assert scope.knowledge_space_id != context.knowledge_space_id
    value._scopes.flush.assert_not_called()


def test_stale_installation_is_rejected_before_scope_write():
    value, _, context, user_id = _service()
    value._installations.lock.return_value.github_installation_id=78
    with pytest.raises(GitHubRepositorySelectionConflict):
        value.persist(context,repository(),user_id)
    value._scopes.lock_by_external_scope_key.assert_not_called()


def test_deselect_is_local_idempotent_and_never_uses_provider():
    value, client, context, _ = _service()
    scope=_scope(context)
    value._scopes.lock_by_id.return_value=scope
    first=value.deselect(context.organization_id,context.connector_id,scope.id)
    second=value.deselect(context.organization_id,context.connector_id,scope.id)
    assert first.status==second.status=="removed" and scope.removed_at==NOW
    assert value._scopes.flush.call_count==1
    client.create_repository_access_token.assert_not_called()
    client.list_installation_repositories.assert_not_called()


def test_list_is_bounded_deterministic_and_filters_repository_scopes_only():
    value, client, context, _ = _service()
    first=_scope(context);second=_scope(context,status="removed")
    value._scopes.list_page.return_value=ConnectorScopePage((first,second),2,False,None)
    page=value.list(context.organization_id,context.connector_id,limit=2)
    assert [item.scope_id for item in page.items]==[first.id,second.id]
    value._scopes.list_page.assert_called_once_with(
        context.organization_id,connector_id=context.connector_id,
        scope_type="repository",limit=2,cursor=None,
    )
    client.create_repository_access_token.assert_not_called()
