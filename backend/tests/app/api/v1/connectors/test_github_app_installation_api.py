from datetime import datetime,timezone
from unittest.mock import Mock
from uuid import uuid4
import pytest
from fastapi import HTTPException,Request
from fastapi.testclient import TestClient
from app.config import GitHubAppSettings
from app.dependencies import (ConnectorAdministrator,get_connector_administrator,get_db_session,
    get_github_app_installation_service,get_github_app_settings,get_secret_store)
from app.main import app,configure_github_app
from application.ports.secret_store import SecretReference,SecretValue
from application.services.github_app_installation_service import GitHubInstallationInitiation,GitHubInstallationStatus

NOW=datetime(2026,8,20,12,tzinfo=timezone.utc)
class Session:
    def __init__(self):self.commits=0;self.rollbacks=0
    def commit(self):self.commits+=1
    def rollback(self):self.rollbacks+=1
def setup(service=None,admin=None):
    service=service or Mock();session=Session();admin=admin or ConnectorAdministrator(uuid4(),uuid4())
    def db():yield session
    app.dependency_overrides[get_db_session]=db;app.dependency_overrides[get_connector_administrator]=lambda:admin
    app.dependency_overrides[get_github_app_installation_service]=lambda:service
    return TestClient(app),service,session,admin
@pytest.fixture(autouse=True)
def clear():
    yield;app.dependency_overrides.clear()
    for name in ("secret_store","github_app_settings"):
        if hasattr(app.state,name):delattr(app.state,name)
def status(connected=True):return GitHubInstallationStatus(connected,"fake-org" if connected else None,"Organization" if connected else None,"99" if connected else None,"selected" if connected else None,"active" if connected else "revoked",NOW if connected else None,NOW if connected else None,NOW if connected else None)
def test_admin_can_initiate_complete_read_and_disconnect_with_redacted_contracts():
    client,service,session,admin=setup();connector=uuid4()
    service.initiate.return_value=GitHubInstallationInitiation("https://github.com/apps/fake/installations/new?state=opaque","https://github.com/login/oauth/authorize?state=opaque",NOW)
    response=client.post(f"/api/v1/connectors/{connector}/github/installation")
    assert response.status_code==200 and set(response.json())=={"installation_url","authorization_url","expires_at"}
    service.complete.return_value=status();response=client.post(f"/api/v1/connectors/{connector}/github/installation/complete",json={"state":"s"*64,"code":"temporary-code","installation_id":77})
    assert response.status_code==200 and response.json()["external_account_id"]=="99"
    rendered=response.text.lower();assert not any(x in rendered for x in ("token","private_key","secret_reference","oauth_state","installation_id"))
    service.status.return_value=status();assert client.get(f"/api/v1/connectors/{connector}/github/installation").status_code==200
    service.disconnect.return_value=status(False);assert client.delete(f"/api/v1/connectors/{connector}/github/installation").status_code==200
    assert session.commits==3
def test_non_admin_is_rejected_before_service_call():
    def forbidden():raise HTTPException(status_code=403,detail="Organization administrator role is required")
    client,service,_,_=setup();app.dependency_overrides[get_connector_administrator]=forbidden
    response=client.post(f"/api/v1/connectors/{uuid4()}/github/installation")
    assert response.status_code==403;service.initiate.assert_not_called()
def test_callback_schema_and_provider_errors_are_fixed_and_rollback():
    client,service,session,_=setup();connector=uuid4()
    response=client.post(f"/api/v1/connectors/{connector}/github/installation/complete",json={"state":"short","installation_id":0,"token":"must-not-be-accepted"})
    assert response.status_code==422 and response.json()["detail"]=="Connector request is invalid"
    service.complete.side_effect=RuntimeError("FAKE token private key provider payload")
    response=client.post(f"/api/v1/connectors/{connector}/github/installation/complete",json={"state":"s"*64,"code":"temporary-code","installation_id":77})
    assert response.status_code==500 and response.json()=={"detail":"Internal server error"} and "FAKE" not in response.text
    assert session.rollbacks==1

def test_runtime_composition_requires_and_accepts_injected_secret_store():
    request=Request({"type":"http","app":app,"headers":[],"method":"GET","path":"/","query_string":b"","server":("test",443),"client":("test",1),"scheme":"https"})
    with pytest.raises(HTTPException) as missing:get_secret_store(request)
    assert missing.value.status_code==503
    class Store:
        def store(self,value):return SecretReference("opaque")
        def retrieve(self,reference):return SecretValue("hidden")
        def delete(self,reference):pass
    settings=GitHubAppSettings(123,"app-slug","Iv1.client-id",SecretReference("fake://opaque-client"),
        SecretReference("fake://opaque-key"),"https://platform.test/callback","https://platform.test/setup")
    store=Store();configure_github_app(app,store,settings=settings)
    assert get_secret_store(request) is store and get_github_app_settings(request) is settings
