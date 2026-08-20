from datetime import datetime,timezone
from unittest.mock import Mock
from urllib.parse import parse_qs

import httpx,jwt,pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.config import GitHubAppSettings
from application.ports.github_app import (GitHubProviderAuthenticationError,
    GitHubProviderAuthorizationError,GitHubProviderUnavailableError,GitHubUserAccessToken)
from application.ports.secret_store import SecretReference,SecretValue
from infrastructure.connectors.github.client import GitHubAppRestClient,MAX_RESPONSE_BYTES

NOW=datetime(2026,8,20,12,tzinfo=timezone.utc)

class Store:
    def __init__(self):
        self.values={"fake://github-app-key":"FAKE TEST RSA PRIVATE KEY","fake://github-client-secret":"client-secret"}
        self.retrieved=[]
    def retrieve(self,ref):self.retrieved.append(ref.value);return SecretValue(self.values[ref.value])

def settings(**kw):
    values=dict(app_id=12345,app_slug="enterprise-ai-test",client_id="Iv1.client-id",
        client_secret_reference=SecretReference("fake://github-client-secret"),
        private_key_reference=SecretReference("fake://github-app-key"),
        callback_url="https://platform.example.test/api/v1/connectors/github/callback",
        setup_url="https://platform.example.test/api/v1/connectors/github/setup")
    values.update(kw);return GitHubAppSettings(**values)

def payload(identifier=77,**kw):
    value=dict(id=identifier,app_id=12345,account={"id":99,"login":"fake-org","type":"Organization"},
        repository_selection="selected",permissions={"contents":"read","metadata":"read"},
        created_at="2026-08-20T10:00:00Z",updated_at="2026-08-20T11:00:00Z")
    value.update(kw);return value

def response(status=200,**kw):return httpx.Response(status,json=payload(**kw))

def client(handler,**kw):
    store=Store();http=httpx.Client(transport=httpx.MockTransport(handler))
    return GitHubAppRestClient(settings(**kw),store,http_client=http,clock=lambda:NOW,sleeper=Mock()),store

def test_configuration_and_urls_are_strict_safe_and_use_pkce():
    with pytest.raises(ValueError):settings(api_base_url="http://api.github.com")
    with pytest.raises(ValueError):settings(callback_url="https://platform.test/api/v1/connectors/github/callback?wild=1")
    with pytest.raises(ValueError):settings(setup_url="https://platform.example.test/wrong-setup")
    with pytest.raises(ValueError):settings(web_base_url="https://github.com/untrusted-path")
    with pytest.raises(ValueError):settings(client_id="12345")
    with pytest.raises(ValueError):settings(client_secret_reference=SecretReference("plaintext-secret"))
    value,_=client(lambda request:response())
    assert value.build_installation_url("s"*64).endswith("state="+"s"*64)
    url=value.build_authorization_url("s"*64,"c"*43)
    assert "client_secret" not in url and "code_challenge_method=S256" in url and "redirect_uri=" in url

def test_code_exchange_is_single_attempt_and_client_secret_comes_only_from_store():
    calls=[]
    def handler(request):
        calls.append(request);return httpx.Response(503 if len(calls)==1 else 200,json={"access_token":"ghu_hidden","token_type":"bearer"})
    value,store=client(handler,max_retries=3)
    with pytest.raises(GitHubProviderUnavailableError):value.exchange_authorization_code("code-value","v"*64)
    assert len(calls)==1 and store.retrieved==["fake://github-client-secret"]
    assert "client-secret" not in repr(value) and "code-value" not in repr(value)

def test_user_lookup_and_installations_use_only_temporary_user_token_with_pagination():
    calls=[]
    def handler(request):
        calls.append(request)
        assert request.headers["Authorization"]=="Bearer ghu_hidden"
        if request.url.path=="/user":return httpx.Response(200,json={"id":42,"login":"installer"})
        page=int(parse_qs(request.url.query.decode())["page"][0])
        items=[payload(i+1) for i in range(100)] if page==1 else [payload(777)]
        return httpx.Response(200,json={"total_count":101,"installations":items})
    value,_=client(handler);token=GitHubUserAccessToken("ghu_hidden")
    assert value.get_authenticated_user(token).user_id==42
    installations=value.list_user_installations(token)
    assert installations[-1].installation_id==777 and len(calls)==3

def test_jwt_claims_are_bounded_and_private_key_comes_from_store(monkeypatch):
    captured={}
    def encode(claims,key,algorithm):captured.update(claims=claims,key=key,algorithm=algorithm);return "fake.jwt"
    monkeypatch.setattr(jwt,"encode",encode);value,store=client(lambda request:response())
    assert value.verify_installation(77).account_id==99
    assert captured["algorithm"]=="RS256" and captured["claims"]["iss"]=="Iv1.client-id"
    assert 0<captured["claims"]["exp"]-captured["claims"]["iat"]<=600
    assert store.retrieved==["fake://github-app-key"]

def test_real_rs256_app_jwt_is_ephemeral():
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    pem=key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()).decode()
    store=Store();store.values["fake://github-app-key"]=pem
    value=GitHubAppRestClient(settings(),store,http_client=httpx.Client(transport=httpx.MockTransport(lambda r:response())),clock=lambda:NOW)
    token=value._app_jwt();claims=jwt.decode(token,key.public_key(),algorithms=["RS256"],options={"verify_exp":False,"verify_iat":False})
    assert claims["iss"]=="Iv1.client-id" and token not in repr(value)

def test_wrong_app_or_spoofed_installation_is_rejected(monkeypatch):
    monkeypatch.setattr(jwt,"encode",lambda *a,**k:"fake.jwt")
    value,_=client(lambda request:response(id=78))
    with pytest.raises(GitHubProviderAuthorizationError):value.verify_installation(77)
    value,_=client(lambda request:response(app_id=999))
    with pytest.raises(GitHubProviderAuthorizationError):value.verify_installation(77)

def test_retries_rate_limits_timeouts_and_response_size_are_bounded(monkeypatch):
    monkeypatch.setattr(jwt,"encode",lambda *a,**k:"fake.jwt");calls=[]
    value,_=client(lambda request:(calls.append(1) or httpx.Response(503)),max_retries=2);value._sleep=Mock()
    with pytest.raises(GitHubProviderUnavailableError):value.verify_installation(77)
    assert len(calls)==3 and value._sleep.call_count==2
    calls.clear();value,_=client(lambda request:(calls.append(1) or httpx.Response(401)),max_retries=3)
    with pytest.raises(GitHubProviderAuthenticationError):value.verify_installation(77)
    assert len(calls)==1
    value,_=client(lambda request:httpx.Response(200,content=b"x"*(MAX_RESPONSE_BYTES+1)),max_retries=0)
    with pytest.raises(GitHubProviderUnavailableError):value.verify_installation(77)
    calls=[];value,_=client(lambda request:(calls.append(1) or httpx.Response(403,headers={"X-RateLimit-Remaining":"0","Retry-After":"0"})),max_retries=1)
    value._sleep=Mock()
    with pytest.raises(Exception,match="GitHub provider request failed"):value.verify_installation(77)
    assert len(calls)==2 and value._sleep.call_count==1

def test_installation_pagination_has_a_hard_page_and_item_bound():
    calls=[]
    def handler(request):
        calls.append(1);return httpx.Response(200,json={"total_count":2000,"installations":[payload(i+1) for i in range(100)]})
    value,_=client(handler)
    with pytest.raises(GitHubProviderUnavailableError):value.list_user_installations(GitHubUserAccessToken("ghu_hidden"))
    assert len(calls)==10

def test_provider_errors_are_redacted(monkeypatch):
    monkeypatch.setattr(jwt,"encode",lambda *a,**k:"fake.jwt")
    def timeout(request):raise httpx.ReadTimeout("FAKE code token secret",request=request)
    value,_=client(timeout,max_retries=0)
    with pytest.raises(GitHubProviderUnavailableError) as caught:value.verify_installation(77)
    assert "FAKE" not in str(caught.value)
