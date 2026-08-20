"""Bounded GitHub App REST client; generated credentials never leave this adapter."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import time
import re
from typing import Callable
from urllib.parse import urlencode

import httpx
import jwt

from app.config import GitHubAppSettings
from application.ports.github_app import (
    GitHubAppClient, GitHubInstallation, GitHubProviderAuthenticationError,
    GitHubProviderAuthorizationError, GitHubProviderNotFoundError,
    GitHubProviderRateLimitError, GitHubProviderUnavailableError, GitHubUser,
    GitHubUserAccessToken,
)
from application.ports.secret_store import SecretStore

MAX_RESPONSE_BYTES = 1_048_576
MAX_INSTALLATION_PAGES = 10
INSTALLATIONS_PER_PAGE = 100


class GitHubAppRestClient(GitHubAppClient):
    def __init__(self, settings: GitHubAppSettings, secrets: SecretStore, *,
        http_client: httpx.Client | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic) -> None:
        self._settings=settings;self._secrets=secrets;self._clock=clock
        self._sleep=sleeper;self._monotonic=monotonic
        timeout=httpx.Timeout(settings.request_timeout_seconds,connect=min(5.0,settings.request_timeout_seconds))
        self._http=http_client or httpx.Client(timeout=timeout,follow_redirects=False)

    @property
    def app_id(self)->int:return self._settings.app_id

    @property
    def web_base_url(self)->str:return self._settings.web_base_url

    @property
    def client_id(self)->str:return self._settings.client_id

    @property
    def callback_url(self)->str:return self._settings.callback_url

    def build_installation_url(self,state:str)->str:
        _opaque(state,"GitHub installation state",43,512)
        return f"{self._settings.web_base_url.rstrip('/')}/apps/{self._settings.app_slug}/installations/new?{urlencode({'state':state})}"

    def build_authorization_url(self,state:str,pkce_challenge:str)->str:
        _opaque(state,"GitHub authorization state",43,512);_opaque(pkce_challenge,"GitHub PKCE challenge",43,128)
        query=urlencode({"client_id":self._settings.client_id,"redirect_uri":self._settings.callback_url,
            "state":state,"code_challenge":pkce_challenge,"code_challenge_method":"S256"})
        return f"{self._settings.web_base_url.rstrip('/')}/login/oauth/authorize?{query}"

    def exchange_authorization_code(self,code:str,pkce_verifier:str)->GitHubUserAccessToken:
        _opaque(code,"GitHub authorization code",1,1024);_opaque(pkce_verifier,"GitHub PKCE verifier",43,512)
        client_secret=self._secrets.retrieve(self._settings.client_secret_reference).value
        response=self._request("POST","/login/oauth/access_token",base_url=self._settings.web_base_url,
            authorization=None,retry=False,data={"client_id":self._settings.client_id,
            "client_secret":client_secret,"code":code,"redirect_uri":self._settings.callback_url,
            "code_verifier":pkce_verifier})
        body=_json_object(response);token=body.get("access_token");token_type=body.get("token_type")
        if not isinstance(token,str) or not token or not isinstance(token_type,str) or token_type.lower()!="bearer":
            raise GitHubProviderAuthenticationError()
        return GitHubUserAccessToken(token)

    def get_authenticated_user(self,token:GitHubUserAccessToken)->GitHubUser:
        body=_json_object(self._user_request("/user",token))
        try:return GitHubUser(_positive_int(body["id"]),_nonblank(body["login"],255))
        except (KeyError,TypeError,ValueError) as exc:raise GitHubProviderUnavailableError() from exc

    def list_user_installations(self,token:GitHubUserAccessToken)->tuple[GitHubInstallation,...]:
        results:list[GitHubInstallation]=[]
        for page in range(1,MAX_INSTALLATION_PAGES+1):
            body=_json_object(self._user_request(f"/user/installations?{urlencode({'per_page':INSTALLATIONS_PER_PAGE,'page':page})}",token))
            raw_items=body.get("installations")
            if not isinstance(raw_items,list) or len(raw_items)>INSTALLATIONS_PER_PAGE:raise GitHubProviderUnavailableError()
            results.extend(_installation(item) for item in raw_items)
            if len(results)>MAX_INSTALLATION_PAGES*INSTALLATIONS_PER_PAGE:raise GitHubProviderUnavailableError()
            if len(raw_items)<INSTALLATIONS_PER_PAGE:return tuple(results)
        raise GitHubProviderUnavailableError()

    def verify_installation(self,installation_id:int)->GitHubInstallation:
        installation_id=_positive_int(installation_id)
        result=_installation(_json_object(self._app_request(f"/app/installations/{installation_id}")))
        if result.installation_id!=installation_id or result.app_id!=self._settings.app_id:raise GitHubProviderAuthorizationError()
        return result

    def _user_request(self,path:str,token:GitHubUserAccessToken)->httpx.Response:
        if not isinstance(token,GitHubUserAccessToken):raise GitHubProviderAuthenticationError()
        return self._request("GET",path,authorization=f"Bearer {token.value}",retry=True)

    def _app_request(self,path:str)->httpx.Response:
        return self._request("GET",path,authorization=f"Bearer {self._app_jwt()}",retry=True)

    def _app_jwt(self)->str:
        now=self._clock()
        if now.tzinfo is None or now.utcoffset() is None:raise GitHubProviderUnavailableError()
        key=self._secrets.retrieve(self._settings.private_key_reference).value
        try:return jwt.encode({"iat":int((now-timedelta(seconds=60)).timestamp()),
            "exp":int((now+timedelta(minutes=9)).timestamp()),"iss":self._settings.client_id},key,algorithm="RS256")
        except Exception as exc:raise GitHubProviderAuthenticationError() from exc

    def _request(self,method:str,path:str,*,authorization:str|None,retry:bool,
        base_url:str|None=None,data:dict[str,str]|None=None)->httpx.Response:
        deadline=self._monotonic()+self._settings.request_timeout_seconds
        attempts=self._settings.max_retries+1 if retry and method=="GET" else 1
        headers={"Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28","User-Agent":"enterprise-ai-platform"}
        if authorization is not None:headers["Authorization"]=authorization
        for attempt in range(attempts):
            remaining=deadline-self._monotonic()
            if remaining<=0:raise GitHubProviderUnavailableError()
            try:response=self._http.request(method,f"{(base_url or self._settings.api_base_url).rstrip('/')}{path}",
                headers=headers,data=data,timeout=httpx.Timeout(remaining,connect=min(5.0,remaining)),follow_redirects=False)
            except (httpx.TimeoutException,httpx.NetworkError) as exc:
                if attempt+1>=attempts:raise GitHubProviderUnavailableError() from exc
                self._bounded_sleep(min(2**attempt,4),deadline);continue
            _bounded_response(response)
            rate_limited=response.status_code in {403,429} and (response.headers.get("X-RateLimit-Remaining")=="0" or "Retry-After" in response.headers)
            retryable=response.status_code in {429,502,503,504} or rate_limited
            if retryable and attempt+1<attempts:
                self._bounded_sleep(_retry_delay(response,attempt,self._clock()),deadline);continue
            if response.status_code==401:raise GitHubProviderAuthenticationError()
            if rate_limited or response.status_code==429:raise GitHubProviderRateLimitError()
            if response.status_code==403:raise GitHubProviderAuthorizationError()
            if response.status_code==404:raise GitHubProviderNotFoundError()
            if response.status_code>=500:raise GitHubProviderUnavailableError()
            if response.status_code!=200:raise GitHubProviderAuthorizationError()
            return response
        raise GitHubProviderUnavailableError()

    def _bounded_sleep(self,delay:float,deadline:float)->None:
        remaining=deadline-self._monotonic()
        if remaining<=0:raise GitHubProviderUnavailableError()
        self._sleep(min(delay,remaining))


def _installation(value:object)->GitHubInstallation:
    if not isinstance(value,dict):raise GitHubProviderUnavailableError()
    try:
        account=value["account"]
        if not isinstance(account,dict):raise TypeError
        result=GitHubInstallation(_positive_int(value["id"]),_positive_int(value["app_id"]),
            _positive_int(account["id"]),_nonblank(account["login"],255),_nonblank(account["type"],32),
            _nonblank(value["repository_selection"],16),_permissions(value["permissions"]),
            _timestamp(value["created_at"]),_timestamp(value["updated_at"]))
    except (KeyError,TypeError,ValueError) as exc:raise GitHubProviderUnavailableError() from exc
    if result.account_type not in {"Organization","User"} or result.repository_selection not in {"all","selected"}:
        raise GitHubProviderAuthorizationError()
    return result

def _json_object(response:httpx.Response)->dict[str,object]:
    try:value=response.json()
    except (ValueError,UnicodeError) as exc:raise GitHubProviderUnavailableError() from exc
    if not isinstance(value,dict):raise GitHubProviderUnavailableError()
    return value

def _permissions(value:object)->tuple[tuple[str,str],...]:
    if not isinstance(value,dict) or not 1<=len(value)<=100:raise ValueError
    result=[]
    for name,level in value.items():
        if (not isinstance(name,str) or not re.fullmatch(r"[a-z][a-z0-9_]*",name)
            or level not in {"read","write"}):raise ValueError
        result.append((name,level))
    return tuple(sorted(result))

def _bounded_response(response:httpx.Response)->None:
    length=response.headers.get("Content-Length")
    try:
        if length is not None and int(length)>MAX_RESPONSE_BYTES:raise GitHubProviderUnavailableError()
    except ValueError as exc:raise GitHubProviderUnavailableError() from exc
    if len(response.content)>MAX_RESPONSE_BYTES:raise GitHubProviderUnavailableError()

def _positive_int(value:object)->int:
    if isinstance(value,bool) or not isinstance(value,int) or value<1:raise ValueError
    return value

def _nonblank(value:object,maximum:int)->str:
    if not isinstance(value,str) or not value.strip() or len(value)>maximum:raise ValueError
    return value

def _opaque(value:object,name:str,minimum:int,maximum:int)->str:
    if not isinstance(value,str) or not minimum<=len(value)<=maximum or any(ch.isspace() for ch in value):raise ValueError(f"{name} is invalid")
    return value

def _timestamp(value:object)->datetime:
    if not isinstance(value,str):raise ValueError
    result=datetime.fromisoformat(value.replace("Z","+00:00"))
    if result.tzinfo is None:raise ValueError
    return result

def _retry_delay(response:httpx.Response,attempt:int,now:datetime)->float:
    value=response.headers.get("Retry-After")
    if value:
        try:return min(max(float(value),0),30)
        except ValueError:
            try:return min(max((parsedate_to_datetime(value)-now).total_seconds(),0),30)
            except (TypeError,ValueError):pass
    reset=response.headers.get("X-RateLimit-Reset")
    if reset:
        try:return min(max(float(reset)-now.timestamp(),0),30)
        except ValueError:pass
    return min(2**attempt,4)
