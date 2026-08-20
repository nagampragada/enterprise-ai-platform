"""Bounded GitHub App REST client; generated credentials never leave this adapter."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import random
import re
import time
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
import jwt

from app.config import GitHubAppSettings
from application.ports.github_app import (
    GitHubAppClient,
    GitHubInstallation,
    GitHubInstallationAccessToken,
    GitHubProviderAuthenticationError,
    GitHubProviderAuthorizationError,
    GitHubProviderNotFoundError,
    GitHubProviderRateLimitError,
    GitHubProviderUnavailableError,
    GitHubRepository,
    GitHubRepositoryPage,
    GitHubUser,
    GitHubUserAccessToken,
)
from application.ports.secret_store import SecretStore


GITHUB_API_VERSION = "2022-11-28"
MAX_RESPONSE_BYTES = 1_048_576
MAX_INSTALLATION_PAGES = 10
INSTALLATIONS_PER_PAGE = 100
MAX_REPOSITORY_PAGE = 1_000
MAX_REPOSITORY_PAGE_SIZE = 100
MAX_REPOSITORY_TOTAL_COUNT = 1_000_000
MAX_LINK_HEADER_BYTES = 8_192
MIN_INSTALLATION_TOKEN_LIFETIME = timedelta(seconds=30)
MAX_INSTALLATION_TOKEN_LIFETIME = timedelta(minutes=65)
INSTALLATION_TOKEN_PERMISSIONS = {"metadata": "read"}
_REPOSITORY_NAME = re.compile(r"[A-Za-z0-9_.-]{1,100}")
_ACCOUNT_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?")
_DEFAULT_BRANCH = re.compile(r"[A-Za-z0-9._/-]{1,255}")
_LINK_PART = re.compile(r'\s*<([^<>]+)>\s*;\s*rel="(next|prev|first|last)"\s*')
_REPOSITORY_VISIBILITIES = frozenset({"public", "private", "internal"})


class GitHubAppRestClient(GitHubAppClient):
    def __init__(
        self,
        settings: GitHubAppSettings,
        secrets: SecretStore,
        *,
        http_client: httpx.Client | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[float, float], float] = random.SystemRandom().uniform,
    ) -> None:
        self._settings = settings
        self._secrets = secrets
        self._clock = clock
        self._sleep = sleeper
        self._monotonic = monotonic
        self._jitter = jitter
        timeout = httpx.Timeout(
            settings.request_timeout_seconds,
            connect=min(5.0, settings.request_timeout_seconds),
        )
        self._http = http_client or httpx.Client(timeout=timeout, follow_redirects=False)

    @property
    def app_id(self) -> int:
        return self._settings.app_id

    @property
    def web_base_url(self) -> str:
        return self._settings.web_base_url

    @property
    def client_id(self) -> str:
        return self._settings.client_id

    @property
    def callback_url(self) -> str:
        return self._settings.callback_url

    def build_installation_url(self, state: str) -> str:
        _opaque(state, "GitHub installation state", 43, 512)
        return (
            f"{self._settings.web_base_url.rstrip('/')}/apps/"
            f"{self._settings.app_slug}/installations/new?{urlencode({'state': state})}"
        )

    def build_authorization_url(self, state: str, pkce_challenge: str) -> str:
        _opaque(state, "GitHub authorization state", 43, 512)
        _opaque(pkce_challenge, "GitHub PKCE challenge", 43, 128)
        query = urlencode(
            {
                "client_id": self._settings.client_id,
                "redirect_uri": self._settings.callback_url,
                "state": state,
                "code_challenge": pkce_challenge,
                "code_challenge_method": "S256",
            }
        )
        return f"{self._settings.web_base_url.rstrip('/')}/login/oauth/authorize?{query}"

    def exchange_authorization_code(
        self, code: str, pkce_verifier: str
    ) -> GitHubUserAccessToken:
        _opaque(code, "GitHub authorization code", 1, 1024)
        _opaque(pkce_verifier, "GitHub PKCE verifier", 43, 512)
        client_secret = self._secrets.retrieve(
            self._settings.client_secret_reference
        ).value
        response = self._request(
            "POST",
            "/login/oauth/access_token",
            base_url=self._settings.web_base_url,
            authorization=None,
            retry=False,
            data={
                "client_id": self._settings.client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": self._settings.callback_url,
                "code_verifier": pkce_verifier,
            },
        )
        body = _json_object(response)
        token = body.get("access_token")
        token_type = body.get("token_type")
        if (
            not isinstance(token, str)
            or not token
            or not isinstance(token_type, str)
            or token_type.lower() != "bearer"
        ):
            raise GitHubProviderAuthenticationError()
        return GitHubUserAccessToken(token)

    def get_authenticated_user(self, token: GitHubUserAccessToken) -> GitHubUser:
        body = _json_object(self._user_request("/user", token))
        try:
            return GitHubUser(_positive_int(body["id"]), _nonblank(body["login"], 255))
        except (KeyError, TypeError, ValueError) as exc:
            raise GitHubProviderUnavailableError() from exc

    def list_user_installations(
        self, token: GitHubUserAccessToken
    ) -> tuple[GitHubInstallation, ...]:
        results: list[GitHubInstallation] = []
        for page in range(1, MAX_INSTALLATION_PAGES + 1):
            path = "/user/installations?" + urlencode(
                {"per_page": INSTALLATIONS_PER_PAGE, "page": page}
            )
            body = _json_object(self._user_request(path, token))
            raw_items = body.get("installations")
            if not isinstance(raw_items, list) or len(raw_items) > INSTALLATIONS_PER_PAGE:
                raise GitHubProviderUnavailableError()
            results.extend(_installation(item) for item in raw_items)
            if len(results) > MAX_INSTALLATION_PAGES * INSTALLATIONS_PER_PAGE:
                raise GitHubProviderUnavailableError()
            if len(raw_items) < INSTALLATIONS_PER_PAGE:
                return tuple(results)
        raise GitHubProviderUnavailableError()

    def verify_installation(self, installation_id: int) -> GitHubInstallation:
        installation_id = _positive_int(installation_id)
        result = _installation(
            _json_object(
                self._app_request(f"/app/installations/{installation_id}")
            )
        )
        if result.installation_id != installation_id or result.app_id != self._settings.app_id:
            raise GitHubProviderAuthorizationError()
        return result

    def create_installation_access_token(
        self, installation_id: int
    ) -> GitHubInstallationAccessToken:
        installation_id = _positive_int(installation_id)
        response = self._request(
            "POST",
            f"/app/installations/{installation_id}/access_tokens",
            authorization=f"Bearer {self._app_jwt()}",
            retry=False,
            json_data={"permissions": INSTALLATION_TOKEN_PERMISSIONS},
            expected_statuses=(201,),
        )
        body = _json_object(response)
        try:
            token = GitHubInstallationAccessToken(
                _opaque(body["token"], "GitHub installation token", 1, 8192),
                _timestamp(body["expires_at"]),
            )
            permissions = _permissions(body["permissions"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GitHubProviderUnavailableError() from exc
        now = _aware_clock(self._clock())
        lifetime = token.expires_at - now
        if (
            not MIN_INSTALLATION_TOKEN_LIFETIME <= lifetime <= MAX_INSTALLATION_TOKEN_LIFETIME
            or permissions != tuple(sorted(INSTALLATION_TOKEN_PERMISSIONS.items()))
        ):
            raise GitHubProviderUnavailableError()
        return token

    def list_installation_repositories(
        self,
        token: GitHubInstallationAccessToken,
        *,
        page: int,
        page_size: int,
        account_id: int,
        account_login: str,
    ) -> GitHubRepositoryPage:
        if not isinstance(token, GitHubInstallationAccessToken):
            raise GitHubProviderAuthenticationError()
        if token.expires_at <= _aware_clock(self._clock()):
            raise GitHubProviderAuthenticationError()
        page = _bounded_integer("page", page, 1, MAX_REPOSITORY_PAGE)
        page_size = _bounded_integer(
            "page_size", page_size, 1, MAX_REPOSITORY_PAGE_SIZE
        )
        account_id = _positive_int(account_id)
        if not isinstance(account_login, str) or _ACCOUNT_LOGIN.fullmatch(account_login) is None:
            raise GitHubProviderAuthorizationError()

        path = "/installation/repositories?" + urlencode(
            {"per_page": page_size, "page": page}
        )
        response = self._request(
            "GET",
            path,
            authorization=f"Bearer {token.value}",
            retry=True,
        )
        body = _json_object(response)
        raw_items = body.get("repositories")
        if not isinstance(raw_items, list) or len(raw_items) > page_size:
            raise GitHubProviderUnavailableError()
        items = tuple(
            _repository(
                item,
                account_id=account_id,
                account_login=account_login,
                web_base_url=self._settings.web_base_url,
            )
            for item in raw_items
        )
        identifiers = tuple(item.repository_id for item in items)
        if len(set(identifiers)) != len(identifiers):
            raise GitHubProviderUnavailableError()
        total_count = _total_count(body.get("total_count"), page, page_size, len(items))
        link_has_next = _link_has_next(
            response.headers.get("Link"),
            api_base_url=self._settings.api_base_url,
            page=page,
            page_size=page_size,
        )
        offset_end = (page - 1) * page_size + len(items)
        total_has_next = total_count is not None and offset_end < total_count
        if link_has_next is not None and total_count is not None:
            if link_has_next != total_has_next:
                raise GitHubProviderUnavailableError()
        has_next = (
            link_has_next
            if link_has_next is not None
            else total_has_next if total_count is not None else len(items) == page_size
        )
        return GitHubRepositoryPage(items, page, page_size, has_next, total_count)

    def _user_request(self, path: str, token: GitHubUserAccessToken) -> httpx.Response:
        if not isinstance(token, GitHubUserAccessToken):
            raise GitHubProviderAuthenticationError()
        return self._request(
            "GET", path, authorization=f"Bearer {token.value}", retry=True
        )

    def _app_request(self, path: str) -> httpx.Response:
        return self._request(
            "GET", path, authorization=f"Bearer {self._app_jwt()}", retry=True
        )

    def _app_jwt(self) -> str:
        now = _aware_clock(self._clock())
        key = self._secrets.retrieve(self._settings.private_key_reference).value
        try:
            token = jwt.encode(
                {
                    "iat": int((now - timedelta(seconds=60)).timestamp()),
                    "exp": int((now + timedelta(minutes=9)).timestamp()),
                    "iss": self._settings.client_id,
                },
                key,
                algorithm="RS256",
            )
            return _opaque(token, "GitHub App JWT", 1, 8192)
        except Exception as exc:
            raise GitHubProviderAuthenticationError() from exc

    def _request(
        self,
        method: str,
        path: str,
        *,
        authorization: str | None,
        retry: bool,
        base_url: str | None = None,
        data: dict[str, str] | None = None,
        json_data: dict[str, object] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        deadline = self._monotonic() + self._settings.request_timeout_seconds
        attempts = min(self._settings.max_retries + 1, 3) if retry and method == "GET" else 1
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "User-Agent": "enterprise-ai-platform",
        }
        if authorization is not None:
            headers["Authorization"] = authorization
        url = f"{(base_url or self._settings.api_base_url).rstrip('/')}{path}"
        for attempt in range(attempts):
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise GitHubProviderUnavailableError()
            request_arguments: dict[str, object] = {
                "headers": headers,
                "timeout": httpx.Timeout(remaining, connect=min(5.0, remaining)),
                "follow_redirects": False,
            }
            if data is not None:
                request_arguments["data"] = data
            if json_data is not None:
                request_arguments["json"] = json_data
            try:
                response = self._http.request(method, url, **request_arguments)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt + 1 >= attempts:
                    raise GitHubProviderUnavailableError() from exc
                self._bounded_sleep(min(2**attempt, 4), deadline, required=False)
                continue
            _bounded_response(response)
            rate_limited = response.status_code == 429 or (
                response.status_code == 403
                and (
                    response.headers.get("X-RateLimit-Remaining") == "0"
                    or "Retry-After" in response.headers
                )
            )
            if rate_limited:
                delay = _rate_limit_delay(response, self._clock())
                if attempt + 1 < attempts and delay is not None:
                    self._bounded_sleep(delay, deadline, required=True)
                    continue
                raise GitHubProviderRateLimitError()
            if response.status_code in {502, 503, 504} and attempt + 1 < attempts:
                self._bounded_sleep(min(2**attempt, 4), deadline, required=False)
                continue
            if response.status_code == 401:
                raise GitHubProviderAuthenticationError()
            if response.status_code == 403:
                raise GitHubProviderAuthorizationError()
            if response.status_code == 404:
                raise GitHubProviderNotFoundError()
            if response.status_code >= 500:
                raise GitHubProviderUnavailableError()
            if response.status_code not in expected_statuses:
                if 400 <= response.status_code < 500:
                    raise GitHubProviderAuthorizationError()
                raise GitHubProviderUnavailableError()
            return response
        raise GitHubProviderUnavailableError()

    def _bounded_sleep(self, delay: float, deadline: float, *, required: bool) -> None:
        remaining = deadline - self._monotonic()
        if remaining <= 0 or delay < 0 or delay >= remaining:
            if required:
                raise GitHubProviderRateLimitError()
            raise GitHubProviderUnavailableError()
        actual = delay if required else self._jitter(0.0, min(delay, remaining))
        self._sleep(actual)


def _installation(value: object) -> GitHubInstallation:
    if not isinstance(value, dict):
        raise GitHubProviderUnavailableError()
    try:
        account = value["account"]
        if not isinstance(account, dict):
            raise TypeError
        result = GitHubInstallation(
            _positive_int(value["id"]),
            _positive_int(value["app_id"]),
            _positive_int(account["id"]),
            _nonblank(account["login"], 255),
            _nonblank(account["type"], 32),
            _nonblank(value["repository_selection"], 16),
            _permissions(value["permissions"]),
            _timestamp(value["created_at"]),
            _timestamp(value["updated_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GitHubProviderUnavailableError() from exc
    if (
        result.account_type not in {"Organization", "User"}
        or result.repository_selection not in {"all", "selected"}
    ):
        raise GitHubProviderAuthorizationError()
    return result


def _repository(
    value: object,
    *,
    account_id: int,
    account_login: str,
    web_base_url: str,
) -> GitHubRepository:
    if not isinstance(value, dict):
        raise GitHubProviderUnavailableError()
    try:
        owner = value["owner"]
        if not isinstance(owner, dict):
            raise TypeError
        owner_id = _positive_int(owner["id"])
        owner_login = _repository_component(owner["login"], _ACCOUNT_LOGIN)
        name = _repository_component(value["name"], _REPOSITORY_NAME)
        full_name = _nonblank(value["full_name"], 255)
        private = _boolean(value["private"])
        archived = _boolean(value["archived"])
        disabled = _boolean(value["disabled"])
        visibility = value.get("visibility")
        if visibility is not None and visibility not in _REPOSITORY_VISIBILITIES:
            raise ValueError
        default_branch = _default_branch(value.get("default_branch"))
        html_url = _repository_html_url(
            value["html_url"], owner_login, name, web_base_url
        )
        updated_at = (
            _timestamp(value["updated_at"]) if value.get("updated_at") is not None else None
        )
        result = GitHubRepository(
            _positive_int(value["id"]),
            name,
            full_name,
            owner_login,
            private,
            visibility,
            archived,
            disabled,
            default_branch,
            html_url,
            updated_at,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GitHubProviderUnavailableError() from exc
    if (
        owner_id != account_id
        or owner_login.casefold() != account_login.casefold()
        or result.full_name != f"{owner_login}/{name}"
    ):
        raise GitHubProviderAuthorizationError()
    return result


def _json_object(response: httpx.Response) -> dict[str, object]:
    try:
        value = response.json()
    except (ValueError, UnicodeError) as exc:
        raise GitHubProviderUnavailableError() from exc
    if not isinstance(value, dict):
        raise GitHubProviderUnavailableError()
    return value


def _permissions(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, dict) or not 1 <= len(value) <= 100:
        raise ValueError
    result = []
    for name, level in value.items():
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
            or level not in {"read", "write"}
        ):
            raise ValueError
        result.append((name, level))
    return tuple(sorted(result))


def _bounded_response(response: httpx.Response) -> None:
    length = response.headers.get("Content-Length")
    try:
        if length is not None and int(length) > MAX_RESPONSE_BYTES:
            raise GitHubProviderUnavailableError()
    except ValueError as exc:
        raise GitHubProviderUnavailableError() from exc
    if len(response.content) > MAX_RESPONSE_BYTES:
        raise GitHubProviderUnavailableError()


def _positive_int(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= 9_223_372_036_854_775_807
    ):
        raise ValueError
    return value


def _bounded_integer(name: str, value: object, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"GitHub {name} is invalid")
    return value


def _nonblank(value: object, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError
    return value


def _repository_component(value: object, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError
    return value


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError
    return value


def _default_branch(value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or _DEFAULT_BRANCH.fullmatch(value) is None
        or value.startswith(("-", "/", "."))
        or value.endswith(("/", ".", ".lock"))
        or any(part in value for part in ("..", "//", "@{"))
    ):
        raise ValueError
    return value


def _repository_html_url(
    value: object, owner_login: str, name: str, web_base_url: str
) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError
    parsed = urlparse(value)
    trusted = urlparse(web_base_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != trusted.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != f"/{owner_login}/{name}"
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError
    return value


def _opaque(value: object, name: str, minimum: int, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not minimum <= len(value) <= maximum
        or any(character.isspace() for character in value)
    ):
        raise ValueError(f"{name} is invalid")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError
    return result


def _aware_clock(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GitHubProviderUnavailableError()
    return value


def _total_count(
    value: object, page: int, page_size: int, item_count: int
) -> int | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= MAX_REPOSITORY_TOTAL_COUNT
    ):
        raise GitHubProviderUnavailableError()
    if item_count and value < (page - 1) * page_size + item_count:
        raise GitHubProviderUnavailableError()
    return value


def _link_has_next(
    value: str | None, *, api_base_url: str, page: int, page_size: int
) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > MAX_LINK_HEADER_BYTES:
        raise GitHubProviderUnavailableError()
    trusted = urlparse(api_base_url)
    relations: dict[str, int] = {}
    for part in value.split(","):
        match = _LINK_PART.fullmatch(part)
        if match is None or match.group(2) in relations:
            raise GitHubProviderUnavailableError()
        parsed = urlparse(match.group(1))
        try:
            query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
            if set(query) != {"page", "per_page"}:
                raise ValueError
            raw_page = query["page"]
            raw_size = query["per_page"]
            if len(raw_page) != 1 or len(raw_size) != 1:
                raise ValueError
            linked_page = int(raw_page[0])
            linked_size = int(raw_size[0])
            if str(linked_page) != raw_page[0] or str(linked_size) != raw_size[0]:
                raise ValueError
            _bounded_integer("page", linked_page, 1, MAX_REPOSITORY_PAGE)
            _bounded_integer("page_size", linked_size, 1, MAX_REPOSITORY_PAGE_SIZE)
        except (KeyError, TypeError, ValueError) as exc:
            raise GitHubProviderUnavailableError() from exc
        if (
            parsed.scheme != "https"
            or parsed.netloc != trusted.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != "/installation/repositories"
            or parsed.params
            or parsed.fragment
            or linked_size != page_size
        ):
            raise GitHubProviderUnavailableError()
        relations[match.group(2)] = linked_page
    if "next" in relations and relations["next"] != page + 1:
        raise GitHubProviderUnavailableError()
    return "next" in relations


def _rate_limit_delay(response: httpx.Response, now: datetime) -> float | None:
    value = response.headers.get("Retry-After")
    if value is not None:
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(value) - _aware_clock(now)).total_seconds()
            except (TypeError, ValueError):
                return None
        return max(delay, 0.0) if delay <= 30 else None
    if response.headers.get("X-RateLimit-Remaining") == "0":
        reset = response.headers.get("X-RateLimit-Reset")
        try:
            delay = float(reset) - _aware_clock(now).timestamp()
        except (TypeError, ValueError):
            return None
        return max(delay, 0.0) if delay <= 30 else None
    return None
