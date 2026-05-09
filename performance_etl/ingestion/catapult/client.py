"""
Catapult provider configuration and HTTP client wiring.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ingestion.common.config import get_env, load_provider_config
from ingestion.common.http_client import BaseHttpClient

EnvGetter = Callable[[str, str | None], str | None]

_REGIONAL_BASE_URLS = {
    "au": "https://connect-au.catapultsports.com/api/v6",
    "us": "https://connect-us.catapultsports.com/api/v6",
    "eu": "https://connect-eu.catapultsports.com/api/v6",
    "cn": "https://connect-cn.catapultsports-cn.com/api/v6",
}


@dataclass(frozen=True)
class CatapultAccountConfig:
    name: str
    api_key_env: str
    api_key: str
    team_code: str
    team_level: str


@dataclass(frozen=True)
class CatapultRuntimeConfig:
    provider: str
    api_version: str
    base_url: str
    default_page_size: int
    rate_limit_ms: int
    max_retries: int
    accounts: tuple[CatapultAccountConfig, ...]


class CatapultClient(BaseHttpClient):
    """HTTP client bound to a single Catapult account."""

    def __init__(
        self,
        runtime_config: CatapultRuntimeConfig,
        account: CatapultAccountConfig,
        *,
        timeout: int = 60,
    ) -> None:
        super().__init__(
            base_url=runtime_config.base_url,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {account.api_key}",
            },
            rate_limit_ms=runtime_config.rate_limit_ms,
            timeout=timeout,
            max_retries=runtime_config.max_retries,
        )
        self.account = account
        self.api_version = runtime_config.api_version
        self.default_page_size = runtime_config.default_page_size

    def build_activity_params(
        self,
        *,
        page: int = 1,
        page_size: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the standard paginated parameter payload for `/activities`."""
        params = dict(extra_params or {})
        params["page"] = page
        params["page_size"] = page_size or self.default_page_size
        return params


def build_catapult_runtime_config(
    provider_config: dict[str, Any] | None = None,
    *,
    env_get: EnvGetter = get_env,
) -> CatapultRuntimeConfig:
    """Resolve the configured Catapult accounts and shared client settings."""
    raw_config = provider_config or load_provider_config("catapult")
    provider = str(raw_config.get("provider", "catapult"))
    api_version = str(raw_config.get("api_version", "v6"))
    base_url = _resolve_base_url(raw_config, env_get=env_get)

    pagination_config = raw_config.get("pagination", {})
    default_page_size = int(pagination_config.get("default_page_size", 100))

    rate_limiting = raw_config.get("rate_limiting", {})
    rate_limit_ms = int(rate_limiting.get("delay_between_requests_ms", 200))
    max_retries = int(rate_limiting.get("max_retries", 3))

    accounts = tuple(_resolve_accounts(raw_config, env_get=env_get))
    if not accounts:
        raise ValueError("Catapult provider config must define at least one account.")

    return CatapultRuntimeConfig(
        provider=provider,
        api_version=api_version,
        base_url=base_url,
        default_page_size=default_page_size,
        rate_limit_ms=rate_limit_ms,
        max_retries=max_retries,
        accounts=accounts,
    )


def _resolve_base_url(
    provider_config: dict[str, Any],
    *,
    env_get: EnvGetter,
) -> str:
    base_url_env = provider_config.get("base_url_env")
    if base_url_env:
        env_value = env_get(str(base_url_env), None)
        if env_value:
            return env_value.rstrip("/")

    explicit_base_url = provider_config.get("base_url")
    if explicit_base_url:
        return str(explicit_base_url).rstrip("/")

    region = str(provider_config.get("region", "")).lower()
    if region in _REGIONAL_BASE_URLS:
        return _REGIONAL_BASE_URLS[region]

    raise ValueError("Catapult provider config is missing a resolvable base URL.")


def _resolve_accounts(
    provider_config: dict[str, Any],
    *,
    env_get: EnvGetter,
) -> list[CatapultAccountConfig]:
    seen_names: set[str] = set()
    resolved: list[CatapultAccountConfig] = []

    for account_config in provider_config.get("accounts", []):
        name = str(account_config.get("name", "")).strip()
        api_key_env = str(account_config.get("api_key_env", "")).strip()
        team_code = str(account_config.get("team_code", "")).strip()
        team_level = str(account_config.get("team_level", "")).strip()

        if not name:
            raise ValueError("Catapult account entries must define `name`.")
        if name in seen_names:
            raise ValueError(f"Duplicate Catapult account name: {name}")
        if not api_key_env:
            raise ValueError(f"Catapult account '{name}' is missing `api_key_env`.")

        api_key = env_get(api_key_env, None)
        if not api_key:
            continue

        resolved.append(
            CatapultAccountConfig(
                name=name,
                api_key_env=api_key_env,
                api_key=api_key,
                team_code=team_code,
                team_level=team_level,
            )
        )
        seen_names.add(name)

    return resolved
