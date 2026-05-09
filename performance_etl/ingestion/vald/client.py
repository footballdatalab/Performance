"""
VALD API client with OAuth 2.0 client-credentials authentication.

Manages token acquisition/caching and exposes per-product
:class:`~ingestion.common.http_client.BaseHttpClient` instances configured
with the correct regional base URL and ``Authorization`` header.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from ingestion.common.config import get_env, load_provider_config
from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_PRODUCTS = (
    "tenants",
    "profiles",
    "forcedecks",
    "forceframe",
    "nordbord",
    "humantrak",
    "smartspeed",
    "dynamo",
)

# Env var names for direct base URL overrides (product -> env var)
_PRODUCT_URL_ENV_MAP = {
    "forcedecks": "VALD_FD_BASE_URL",
    "forceframe": "VALD_FF_BASE_URL",
    "nordbord": "VALD_ND_BASE_URL",
    "humantrak": "VALD_HT_BASE_URL",
    "smartspeed": "VALD_SS_BASE_URL",
    "dynamo": "VALD_DM_BASE_URL",
    "tenants": "VALD_TENANTS_BASE_URL",
    "profiles": "VALD_PROFILES_BASE_URL",
}


class ValdClient:
    """Central VALD API client.

    Handles OAuth 2.0 client-credentials token lifecycle and provides
    :class:`BaseHttpClient` instances for each VALD product, pre-configured
    with the regional base URL and a valid ``Authorization: Bearer`` header.

    Args:
        config: Optional provider configuration dict.  When *None*, the
            configuration is loaded automatically from
            ``config/providers/vald.yml`` and corresponding env vars.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        if config is None:
            config = load_provider_config("vald")

        self._config = config
        auth_cfg = config["auth"]

        # Auth settings
        self._token_url: str = get_env(auth_cfg["token_url_env"])
        self._client_id: str = get_env(auth_cfg["client_id_env"])
        self._client_secret: str = get_env(auth_cfg["client_secret_env"])
        self._audience: str = auth_cfg["audience"]
        self._token_buffer_s: int = int(auth_cfg.get("token_buffer_seconds", 60))

        # Region
        region_env = config.get("region_env", "VALD_REGION")
        self._region: str = get_env(region_env, "eu")
        if self._region not in config["regional_hosts"]:
            raise ValueError(
                f"Unknown VALD region '{self._region}'. "
                f"Expected one of {list(config['regional_hosts'].keys())}."
            )
        self._regional_hosts: dict[str, str] = config["regional_hosts"][self._region]

        # Token cache
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

        # Per-product client cache (lazily created)
        self._clients: dict[str, BaseHttpClient] = {}

        logger.info(
            "ValdClient initialised — region=%s, token_url=%s",
            self._region,
            self._token_url,
        )

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _authenticate(self) -> str:
        """Request a new OAuth 2.0 access token via client-credentials flow.

        Posts to the VALD token endpoint with
        ``Content-Type: application/x-www-form-urlencoded``.

        Returns:
            The ``access_token`` string.

        Raises:
            requests.exceptions.HTTPError: If the token request fails.
        """
        logger.debug("Requesting new VALD access token from %s", self._token_url)

        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "audience": self._audience,
        }

        response = requests.post(
            self._token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )

        if response.status_code != 200:
            logger.error(
                "Token request failed — status=%d body=%s",
                response.status_code,
                response.text[:500],
            )
        response.raise_for_status()

        body = response.json()
        access_token: str = body["access_token"]
        expires_in: int = int(body.get("expires_in", 86400))

        # Cache token with a safety buffer so we refresh before actual expiry.
        self._access_token = access_token
        self._token_expires_at = time.monotonic() + expires_in - self._token_buffer_s

        logger.info(
            "VALD token acquired — expires_in=%ds, refresh_at=-%ds",
            expires_in,
            self._token_buffer_s,
        )
        return access_token

    def _get_token(self) -> str:
        """Return a valid access token, refreshing if necessary.

        Returns:
            A cached or freshly acquired ``access_token``.
        """
        if self._access_token is None or time.monotonic() >= self._token_expires_at:
            return self._authenticate()
        return self._access_token

    # ------------------------------------------------------------------
    # Product URL resolution
    # ------------------------------------------------------------------

    def _get_base_url(self, product: str) -> str:
        """Return the ``https://`` base URL for a given product.

        Checks for a direct env var override first (e.g. ``VALD_FD_BASE_URL``),
        then falls back to the regional host mapping from YAML config.

        Args:
            product: Product key (e.g. ``"forcedecks"``, ``"nordbord"``).

        Returns:
            Full base URL string.

        Raises:
            ValueError: If *product* is not found in env or regional config.
        """
        # 1. Check direct env var override
        env_var = _PRODUCT_URL_ENV_MAP.get(product)
        if env_var:
            url = get_env(env_var, "")
            if url:
                return url.rstrip("/")

        # 2. Fall back to regional hosts from YAML
        host = self._regional_hosts.get(product)
        if host is None:
            raise ValueError(
                f"No host configured for product '{product}' in region "
                f"'{self._region}'. Set {env_var or 'VALD_<PRODUCT>_BASE_URL'} "
                f"in .env or configure regional_hosts in vald.yml."
            )
        return f"https://{host}"

    # ------------------------------------------------------------------
    # Client factory
    # ------------------------------------------------------------------

    def get_client(self, product: str) -> BaseHttpClient:
        """Return a :class:`BaseHttpClient` for the specified product.

        The client is configured with the correct regional base URL and
        carries an ``Authorization: Bearer`` header.  Clients are cached
        per product; calling this method again for the same product returns
        the same instance (with a refreshed token if needed).

        Args:
            product: One of ``tenants``, ``profiles``, ``forcedecks``,
                ``forceframe``, ``nordbord``, ``humantrak``, ``smartspeed``,
                ``dynamo``.

        Returns:
            Configured :class:`BaseHttpClient`.
        """
        token = self._get_token()

        if product in self._clients:
            # Update the Authorization header in case the token was refreshed.
            self._clients[product]._session.headers["Authorization"] = f"Bearer {token}"
            return self._clients[product]

        base_url = self._get_base_url(product)
        client = BaseHttpClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            rate_limit_ms=200,
            timeout=60,
        )
        self._clients[product] = client
        logger.debug("Created BaseHttpClient for product=%s url=%s", product, base_url)
        return client

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def tenants_client(self) -> BaseHttpClient:
        """Pre-configured client for the Tenants API."""
        return self.get_client("tenants")

    @property
    def profiles_client(self) -> BaseHttpClient:
        """Pre-configured client for the Profiles API."""
        return self.get_client("profiles")

    @property
    def forcedecks_client(self) -> BaseHttpClient:
        """Pre-configured client for the ForceDecks API."""
        return self.get_client("forcedecks")

    @property
    def forceframe_client(self) -> BaseHttpClient:
        """Pre-configured client for the ForceFrame API."""
        return self.get_client("forceframe")

    @property
    def nordbord_client(self) -> BaseHttpClient:
        """Pre-configured client for the NordBord API."""
        return self.get_client("nordbord")

    @property
    def humantrak_client(self) -> BaseHttpClient:
        """Pre-configured client for the HumanTrak API."""
        return self.get_client("humantrak")

    @property
    def smartspeed_client(self) -> BaseHttpClient:
        """Pre-configured client for the SmartSpeed API."""
        return self.get_client("smartspeed")

    @property
    def dynamo_client(self) -> BaseHttpClient:
        """Pre-configured client for the DynaMo API."""
        return self.get_client("dynamo")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close all cached :class:`BaseHttpClient` sessions."""
        for client in self._clients.values():
            client.close()
        self._clients.clear()
        logger.debug("All VALD product clients closed.")

    def __enter__(self) -> "ValdClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
