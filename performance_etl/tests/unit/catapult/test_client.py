from __future__ import annotations

from ingestion.catapult.client import CatapultClient, build_catapult_runtime_config


def _provider_config() -> dict:
    return {
        "provider": "catapult",
        "api_version": "v6",
        "base_url_env": "CATAPULT_BASE_URL",
        "region": "eu",
        "accounts": [
            {
                "name": "CATAPULT_A",
                "api_key_env": "CATAPULT_A_API_KEY",
                "team_code": "A",
                "team_level": "senior",
            },
            {
                "name": "CATAPULT_B",
                "api_key_env": "CATAPULT_B_API_KEY",
                "team_code": "B",
                "team_level": "senior",
            },
        ],
        "pagination": {"default_page_size": 100},
        "rate_limiting": {"delay_between_requests_ms": 200, "max_retries": 3},
    }


def _env_get(values: dict[str, str]):
    return lambda key, default=None: values.get(key, default)


def test_build_catapult_runtime_config_resolves_accounts_and_defaults() -> None:
    config = build_catapult_runtime_config(
        _provider_config(),
        env_get=_env_get(
            {
                "CATAPULT_BASE_URL": "https://connect-eu.catapultsports.com/api/v6",
                "CATAPULT_A_API_KEY": "token-a",
                "CATAPULT_B_API_KEY": "token-b",
            }
        ),
    )

    assert config.base_url == "https://connect-eu.catapultsports.com/api/v6"
    assert config.default_page_size == 100
    assert config.rate_limit_ms == 200
    assert [account.name for account in config.accounts] == ["CATAPULT_A", "CATAPULT_B"]


def test_build_catapult_runtime_config_skips_accounts_without_api_keys() -> None:
    config = build_catapult_runtime_config(
        _provider_config(),
        env_get=_env_get(
            {
                "CATAPULT_BASE_URL": "https://connect-eu.catapultsports.com/api/v6",
                "CATAPULT_B_API_KEY": "token-b",
            }
        ),
    )

    assert [account.name for account in config.accounts] == ["CATAPULT_B"]


def test_build_catapult_runtime_config_requires_at_least_one_account_api_key() -> None:
    try:
        build_catapult_runtime_config(
            _provider_config(),
            env_get=_env_get({"CATAPULT_BASE_URL": "https://connect-eu.catapultsports.com/api/v6"}),
        )
    except ValueError as exc:
        assert "at least one account" in str(exc)
    else:
        raise AssertionError("Expected Catapult runtime config without keys to raise ValueError.")


def test_catapult_client_builds_headers_base_url_and_default_pagination() -> None:
    runtime_config = build_catapult_runtime_config(
        _provider_config(),
        env_get=_env_get(
            {
                "CATAPULT_BASE_URL": "https://connect-eu.catapultsports.com/api/v6",
                "CATAPULT_A_API_KEY": "token-a",
                "CATAPULT_B_API_KEY": "token-b",
            }
        ),
    )

    client = CatapultClient(runtime_config, runtime_config.accounts[0])

    assert client.base_url == "https://connect-eu.catapultsports.com/api/v6"
    assert client._session.headers["Authorization"] == "Bearer token-a"
    assert client._session.headers["Accept"] == "application/json"
    assert client.build_activity_params(page=2) == {"page": 2, "page_size": 100}
