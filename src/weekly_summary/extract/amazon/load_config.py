from __future__ import annotations 

import os

from weekly_summary.extract.amazon.spapi_config import SpApiConfig

class SpApiConfigError(ValueError):
    """Raised when required SP-API configuration is missing or invalid. """

def _require_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or value.strip() == "":
        raise SpApiConfigError(f"Missing required environment variable: {key}")
    return value.strip()

def load_spapi_config() -> SpApiConfig:
    """
    Load SP-API config from environment variables.

    This function performs only validation + object construction.
    It does NOT make any network calls.
    """
    return SpApiConfig(
        marketplace_id=_require_env("SPAPI_MARKETPLACE_ID"),
        region=_require_env("SPAPI_REGION"),
        refresh_token=_require_env("SPAPI_REFRESH_TOKEN"),
        lwa_app_id=_require_env("SPAPI_LWA_APP_ID"),
        lwa_client_secret=_require_env("SPAPI_LWA_CLIENT_SECRET"),
    )
