from __future__ import annotations

import os
from dataclasses import dataclass

@dataclass(frozen=True)
class SpApiConfig:
    refresh_token: str
    lwa_app_id: str
    lwa_client_secret: str
    region: str
    marketplace_id: str

def _require(name: str) -> str:
    value = os.getenv(name)
    if not value: 
        raise RuntimeError(f"Missing required env var: {name}")
    return value

def load_spapi_config() -> SpApiConfig: 
    """
    Load Amazon SP-API configuration from environment variables.
    """
    return SpApiConfig(
        refresh_token=_require("SPAPI_REFRESH_TOKEN"),
        lwa_app_id=_require("SPAPI_LWA_APP_ID"),
        lwa_client_secret=_require("SPAPI_LWA_CLIENT_SECRET"),
        region=os.getenv("SPAPI_REGION", "us-east-1"),
        marketplace_id=_require("SPAPI_MARKETPLACE_ID"),
    )
    

