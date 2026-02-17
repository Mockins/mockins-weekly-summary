from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .spapi_config import load_spapi_config



@dataclass(frozen=True)
class SpApiAuthBundle:
    """Everything needed to make authenticated SP-API calls (no calls executed here)."""
    lwa_app_id: str
    lwa_client_secret: str
    refresh_token: str
    region: str
    marketplace_id: str


def get_spapi_auth_bundle() -> SpApiAuthBundle:
    """
    Load SP-API credentials from environment (via load_spapi_config) and return
    an auth bundle. No network calls.
    """
    cfg = load_spapi_config()

    return SpApiAuthBundle(
        lwa_app_id=cfg.lwa_app_id,
        lwa_client_secret=cfg.lwa_client_secret,
        refresh_token=cfg.refresh_token,
        region=cfg.region,
        marketplace_id=cfg.marketplace_id,
    )

