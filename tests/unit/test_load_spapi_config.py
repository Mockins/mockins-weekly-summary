import os
import pytest
from weekly_summary.extract.amazon.load_config import SpApiConfigError, load_spapi_config

def test_load_spapi_config_missing_vars_raises(monkeypatch):
    # Ensure all required vars are unset
    for k in [
        "SPAPI_MARKETPLACE_ID",
        "SPAPI_REGION",
        "SPAPI_REFRESH_TOKEN",
        "SPAPI_LWA_CLIENT_ID",
        "SPAPI_LWA_CLIENT_SECRET",
    ]:
        monkeypatch.delenv(k, raising=False)

    with pytest.raises(SpApiConfigError) as exc: 
        load_spapi_config()

    assert "Missing required environment variable" in str(exc.value)

def test_load_spapi_config_success(monkeypatch):
    monkeypatch.setenv("SPAPI_MARKETPLACE_ID", "ATVPDKIKX0DER")
    monkeypatch.setenv("SPAPI_REGION", "us-east-1")
    monkeypatch.setenv("SPAPI_REFRESH_TOKEN", "rtok")
    monkeypatch.setenv("SPAPI_LWA_APP_ID", "appid")
    monkeypatch.setenv("SPAPI_LWA_CLIENT_SECRET", "secret")


    cfg = load_spapi_config()

    assert cfg.marketplace_id == "ATVPDKIKX0DER"
    assert cfg.region == "us-east-1"
    assert cfg.refresh_token == "rtok"
    assert cfg.lwa_app_id == "appid"
    assert cfg.lwa_client_secret == "secret"

    