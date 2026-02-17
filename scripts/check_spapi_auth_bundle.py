from __future__ import annotations

from dotenv import load_dotenv

from weekly_summary.extract.amazon.spapi_client import get_spapi_auth_bundle


def mask(s: str, showz: int = 4) -> str:
    if not s: 
        return "<EMPTY>"
    if len(s) <= showz: 
        return "*" * len(s)
    return ("*" * (len(s) - showz)) + s[-showz:]


def main() -> None:
    load_dotenv() # Loads .env from project root

    b = get_spapi_auth_bundle()

    print("SP-API auth bundle loaded")
    print(f"region: {b.region}")
    print(f"marketplace_id: {b.marketplace_id}")
    print(f"lwa_app_id: {mask(b.lwa_app_id)}")
    print(f"refresh_token: {mask(b.refresh_token)}")

if __name__ == "__main__":
    main()