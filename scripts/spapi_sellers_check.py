import os
from dotenv import load_dotenv
from sp_api.api import Sellers
from sp_api.base import Marketplaces

load_dotenv()

sellers = Sellers(
    credentials={
        "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN"),
        "lwa_app_id": os.getenv("SPAPI_LWA_APP_ID"),
        "lwa_client_secret": os.getenv("SPAPI_LWA_CLIENT_SECRET"),
    },
    marketplace=Marketplaces.US,
)

resp = sellers.get_marketplace_participation()
print(resp.payload)

assert os.getenv("SPAPI_REFRESH_TOKEN")
assert os.getenv("SPAPI_LWA_APP_ID")
assert os.getenv("SPAPI_LWA_CLIENT_SECRET")
print("All env vars present")
