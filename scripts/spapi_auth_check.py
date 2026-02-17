import os
from dotenv import load_dotenv
from sp_api.api import Reports
from sp_api.base import Marketplaces

load_dotenv()

reports = Reports(
    credentials={
        "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN"),
        "lwa_app_id": os.getenv("SPAPI_LWA_APP_ID"),
        "lwa_client_secret": os.getenv("SPAPI_LWA_CLIENT_SECRET"),
    },
    marketplace=Marketplaces.US,  # this implies North America region
)

resp = reports.get_reports(reportTypes=["GET_MERCHANT_LISTINGS_ALL_DATA"], pageSize=1)
print("SP-API auth check OK. Sample response keys:", list(resp.payload.keys()))
