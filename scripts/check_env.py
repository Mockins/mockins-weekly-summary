from dotenv import load_dotenv
import os

load_dotenv()

keys = [
    "SPAPI_REFRESH_TOKEN",
    "SPAPI_LWA_APP_ID",
    "SPAPI_LWA_CLIENT_SECRET",
    "SPAPI_AWS_ACCESS_KEY_ID",
    "SPAPI_AWS_SECRET_ACCESS_KEY",
    "SPAPI_AWS_ROLE_ARN",
    "SPAPI_REGION",
    "SPAPI_MARKETPLACE_ID",
]

for k in keys: 
    print(f"{k} = ", "SET" if os.getenv(k) else "MISSING")