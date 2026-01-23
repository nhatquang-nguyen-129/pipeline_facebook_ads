import os
from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import json
import logging
from zoneinfo import ZoneInfo

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_facebook_ads import dags_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
    MODE
]):
    raise EnvironmentError("❌ [MAIN] Failed to execute Google Ads main entrypoint due to missing required environment variables.")

def main():
    """
    Google Ads entrypoint
    ---------
    Main is responsible for preparing the entire execution environment:
        - Resolve execution time window from MODE
        - Read & validate OS environment variables
        - Load secrets from GCP Secret Manager
        - Initialize Google Ads client exactly once
        - Dispatch execution to DAG orchestrator

    DAGs must NOT initialize clients or read secrets.
    DAGs only coordinate execution order and retries.
    """
    
    msg = (
        "🔄 [MAIN] Triggering to update Google Ads for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company with "
        f"{MODE} mode to Google Cloud project "
        f"{PROJECT}..."
    )
    print(msg)
    logging.info(msg)    

# Resolve input time range
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    today = datetime.now(ICT)
    
    if MODE == "today":
        start_date = end_date = today.strftime("%Y-%m-%d")

    elif MODE == "last3days":
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "last7days":
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "thismonth":
        start_date = today.replace(day=1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

    elif MODE == "lastmonth":
        last_month_end = today.replace(day=1) - timedelta(days=1)
        start_date = last_month_end.replace(day=1).strftime("%Y-%m-%d")
        end_date = last_month_end.strftime("%Y-%m-%d")

    else:
        raise ValueError(f"⚠️ [MAIN] Failed  Unsupported MODE='{MODE}'")
    
    msg = (
        "✅ [MAIN] Successfully resolved "
        f"{MODE} mode to date range from "
        f"{start_date} to "
        f"{end_date}."
    )
    print(msg)
    logging.info(msg)

# Initialize Google Secret Manager
    try:
        msg = ("🔍 [MAIN] Initialize Google Secret Manager client...")
        print(msg)
        logging.info(msg)
        
        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        msg = ("✅ [MAIN] Successfully initialized Google Secret Manager client.")
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to initialize Google Secret Manager client due to."
            f"{e}."
        )
        
# Resolve customer_id from Google Secret Manager
    try:
        secret_customer_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}"
        )
        secret_customer_name = (
            f"projects/{PROJECT}/secrets/{secret_customer_id}/versions/latest"
        )
        
        msg = (
            "🔍 [MAIN] Retrieving Google Ads secret_customer_id "
            f"{secret_customer_id} from Google Secret Manager..."
        )
        print(msg)
        logging.info(msg)        

        secret_customer_response = google_secret_client.access_secret_version(
            name=secret_customer_name,
            timeout=10.0, #DEBUG
        )
        google_customer_id = (
            secret_customer_response.payload.data.decode("utf-8")
            .replace("-", "")
            .replace(" ", "")
            .strip()
        )
        
        msg = (
            "✅ [MAIN] Successfully retrieved Google Ads customer_id "
            f"{google_customer_id} from Google Secret Manager."
        )
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve Google Ads customer_id from Google Secret Manager due to "
            f"{e}."
        )

    try:
        secret_credentials_json = (
            f"{COMPANY}_secret_all_google_token_access_user"
        )
        secret_credentials_name = (
            f"projects/{PROJECT}/secrets/{secret_credentials_json}/versions/latest"
        )
        
        msg = (
            "🔍 [MAIN] Retrieving Google Ads secret_credentials_json "
            f"{secret_credentials_json} from Google Secret Manager..."
        )
        print(msg)
        logging.info(msg)

        secret_credentials_response = google_secret_client.access_secret_version(
            name=secret_credentials_name
        )
        google_ads_credentials = json.loads(
            secret_credentials_response.payload.data.decode("UTF-8")
        )
        
        msg = ("✅ [MAIN] Successfully retrieved Google Ads credentials from Google Secret Manager.")
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve Google Ads credentials from Google Secret Manager due to "
            f"{e}."
        )        

# Initialize global Google Ads client
    google_ads_config = {
        "developer_token": google_ads_credentials["developer_token"],
        "client_id": google_ads_credentials["client_id"],
        "client_secret": google_ads_credentials["client_secret"],
        "refresh_token": google_ads_credentials["refresh_token"],
        "login_customer_id": google_ads_credentials["login_customer_id"],
        "use_proto_plus": True,
    }
    try:
        msg = (
            "🔍 [MAIN] Initializing global Google Ads client for customer_id "
            f"{google_customer_id}..."
        )
        print(msg)
        logging.info(msg)

        google_ads_client = GoogleAdsClient.load_from_dict(
            google_ads_config
        )

        msg = (
            "✅ [MAIN] Successfully initialized global Google Ads client for customer_id "
            f"{google_customer_id}."
        )
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to initialize global Google Ads client due to."
            f"{e}."
        )
   

# Execute DAGS
    dags_facebook_ads(
        google_ads_client=google_ads_client,
        customer_id=google_customer_id,
        start_date=start_date,
        end_date=end_date
    )

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)