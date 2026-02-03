from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import argparse
from datetime import datetime
import logging
import os

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from facebook_business.api import FacebookAdsApi

from dags.dags_facebook_ads import dags_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
]):
    raise EnvironmentError("❌ [BACKFILL] Failed to execute Facebook Ads manual entrypoint due to missing required environment variables.")

def backfill():
    """
    Backfill Facebook Ads campaign insights entrypoint
    ---------
    Workflow:
        1. Get execution time window through argparse
        2. Validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Initialize Facebook Ads SDK wrapper exactly once
        5. Dispatch execution to DAG orchestrator
    Return:
        None
    """

# CLI arguments parser for manual date range
    parser = argparse.ArgumentParser(description="Manual Facebook Ads ETL executor")
    parser.add_argument(
        "--start_date",
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end_date",
        required=True,
        help="End date in YYYY-MM-DD format"
    )
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError("❌ [BACKFILL] Failed to execute Facebook Ads main entrypoint due to start_date and end_date must be in YYYY-MM-DD format.")

    if start_date > end_date:
        raise ValueError("❌ [BACKFILL] Failed to execute Facebook Ads main entrypoint due to start_date must be less than or equal to end_date.")

    msg = (
        "🔄 [BACKFILL] Triggering to execute Facebook Ads main entrypoint for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company from "
        f"{start_date} to "
        f"{end_date} on Google Cloud Project "
        f"{PROJECT}..."
    )
    print(msg)
    logging.info(msg)

# Initialize Google Secret Manager
    try:
        msg = ("🔍 [BACKFILL] Initialize Google Secret Manager client...")
        print(msg)
        logging.info(msg)
        
        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        msg = ("✅ [BACKFILL] Successfully initialized Google Secret Manager client.")
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to initialize Google Secret Manager client due to."
            f"{e}."
        )
        
# Resolve account_id from Google Secret Manager
    try:
        secret_account_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_facebook_account_id_{ACCOUNT}"
        )
        secret_account_name = (
            f"projects/{PROJECT}/secrets/{secret_account_id}/versions/latest"
        )
        
        msg = (
            "🔍 [BACKFILL] Retrieving Facebook Ads secret_account_id "
            f"{secret_account_name} from Google Secret Manager..."
        )
        print(msg)
        logging.info(msg)        

        secret_account_response = google_secret_client.access_secret_version(
            name=secret_account_name,
            timeout=10.0,
        )
        facebook_account_id = secret_account_response.payload.data.decode("utf-8")
        
        msg = (
            "✅ [BACKFILL] Successfully retrieved Facebook Ads customer_id "
            f"{facebook_account_id} from Google Secret Manager."
        )
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to retrieve Facebook Ads customer_id from Google Secret Manager due to "
            f"{e}."
        )

    try:
        secret_token_id = (
            f"{COMPANY}_secret_all_facebook_token_access_user"
        )
        secret_token_name = (
            f"projects/{PROJECT}/secrets/{secret_token_id}/versions/latest"
        )
        
        msg = (
            "🔍 [BACKFILL] Retrieving Facebook Ads secret_credentials_json "
            f"{secret_token_name} from Google Secret Manager..."
        )
        print(msg)
        logging.info(msg)

        secret_token_response = google_secret_client.access_secret_version(
            name=secret_token_response
        )
        facebook_token_user = secret_token_response.payload.data.decode("utf-8")
        
        msg = ("✅ [BACKFILL] Successfully retrieved Google Ads credentials from Google Secret Manager.")
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to retrieve Google Ads credentials from Google Secret Manager due to "
            f"{e}."
        )        

# Initialize global Google Ads client
    try:
        msg = (
            "🔍 [BACKFILL] Initializing global Facebook Ads client for account_id "
            f"{facebook_account_id}..."
        )
        print(msg)
        logging.info(msg)

        FacebookAdsApi.init(
            access_token=facebook_token_user, 
            timeout=180
        )

        msg = (
            "✅ [BACKFILL] Successfully initialized global Facebook Ads client for customer_id "
            f"{facebook_account_id}."
        )
        print(msg)
        logging.info(msg)
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to initialize global Facebook Ads client due to."
            f"{e}."
        )
   
# Execute DAGS
    dags_facebook_ads(
        account_id=facebook_account_id,
        start_date=start_date,
        end_date=end_date
    )

# Entrypoint
if __name__ == "__backfill__":
    try:
        backfill()
    except Exception:
        sys.exit(1)