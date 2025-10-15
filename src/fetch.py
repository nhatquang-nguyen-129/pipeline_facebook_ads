"""
==================================================================
FACEBOOK FETCHING MODULE
------------------------------------------------------------------
This module is responsible for direct, authenticated access to the 
Facebook Marketing API, encapsulating all logic required to 
fetch raw campaign, ad, creative, and metadata records.

It provides a clean interface to centralize API-related operations, 
enabling reusable, testable, and isolated logic for data ingestion 
pipelines without mixing transformation or storage responsibilities.

✔️ Initializes secure Facebook SDK sessions and retrieves credentials dynamically  
✔️ Fetches data via API calls (with pagination) and returns structured DataFrames  
✔️ Does not handle BigQuery upload, schema validation, or enrichment logic

⚠️ This module focuses only on *data retrieval from the API*. 
It does not handle schema validation, data transformation, or 
storage operations such as uploading to BigQuery.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integraton
import logging

# Add Python time ultilities for integration
import time

# Add Python Pandas libraries for integration
import pandas as pd

# Add Facebook Business modules for integration
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError
from facebook_business.api import FacebookAdsApi

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    GoogleAPICallError,
    NotFound,
    PermissionDenied, 
)
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal Facebook modules for handling
from src.schema import ensure_table_schema

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. FETCH FACEBOOK ADS METADATA

# 1.1. Fetch campaign metadata for Facebook Ads
def fetch_campaign_metadata(campaign_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Start timing the Facebook Ads campaign metadata fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    # 1.1.2. Validate input for Facebook Ads campaign metadata
    if not campaign_id_list:
        print("⚠️ [FETCH] Empty Facebook Ads campaign_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty Facebook Ads campaign_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty Facebook Ads campaign_id_list provided then fetching is suspended.")

    # 1.1.3. Prepare fields for Facebook Ads campaign metadata
    fetch_fields_default = [
        "id", 
        "name", 
        "status",
        "effective_status",
        "objective",
        "configured_status",
        "buying_type"
    ]
    all_records = []    
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads campaign metadata with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebooks Ads campaign metadata with {fetch_fields_default} field(s)...")
    
    try:
    
    # 1.1.4 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 1.1.5. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.1.6. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 1.1.7. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.1.8. Make Facebook Ads API call for ad account informatiohn
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads account name for account_id {account_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads account name for account_id {account_id}...")    
            account_id_prefixed = AdAccount(f"act_{account_id}")
            account_info = AdAccount(f"act_{account_id_prefixed}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")       
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account name {account_name} for account_id {account_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account name {account_name} for account_id {account_id}.")        
            return account_info.get("name", "")
        except FacebookRequestError as e:        
            print(f"⚠️ [FETCH] Failed to retrieved Facebook Ads account name for account_id {account_id} due to API Error {e.api_error_message()}.")
            logging.warning(f"⚠️ [FETCH] Failed to retrieved Facebook Ads account name for account_id {account_id} due to API Error {e.api_error_message()}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account name for account_id {account_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account name for account_id {account_id} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account name for account_id {account_id} due to {e}.")      

    # 1.1.9. Make Facebook Ads API call for campaign metadata
        print(f"🔍 [FETCH] Retrieving Facebook Ads campaign metadata for account_id {account_id} with {len(campaign_id_list)} campaign_id(s).")
        logging.info(f"🔍 [FETCH] Retrieving Facebook Ads campaign metadata for account_id {account_id} with {len(campaign_id_list)} campaign_id(s).")        
        for campaign_id in campaign_id_list:
            try:
                campaign = Campaign(fbid=campaign_id).api_get(fields=fetch_fields_default)
                record = {f: campaign.get(f, None) for f in fetch_fields_default}
                record["campaign_id"] = record.pop("id", None)
                record["campaign_name"] = record.pop("name", None)
                record["account_id"] = account_id
                record["account_name"] = account_name
                all_records.append(record)
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads campaign metadata for campaign_id {campaign_id} due to API error {facebook_error_request.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads campaign metadata for campaign_id {campaign_id} due to API error {facebook_error_request.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to retrieve Facebook Ads campaign metadata for campaign_id {campaign_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads campaign metadata for campaign_id {campaign_id} due to {e}.")
        if not all_records:
            print(f"⚠️ [FETCH] No Facebook Ads campaign metadata retrieved for account_id {account_id} then fetching is suspended.")
            logging.warning(f"⚠️ [FETCH] No Facebook Ads campaign metadata retrieved for account_id {account_id} then fetching is suspended.")
            return pd.DataFrame()

    # 1.1.10. Convert to Python DataFrame
        try:
            print(f"🔄 [FETCH] Converting Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) to Python DataFrame...")
            logging.info(f"🔄 [FETCH] Converting Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) to Python DataFrame...")  
            fetch_df_flattened = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted Facebook Ads campaign metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook Ads campaign metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")        
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook Ads campaign metadata for account_id {account_id} to Python DataFrame due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook Ads campaign metadata for account_id {account_id} to Python DataFrame due to {e}.")
            return pd.DataFrame()

    # 1.1.11. Enforce schema for Python DataFrame
        try:
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")            
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_campaign_metadata")            
            print(f"✅ [FETCH] Successfully enforced schema for Facebook Ads campaign metadata with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced schema for Facebook Ads campaign metadata with {len(fetch_df_enforced)} row(s).")        
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Facebook Ads campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Facebook Ads campaign metadata due to {e}.")
            return pd.DataFrame()

    # 1.1.12. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads campaign metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads campaign metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) due to {e}.")
        return pd.DataFrame()

# 1.2. Fetch adset metadata for Facebook Ads
def fetch_adset_metadata(adset_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")

    # 1.2.1. Start timing the Facebook Ads adset metadata fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    # 1.2.2. Validate input for Facebook Ads adset metadata
    if not adset_id_list:
        print("⚠️ [FETCH] Empty Facebook Ads adset_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty Facebook Ads adset_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty Facebook Ads adset_id_list provided then fetching is suspended.")
    
    # 1.2.3. Prepare field(s) for Facebook Ads adset metadata
    fetch_fields_default = [
        "id",
        "name",
        "status",
        "effective_status",
        "campaign_id"
    ]
    all_records = []
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads adset metadata with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebooks Ads adset metadata with {fetch_fields_default} field(s)...")

    try:

    # 1.2.4 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 1.2.5. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.2.6. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 1.2.7. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.2.8. Make Facebook Ads API call for ad account informatiohn
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")    
            account_id_prefixed = AdAccount(f"act_{account_id}")
            account_info = AdAccount(f"act_{account_id_prefixed}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")       
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")        
            return account_info.get("name", "")
        except FacebookRequestError as facebook_error_request:        
            print(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {facebook_error_request.api_error_message()}.")
            logging.warning(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {facebook_error_request.api_error_message()}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads ad account ID for {ACCOUNT} account from Google Secret Manager: {e}")    

    # 1.2.9. Make Facebook Ads API call for adset metadata
        print(f"🔍 [FETCH] Retrieving Facebook Ads adset metadata for account_id {account_id} with {len(adset_id_list)} adset_id(s)...")
        logging.info(f"🔍 [FETCH] Retrieving Facebook Ads adset metadata for account_id {account_id} with {len(adset_id_list)} adset_id(s)...")
        for adset_id in adset_id_list:
            try:
                adset = AdSet(fbid=adset_id).api_get(fields=fetch_fields_default)
                record = {f: adset.get(f, None) for f in fetch_fields_default}
                record["adset_id"] = record.pop("id", None)
                record["adset_name"] = record.pop("name", None)
                record["account_id"] = account_id
                record["account_name"] = account_name
                all_records.append(record)
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads adset metadata for adset_id {adset_id} due to {facebook_error_request.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads adset metadata for adset_id {adset_id} due to {facebook_error_request.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch Facebook Ads adset metadata for adset_id {adset_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads adset metadata for adset_id {adset_id} due to {e}.")
        if not all_records:
            print(f"⚠️ [FETCH] No Facebook Ads adset metadata retrieved for account_id {account_id} then fetching is suspended.")
            logging.warning(f"⚠️ [FETCH] No Facebook Ads adset metadata retrieved for account_id {account_id} then fetching is suspended.")
            return pd.DataFrame()

    # 1.2.10. Convert to Python DataFrame
        try:     
            print(f"🔄 [FETCH] Converting Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) to Python DataFrame...")
            logging.info(f"🔄 [FETCH] Converting Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) to Python DataFrame...")  
            fetch_df_flattened = pd.DataFrame(all_records)       
            print(f"✅ [FETCH] Successfully converted Facebook Ads adset metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook Ads adset metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")       
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook Ads adset metadata to Python DataFrame due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook Ads adset metadata to Python DataFrame due to {e}.")
            return pd.DataFrame()

    # 1.2.11. Enforce schema for Python DataFrame
        try:
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads adset metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads adset metadata with {len(fetch_df_flattened)} row(s)...")        
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_adset_metadata")        
            print(f"✅ [FETCH] Successfully enforced schema for Facebook Ads adset metadata with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced schema for Facebook Ads adset metadata with {len(fetch_df_enforced)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce Facebook Ads adset metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce Facebook Ads adset metadata due to {e}.")
            return pd.DataFrame()

    # 1.2.12. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads adset metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads adset metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) due to {e}.")
        return pd.DataFrame()

# 1.3. Fetch metadata for all ads in the Facebook Ad Account
def fetch_ad_metadata(ad_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")

    # 1.1.1. Start timing the Facebook Ads ad metadata fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign metadata for {len(ad_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign metadata for {len(ad_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    # 1.3.1. Validate input for Facebook Ads ad metadata
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")

    # 1.3.3. Prepare field(s) for Facebook Ads ad metadata
    fetch_fields_default = ["id",
                      "name",
                      "adset_id",
                      "campaign_id",
                      "status",
                      "effective_status"]
    all_records = []    
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads ad metadata with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebooks Ads ad metadata with {fetch_fields_default} field(s)...")

    try:

    # 1.3.4 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 1.3.5. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.3.6. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 1.3.7. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.3.8. Make Facebook Ads API call for ad account informatiohn
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")    
            account_id_prefixed = AdAccount(f"act_{account_id}")
            account_info = AdAccount(f"act_{account_id_prefixed}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")       
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")        
            return account_info.get("name", "")
        except FacebookRequestError as e:        
            print(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
            logging.warning(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager: {e}")      

    # 1.3.9. Make Facebook Ads API call for ad metadata
        print(f"🔍 [FETCH] Retrieving Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s).")
        logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s).")     
        for ad_id in ad_id_list:
            try:
                ad = Ad(fbid=ad_id).api_get(fields=fetch_fields_default)
                record = {
                    "ad_id": ad.get("id"),
                    "ad_name": ad.get("name"),
                    "adset_id": ad.get("adset_id"),
                    "campaign_id": ad.get("campaign_id"),
                    "status": ad.get("status"),
                    "effective_status": ad.get("effective_status"),
                    "account_id": account_id,
                    "account_name": account_name,
                }
                all_records.append(record)
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads ad metadata while fetching ad_id {ad_id} due to API error {facebook_error_request.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads ad metadata while fetching ad_id {ad_id} due to API error {facebook_error_request.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to retrieve Facebook Ads ad metadata for ad_id {ad_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads ad metadata for ad_id {ad_id} due to {e}.")
        if not all_records:
            print("⚠️ [FETCH] No Facebook Ads ad metadata retrieved then fetching is suspended.")
            logging.warning("⚠️ [FETCH] No Facebook Ads ad metadata retrieved then fetching is suspended.")
            return pd.DataFrame()

    # 1.3.10. Convert to Python DataFrame
        try:
            print(f"🔄 [FETCH] Converting Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) to Python DataFrame...")
            logging.info(f"🔄 [FETCH] Converting Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) to Python DataFrame...")  
            fetch_df_flattened = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted Facebook Ads ad metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook Ads ad metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")        
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook Ads ad metadata to Python DataFrame due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook Ads ad metadata to Python DataFrame due to {e}.")
            return pd.DataFrame()

    # 1.3.11. Enforce schema for Python DataFrame
        try:
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad metadata with {len(fetch_df_flattened)} row(s)...")            
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_ad_metadata")            
            print(f"✅ [FETCH] Successfully enforced schema for Facebook Ads ad metadata with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced schema for Facebook Ads ad metadata with {len(fetch_df_enforced)} row(s).")        
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Facebook Ads ad metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Facebook Ads ad metadata due to {e}.")
            return pd.DataFrame()

    # 1.3.12. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads ad metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads ad metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) due to {e}.")
        return pd.DataFrame()

# 1.4. Fetch ad creative for Facebook Ads
def fetch_ad_creative(ad_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")

    # 1.4.1. Start timing the Facebook Ads adset metadata fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads ad creative for {len(ad_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads ad creative for {len(ad_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    # 1.4.2. Validate input for Facebook Ads ad creative
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty Facebook Ads ad_id_list provided then fetching is suspended.")
    all_records = []

    try:

    # 1.4.3 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 1.4.4. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.4.5. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 1.4.6. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.4.7. Make Facebook Ads API call for ad creative
        print(f"🔍 [FETCH] Retrieving Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")    
        for ad_id in ad_id_list:
            try:
                ad = Ad(fbid=ad_id).api_get(fields=["creative"])
                creative_id = ad.get("creative", {}).get("id", None)
                if not creative_id:
                    print(f"⚠️ [FETCH] Failed to retrieve creative ID for Facebook Ads ad_id {ad_id}.")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve creative ID for Facebook Ads ad_id {ad_id}.")
                    raise ValueError(f"⚠️ [FETCH] Failed to retrieve creative ID for Facebook Ads ad_id {ad_id}.")
                creative = AdCreative(fbid=creative_id).api_get(fields=["thumbnail_url"])
                thumbnail_url = creative.get("thumbnail_url", "")
                all_records.append({
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": thumbnail_url,
                    "account_id": account_id,
                })
                break
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads ad creative for ad_id {ad_id} due to {facebook_error_request.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads ad creative for ad_id {ad_id} due to {facebook_error_request.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to retrieve Facebook Ads ad creative for ad_id {ad_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads ad creative for ad_id {ad_id} due to {e}.")
        if not all_records:
            print("⚠️ [FETCH] No Facebook Ads ad creative retrieved then fetching is suspended.")
            logging.warning(" [FETCH] No Facebook Ads ad creative retrieved then fetching is suspended.")
            return pd.DataFrame()
    
    # 1.4.8. Convert to Python DataFrame
        try:
            print(f"🔄 [FETCH] Converting Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) to Python DataFrame...")
            logging.info(f"🔄 [FETCH] Converting Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) to Python DataFrame...")  
            fetch_df_flattened = pd.DataFrame(all_records)    
            print(f"✅ [FETCH] Successfully converted Facebook Ads ad creative to Python DataFrame with {len(fetch_df_flattened)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook Ads ad creative to Python DataFrame with {len(fetch_df_flattened)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook Ads ad creative to Python DataFrame due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook Ads ad creative to Python DataFrame due to {e}.")
            return pd.DataFrame()

    # 1.4.9. Enforce schema for Python DataFrame
        try:
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad creative with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad creative with {len(fetch_df_flattened)} row(s)...")
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_ad_creative")
            print(f"✅ [FETCH] Successfully enforced schema for Facebook Ads ad creative with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced schema for Facebook Ads ad creative with {len(fetch_df_enforced)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce Facebook Ads ad creative due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce Facebook Ads ad creative due to {e}.")
            return pd.DataFrame()

    # 1.2.10. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads ad creative fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads ad creative fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) due to {e}.")
        return pd.DataFrame()

# 2. FETCH FACEBOOK ADS INSIGHTS

# 2.1. Fetch campaign insights for Facebook Ads
def fetch_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads campaign insights from {start_date} to {end_date}...")    

    # 2.1.1. Start timing the Facebook Ads campaign insights fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.1.2. Prepare field(s) and parameter(s) for Facebook Ads campaign insights
    fetch_params_default = {
    "level": "campaign",
    "time_increment": 1,
    "time_range": {"since": start_date, "until": end_date},
    }
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads campaign insights with {fetch_params_default} parameter(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook Ads campaign insights with {fetch_params_default} parameter(s)...")
    fetch_fields_default = [
        "account_id", "campaign_id", "optimization_goal",
        "spend", "reach", "impressions", "clicks", "actions",
        "date_start", "date_stop"
    ]        
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads campaign insights with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook Ads campaign insights with {fetch_fields_default} field(s)...")

    try:

    # 2.1.3 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 2.1.4. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.1.5. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 2.1.6. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.1.7. Make Facebook Ads API call for ad account informatiohn
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")    
            account_id_prefixed = AdAccount(f"act_{account_id}")
            account_info = AdAccount(f"act_{account_id_prefixed}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")       
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")        
            return account_info.get("name", "")
        except FacebookRequestError as e:        
            print(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
            logging.warning(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager: {e}")  

    # 2.1.8. Make Facebook Ads API call for campaign insights
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Retrieving Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s)...")
                logging.info(f"🔍 [FETCH] Retrieving Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s)...")
                campaign_insights_response = account_id_prefixed.get_insights(
                    fields=fetch_fields_default,
                    params=fetch_params_default
                )
                campaign_insights_records = [dict(record) for record in campaign_insights_response]
                if not campaign_insights_records:
                    print("⚠️ [FETCH] Empty data returned for Facebook Ads campaign insights then fetching is suspended.")
                    logging.warning("⚠️ [FETCH] Empty data returned for Facebook Ads campaign insights then fetching is suspended.")
                    return pd.DataFrame()
                fetch_df_flattened = pd.DataFrame(campaign_insights_records)
                print(f"✅ [FETCH] Successfully retrieved Facebook Ads campaign insights with {len(fetch_df_flattened)} row(s) for account_id {account_id} from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads campaign insights with {len(fetch_df_flattened)} row(s) for account_id {account_id} from {start_date} to {end_date}.")
                break
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to API Error {facebook_error_request.api_error_message()}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to API Error {facebook_error_request.api_error_message()}.")
                if attempt == 1:
                    print("⚠️ [FETCH] Exceeded maximum retry attempts for Facebook Ads campaign insights then fetching is suspended.")
                    logging.error("⚠️ [FETCH] Exceeded maximum retry attempts for Facebook Ads campaign insights then fetching is suspended.")
                    return pd.DataFrame()
            except Exception as e:
                print(f"❌ [FETCH] Failed to retrieve Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
                raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
        
    # 2.1.9. Enforce schema for Facebook Ads campaign insights
        try:     
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")                
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_campaign_insights")
            print(f"✅ [FETCH] Successfully enforced Facebook Ads campaign insights schema for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced Facebook Ads campaign insights schema for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_enforced)} row(s).")                
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Facebook Ads campaign insights from {start_date} to {end_date} for account_id {account_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Facebook Ads campaign insights from {start_date} to {end_date} for account_id {account_id} due to {e}.")                

    # 2.2.10. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads campaign insights fetching for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads campaign insights fetching for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads campaign insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
        return pd.DataFrame()

# 2.2. Fetch ad insights for Facebook Ads
def fetch_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook Ads ad insights from {start_date} to {end_date}...")    

    # 2.2.1. Start timing the Facebook Ads ad insights fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to fetch Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")


    # 2.2.2. Prepare field(s) and parameter(s) for Facebook Ads ad insights
    fetch_params_default = {
        "level": "ad",
        "time_increment": 1,
        "time_range": {"since": start_date, "until": end_date},
    }
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads ad insights with {fetch_params_default} parameter(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook Ads ad insights with {fetch_params_default} parameter(s)...")
    fetch_fields_default = [
        "account_id", "campaign_id", "adset_id",
        "ad_id", "spend", "reach", "impressions", "clicks", 
        "optimization_goal", "actions", "date_start", "date_stop"
    ]        
    print(f"🔍 [FETCH] Preparing to fetch Facebook Ads ad insights with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook Ads ad insights with {fetch_fields_default} field(s)...")

    try:

    # 2.2.3 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 2.2.4. Get Facebook Ads access token from Google Secret Manager
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.2.5. Initialize Facebook SDK session from access token
        try:
            print(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            logging.info(f"🔍 [FETCH] Initializing Facebook SDK session for account {ACCOUNT} with access token...")
            FacebookAdsApi.init(access_token=token_access_user, timeout=180)
            print(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
            logging.info(f"✅ [FETCH] Successfully initialized Facebook SDK session for account {ACCOUNT} with access token.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Facebook SDK session for account {ACCOUNT} due to {e}.")

    # 2.2.6. Get Facebook Ads account_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account ID for account {ACCOUNT} from Google Secret Manager...")
            account_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            account_secret_name = f"projects/{PROJECT}/secrets/{account_secret_id}/versions/latest"
            account_secret_response = google_secret_client.access_secret_version(request={"name": account_secret_name})
            account_id = account_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads account_id {account_id} for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads account_id for account {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.2.7. Make Facebook Ads API call for ad account informatiohn
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad account name for Facebook Ads ad account id {account_id}...")    
            account_id_prefixed = AdAccount(f"act_{account_id}")
            account_info = AdAccount(f"act_{account_id_prefixed}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")       
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads ad account name {account_name} for ad account ID {account_id}.")        
            return account_info.get("name", "")
        except FacebookRequestError as e:        
            print(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
            logging.warning(f"⚠️ [FETCH] Failed to retrieved Facebook Ads ad account name due to API Error {e.api_error_message()}.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager: {e}")  

    # 2.2.8. Make Facebook Ads API call for ad insights
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Retrieving Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s)...")
                logging.info(f"🔍 [FETCH] Retrieving Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s)...")
                ad_insights_response = account_id_prefixed.get_insights(
                    fields=fetch_fields_default,
                    params=fetch_params_default
                )
                ad_insights_records = [dict(record) for record in ad_insights_response]
                if not ad_insights_records:
                    print("⚠️ [FETCH] Empty data returned for Facebook Ads ad insights then fetching is suspended.")
                    logging.warning("⚠️ [FETCH] Empty data returned for Facebook Ads ad insights then fetching is suspended.")
                    return pd.DataFrame()
                fetch_df_flattened = pd.DataFrame(ad_insights_records)
                print(f"✅ [FETCH] Successfully retrieved Facebook Ads campaign insights with {len(fetch_df_flattened)} row(s) for account_id {account_id} from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads campaign insights with {len(fetch_df_flattened)} row(s) for account_id {account_id} from {start_date} to {end_date}.")
                break
            except FacebookRequestError as facebook_error_request:
                print(f"⚠️ [FETCH] Failed to retrieve Facebook Ads adn insights for account_id {account_id} from {start_date} to {end_date} due to API Error {facebook_error_request.api_error_message()}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to API Error {facebook_error_request.api_error_message()}.")
                if attempt == 1:
                    print("⚠️ [FETCH] Exceeded maximum retry attempts for Facebook Ads ad insights then fetching is suspended.")
                    logging.error("⚠️ [FETCH] Exceeded maximum retry attempts for Facebook Ads adinsights then fetching is suspended.")
                    return pd.DataFrame()
            except Exception as e:
                print(f"❌ [FETCH] Failed to retrieve Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
                raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")

    # 2.2.9. Enforce schema for Facebook Ads campaign insights
        try:     
            print(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")                
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_ad_insights")
            print(f"✅ [FETCH] Successfully enforced Facebook Ads ad insights schema for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_enforced)} row(s).")
            logging.info(f"✅ [FETCH] Successfully enforced Facebook Ads ad insights schema for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_enforced)} row(s).")                
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Facebook Ads ad insights from {start_date} to {end_date} for account_id {account_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Facebook Ads ad insights from {start_date} to {end_date} for account_id {account_id} due to {e}.")    
        
    # 2.2.10. Summarize fetch result(s)
        fetch_df_final = fetch_df_enforced
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights fetching for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights fetching for account_id {account_id} from {start_date} to {end_date} with {len(fetch_df_final)} row(s) in {elapsed}s.")
        return fetch_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook Ads ad insights for account_id {account_id} from {start_date} to {end_date} due to {e}.")
        return pd.DataFrame()