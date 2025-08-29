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

# Add logging capability for tracking process execution and errors
import logging

# Add Python Pandas library for data processing
import pandas as pd

# Add time utilities for retry delay and datetime handling
import time

# Add Facebook SDK components for accessing objects
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

# Add Google Secret Manager libraries for integration
from google.cloud import secretmanager

# Add internal Facebook module for data handling
from config.schema import ensure_table_schema

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

# 1. FETCH FACEBOOK AD ACCOUNT INFOMATION

# 1.1 Fetch the Facebook ad account's display name via Facebook Marketing API
def fetch_account_name() -> str:
    print("🚀 [FETCH] Starting to fetch Facebook ad account info...")
    logging.info("🚀 [FETCH] Starting to fetch Facebook ad account info...")
    
    try:
        # 1.1.1. Get Facebook Ad Account ID from Google Secret Manager
        print("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")
        logging.info("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")    
        try: 
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            account_id = response.payload.data.decode("utf-8")
            account = AdAccount(f"act_{account_id}")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account from Google Secret Manager due to {e}.")
        print(f"✅ [FETCH] Successfully retrieved information for Facebook ad account ID {account_id}.")
        logging.info(f"✅ [FETCH] Successfully retrieved information for Facebook ad account ID {account_id}.")

        # 1.1.2. Fetch Facebook Ad Account info
        print(f"🔍 [FETCH] Retrieving Facebook ad account name for Facebook ad account id {account_id}...")
        logging.info(f"🔍 [FETCH] Retrieving Facebook ad account name for Facebook ad account id {account_id}...")    
        account = AdAccount(f"act_{account_id}")
        account_info = account.api_get(fields=["name"])
        account_name = account_info.get("name", None)        
        print(f"✅ [FETCH] Successfully retrieved account name {account_name} for Facebook ad account id {account_id}.")
        logging.info(f"✅ [FETCH] Successfully retrieved account name {account_name} for Facebook ad account id {account_id}.")        
        return account_info.get("name", "")
    except FacebookRequestError as e:        
        print(f"⚠️ [FETCH] API Error while fetching Facebook account name due to {e.api_error_message()}.")
        logging.warning(f"⚠️ [FETCH] API Error while fetching Facebook account name due to {e.api_error_message()}.")    
        return ""    
    except Exception as e:        
        print(f"❌ [FETCH] Failed to fetch Facebook account name due to {e}.")
        logging.warning(f"❌ [FETCH] Failed to fetch Facebook account name due to {e}.")
        return ""

# 2. FETCH METADATA FOR FACEBOOK ADS FOR DIM DATAFRAME

# 2.1. Fetch metadata for campaigns in the Facebook Ad Account
def fetch_campaign_metadata(campaign_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 2.1.1. Validate input
    if not campaign_id_list:
        print("⚠️ [FETCH] Empty Facebook campaign_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook campaign_id_list provided.")
        return pd.DataFrame()

    # 2.1.2. Prepare fields
    default_fields = [
        "id", 
        "name", 
        "status",
        "effective_status",
        "objective",
        "configured_status",
        "buying_type"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []    
    print(f"🔍 [FETCH] Preparing to fetch Facebook campaign metadata with {fetch_fields} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook campaign metadata with {fetch_fields} field(s)...")
    
    try:
        # 2.1.3. Get Facebook ad account ad information
        print(f"🔍 [FETCH] Retrieving Facebook ad account information for {ACCOUNT} from Google Secret Manager...")
        logging.info(f"🔍 [FETCH] Retrieving Facebook ad account information for {ACCOUNT} from Google Secret Manager...")        
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            account_id = response.payload.data.decode("utf-8")
            account_info = AdAccount(f"act_{account_id}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account ID from Google Secret Manager: {e}")
        print(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")

        # 2.1.4. Loop through all campaign_id(s)
        print(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} Facebook campaign_id(s).")
        logging.info(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} Facebook campaign_id(s).")        
        for campaign_id in campaign_id_list:
            try:
                campaign = Campaign(fbid=campaign_id).api_get(fields=fetch_fields)
                record = {f: campaign.get(f, None) for f in fetch_fields}
                record["campaign_id"] = record.pop("id", None)
                record["campaign_name"] = record.pop("name", None)
                record["account_id"] = account_id
                record["account_name"] = account_name
                all_records.append(record)
            except FacebookRequestError as fb_err:
                print(f"⚠️ [FETCH] Facebook API error while fetching campaign_id {campaign_id} due to {fb_err.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Facebook API error while fetching campaign_id {campaign_id} due to {fb_err.api_error_message}..")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch Facebook metadata for campaign_id {campaign_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook metadata for campaign_id {campaign_id} due to {e}.")

        # 2.1.5. Convert to dataframe
        print(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} Facebook campaign_id(s) to dataframe...")
        logging.info(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} Facebook campaign_id(s) to dataframe...")   
        if not all_records:
            print("⚠️ [FETCH] No Facebook campaign metadata fetched.")
            logging.warning("⚠️ [FETCH] No Facebook campaign metadata fetched.")
            return pd.DataFrame()
        try:
            df = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted Facebook campaign metadata to dataframe with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook campaign metadata to dataframe with {len(df)} row(s).")        
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook {len(df)} campaign metadata(s) due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook {len(df)} campaign metadata(s) due to {e}.")
            return pd.DataFrame()

        # 2.1.6. Enforce schema
        try:
            print(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook campaign metadata...")
            logging.info(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook campaign metadata...")            
            df = ensure_table_schema(df, "fetch_campaign_metadata")            
            print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook campaign metadata.")
            logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook campaign metadata.")        
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for Facebook campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for Facebook campaign metadata due to {e}.")
            return pd.DataFrame()
        if not isinstance(df, pd.DataFrame):
            print("❌ [FETCH] Final Facebook campaign metadata output is not a DataFrame.")
            logging.error("❌ [FETCH] Final Facebook campaign metadata output is not a DataFrame.")
            return pd.DataFrame()
        print(f"✅ [FETCH] Successfully returned final Facebook campaign metadata dataframe with shape {df.shape}.")
        logging.info(f"✅ [FETCH] Successfully returned final Facebook campaign metadata dataframe with shape {df.shape}.")
        return df
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch Facebook campaign metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook campaign metadata due to {e}.")
        return pd.DataFrame()

# 2.2. Fetch metadata for adset in the Facebook Ad Account
def fetch_adset_metadata(adset_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook {len(adset_id_list)} adset metadata(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook {len(adset_id_list)} adset metadata(s)...")

    # 2.2.1. Validate input
    if not adset_id_list:
        print("⚠️ [FETCH] Empty Facebook adset_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook adset_id_list provided.")
        return pd.DataFrame()

    # 2.2.2. Prepare field(s)
    default_fields = [
        "id",
        "name",
        "status",
        "effective_status",
        "campaign_id"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []
    print(f"🔍 [FETCH] Preparing to fetch Facebook adset metadata with {fetch_fields} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook adset metadata with {fetch_fields} field(s)...")

    try:
        # 2.2.3. Get Facebook ad account ad information
        print("🔍 [FETCH] Retrieving Facebook ad account information...")
        logging.info("🔍 [FETCH] Retrieving Facebook ad account information...")        
        try: 
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            account_id = response.payload.data.decode("utf-8")
            account_info = AdAccount(f"act_{account_id}").api_get(fields=["name"])
            account_name = account_info.get("name", "Unknown")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve Facebook ad account ID for {ACCOUNT} account from Google Secret Manager: {e}")
        print(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")

        # 2.2.4. Loop through all adset_id(s)
        print(f"🔍 [FETCH] Retrieving metadata for {len(adset_id_list)} Facebook adset(s)...")
        logging.info(f"🔍 [FETCH] Retrieving metadata for {len(adset_id_list)} Facebook adset(s)...")
        for adset_id in adset_id_list:
            try:
                adset = AdSet(fbid=adset_id).api_get(fields=fetch_fields)
                record = {f: adset.get(f, None) for f in fetch_fields}
                record["adset_id"] = record.pop("id", None)
                record["adset_name"] = record.pop("name", None)
                record["account_id"] = account_id
                record["account_name"] = account_name
                all_records.append(record)
            except FacebookRequestError as fb_err:
                print(f"⚠️ [FETCH] Facebook API error while fetching adset {adset_id} due to {fb_err.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] Facebook API error while fetching adset {adset_id} due to {fb_err.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch Facebook adset {adset_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook adset {adset_id} due to {e}.")
        if not all_records:
            print("⚠️ [FETCH] No Facebook adset metadata fetched.")
            logging.warning("⚠️ [FETCH] No Facebook adset metadata fetched.")
            return pd.DataFrame()

        # 2.2.5. Convert to dataframe
        print(f"🔄 [FETCH] Converting metadata for {len(adset_id_list)} Facebook adset(s) to DataFrame...")
        logging.info(f"🔄 [FETCH] Converting metadata for {len(adset_id_list)} Facebook adset(s) to DataFrame...")  
        try:     
            df = pd.DataFrame(all_records)       
            print(f"✅ [FETCH] Successfully converted Facebook adset metadata to dataFrame with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted Facebook adset metadata to dataFrame with {len(df)} row(s).")       
        except Exception as e:
            print(f"❌ [FETCH] Faled to convert Facebook adset metadata due to {e}.")
            logging.error(f"❌ [FETCH] Faled to convert Facebook adset metadata due to {e}.")
            return pd.DataFrame()

        # 2.2.6. Enforce schema
        print(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook adset metadata...")
        logging.info(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook adset metadata...")        
        df = ensure_table_schema(df, "fetch_adset_metadata")        
        print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook adset metadata.")
        logging.info(f" [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook adset metadata.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to enforce Facebook adset metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce Facebook adset metadata due to {e}.")
        return pd.DataFrame()
    return df

# 2.3. Fetch metadata for all ads in the Facebook Ad Account
def fetch_ad_metadata(ad_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch Facebook ad metadata...")
    logging.info("🚀 [FETCH] Starting to fetch Facebook ad metadata...")

    # 2.3.1. Validate input
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        return pd.DataFrame()

    # 2.3.2. Implement retry logic
    MAX_RETRIES = 3
    SLEEP_BETWEEN_RETRIES = 2

    # 2.3.3. Prepare field(s)
    default_fields = ["id",
                      "name",
                      "adset_id",
                      "campaign_id",
                      "status",
                      "effective_status"]
    fetch_fields = fields if fields else default_fields
    all_records = []    
    print(f"🔍 [FETCH] Preparing to fetch Facebook ad metadata with {fetch_fields} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch Facebook ad metadata with {fetch_fields} field(s)...")

    # 2.3.4. Get Facebook ad account information
    print("🔍 [FETCH] Retrieving Facebook ad account information...")
    logging.info("🔍 [FETCH] Retrieving Facebook ad account information...")    
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        account_id = response.payload.data.decode("utf-8")
        account_info = AdAccount(f"act_{account_id}").api_get(fields=["name"])
        account_name = account_info.get("name", "Unknown")
        print(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} and account name {account_name} for {ACCOUNT} account.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to retrieve Facebook ad account information due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account information due to {e}.")
        return pd.DataFrame()

    # 2.3.5. Loop through all ad_id(s)
    print(f"🔍 [FETCH] Retrieving metadata for {len(ad_id_list)} Facebook ad(s)...")
    logging.info(f"🔍 [FETCH] Retrieving metadata for {len(ad_id_list)} Facebook ad(s)...")
    for ad_id in ad_id_list:
        for attempt in range(MAX_RETRIES):
            try:
                ad = Ad(fbid=ad_id).api_get(fields=fetch_fields)
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
                break
            except FacebookRequestError as fb_err:
                print(f"⚠️ [FETCH] API error while fetching Facebook ad metdata for ad_id {ad_id} due to {fb_err.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] API error while fetching Facebook ad metdata for ad_id {ad_id} due to {fb_err.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch Facebook ad metadata for ad_id {ad_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook ad metadata for ad_id {ad_id} due to {e}.")
            time.sleep(SLEEP_BETWEEN_RETRIES ** (attempt + 1))
    if not all_records:
        print("⚠️ [FETCH] No Facebook ad metadata fetched.")
        logging.warning("⚠️ [FETCH] No Facebook ad metadata fetched.")
        return pd.DataFrame()

    # 2.3.6. Convert to dataframe
    print(f"🔄 [FETCH] Converting metadata for {len(ad_id_list)} Facebook adset(s) to DataFrame...")
    logging.info(f"🔄 [FETCH] Converting metadata for {len(ad_id_list)} Facebook adset(s) to DataFrame...")  
    try:
        df = pd.DataFrame(all_records)        
        print(f"✅ [FETCH] Successfully converted Facebook ad metadata to DataFrame with {len(df)} row(s).")
        logging.info(f"✅ [FETCH] Successfully converted Facebook ad metadata to DataFrame with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [FETCH] Failed to convert Facebook ad metadata to DataFrame due to {e}.")
        logging.error(f"❌ [FETCH] Failed to convert Facebook ad metadata to DataFrame due to {e}.")
        return pd.DataFrame()

    # 2.3.7. Enforce schema
    print(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook ad metadata...")
    logging.info(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of Facebook ad metadata...")
    try:
        df = ensure_table_schema(df, "fetch_ad_metadata")
        print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook ad metadata.")
        logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook ad metadata.")    
    except Exception as e:
        print(f"❌ [FETCH] Faled to enforce Facebook ad metadata due to {e}.")
        logging.error(f"❌ [FETCH] Faled to enforce Facebook ad metadata due to {e}.")
        return pd.DataFrame()
    return df

# 2.4. Fetch thumbnail URL of the creative linked to the given ad_id
def fetch_ad_creative(ad_id_list: list[str]) -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch Facebook ad creatives (thumbnail only)...")
    logging.info("🚀 [FETCH] Starting to fetch Facebook ad creatives (thumbnail only)...")
    
    # 2.4.1. Validate input
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        return pd.DataFrame()
    
    # 2.4.2. Implement retry logic with maximum retry time limit
    MAX_RETRIES = 3
    SLEEP_BETWEEN_RETRIES = 2
    all_records = []
    
    # 2.4.3. Get Facebook ad account information
    print(f"🔍 [FETCH] Retrieving Facebook ad account information...")
    logging.info(f"🔍 [FETCH] Retrieving Facebook ad account information...")    
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        account_id = response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} for {ACCOUNT} account.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account due to {e}.")
    print(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} for {ACCOUNT} account.")
    logging.info(f"✅ [FETCH] Successfully retrieved Facebook account ID {account_id} for {ACCOUNT} account.")

    # 2.4.4. Loop through all ad_ids
    print(f"🔍 [FETCH] Retrieving Facebook ad creatives (thumbnail only) for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🔍 [FETCH] Retrieving Facebook ad creatives (thumbnail only) for {len(ad_id_list)} ad_id(s)...")    
    for ad_id in ad_id_list:
        for attempt in range(MAX_RETRIES):
            try:
                ad = Ad(fbid=ad_id).api_get(fields=["creative"])
                creative_id = ad.get("creative", {}).get("id", None)
                if not creative_id:
                    print(f"⚠️ [FETCH] Missing creative ID for Facebook ad_id {ad_id}.")
                    logging.warning(f"⚠️ [FETCH] Missing creative ID for Facebook ad_id {ad_id}.")
                    raise ValueError(f"⚠️ [FETCH] Missing creative ID for Facebook ad_id {ad_id}.")
                creative = AdCreative(fbid=creative_id).api_get(fields=["thumbnail_url"])
                thumbnail_url = creative.get("thumbnail_url", "")
                all_records.append({
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": thumbnail_url,
                    "account_id": account_id,
                })
                break
            except FacebookRequestError as fb_err:
                print(f"⚠️ [FETCH] API error while fetching Facebook creative on ad_id {ad_id} due to {fb_err.api_error_message}.")
                logging.warning(f"⚠️ [FETCH] API error while fetching Facebook creative on ad_id {ad_id} due to {fb_err.api_error_message}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch Facebook creative on ad_id {ad_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook creative on ad_id {ad_id} due to {e}.")
            time.sleep(SLEEP_BETWEEN_RETRIES ** (attempt + 1))
    if not all_records:
        print("⚠️ [FETCH] No Facebook creative metadata fetched.")
        logging.warning("⚠️ [FETCH] No Facebook creative metadata fetched.")
        return pd.DataFrame()
    
    # 2.4.5. Convert to dataframe
    print(f"🔄 [FETCH] Converting creative for {len(ad_id_list)} Facebook ad_id(s) to DataFrame...")
    logging.info(f"🔄 [FETCH] Converting creative for {len(ad_id_list)} Facebook ad_id(s) to DataFrame...")  
    try:
        df = pd.DataFrame(all_records)    
        print(f"✅ [FETCH] Successfully converted {len(df)} row(s) of Facebook ad creative to DataFrame.")
        logging.info(f"✅ [FETCH] Successfully converted {len(df)} row(s) of Facebook ad creative to DataFrame.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to convert Facebook ad creative to DataFrame due to {e}.")
        logging.error(f"❌ [FETCH] Failed to convert Facebook ad creative to DataFrame due to {e}.")
        return pd.DataFrame()

    # 2.4.6. Enforce schema
    try:
        print(f"🔄 [FETCH] Enforcing schema for Facebook ad creative with {len(df)} row(s)...")
        logging.info(f"🔄 [FETCH] Enforcing schema for Facebook ad creative with {len(df)} row(s)...")
        df = ensure_table_schema(df, "fetch_ad_creative")
        print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook ad creative.")
        logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of Facebook ad creative.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to enforce Facebook ad creative schema due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce Facebook ad creative schema due to {e}.")
        return pd.DataFrame()
    return df

# 3. FETCH INSIGHTS FOR FACT TABLE(S)

# 3.1. Fetch campaign-level insights from Facebook Marketing API between two dates
def fetch_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook campaign insights from {start_date} to {end_date}...")    

    # 3.1.1. Get Facebook Ad Account Info    
    try:
        print("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")
        logging.info("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")         
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        account_id = response.payload.data.decode("utf-8")
        account = AdAccount(f"act_{account_id}")     
        print(f"✅ [FETCH] Successfully retrieved Facebook ad account ID {account_id}.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook ad account ID {account_id}.")

        # 3.1.2. Define parameter(s) and field(s)
        params = {
            "level": "campaign",
            "time_increment": 1,
            "time_range": {"since": start_date, "until": end_date},
        }
        print(f"🔍 [FETCH] Preparing Facebook API request with {params} parameter(s).")
        logging.info(f"🔍 [FETCH] Preparing Facebook API request with {params} parameter(s).")
        fields = [
            "account_id", "campaign_id", "optimization_goal",
            "spend", "reach", "impressions", "clicks", "actions",
            "date_start", "date_stop"
        ]        
        print(f"🔍 [FETCH] Preparing Facebook API request with {fields} field(s).")
        logging.info(f"🔍 [FETCH] Preparing Facebook API request with {fields} field(s).")

        # 3.1.3. Make Facebook API call
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching Facebook campaign insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s).")
                logging.info(f"🔍 [FETCH] Fetching Facebook campaign insights for account_id {account_id} from {start_date} to {end_date} with {attempt + 1} attempt(s).")
                insights = account.get_insights(fields=fields, params=params)
                records = [dict(record) for record in insights]
                if not records:
                    print("⚠️ [FETCH] No data returned from Facebook API.")
                    logging.warning("⚠️ [FETCH] No data returned from Facebook API.")                    
                    return pd.DataFrame()
                df = pd.DataFrame(records)                
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for Facebook campaign insights from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for Facebook campaign insights from {start_date} to {end_date}.")
        
        # 3.1.4. Enforce schema for Facebook campaign insights
                print(f"🔄 [FETCH] Enforcing schema for Facebook campaign insights from {start_date} to {end_date}...")
                logging.info(f"🔄 [FETCH] Enforcing schema for Facebook campaign insights from {start_date} to {end_date}...")                
                df = ensure_table_schema(df, "fetch_campaign_insights")
                print(f"✅ [FETCH] Successfully enforced Facebook campaign insights schema with {len(df)} row(s) from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully enforced Facebook campaign insights schema with {len(df)} row(s) from {start_date} to {end_date}..")                
                return df
            except FacebookRequestError as e:
                print(f"⚠️ [FETCH] Facebook API error while fetching campaign insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e.api_error_message()}.")
                logging.error(f"⚠️ [FETCH] Facebook API error while fetching campaign insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e.api_error_message()}.")                
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)
            except Exception as e_inner:
                print(f"❌ [FETCH] Failed to fetch Facebook campaign insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e_inner}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook campaign insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e_inner}.")                
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)
    except Exception as e_outer:
        print(f"❌ [FETCH] Failed to fetch Facebook campaign insights from {start_date} to {end_date} due to {e_outer}.")
        logging.error(f"❌ [FETCH] Failed to fetch Facebook campaign insights from {start_date} to {end_date} due to {e_outer}.")     
        return pd.DataFrame()

# 3.2. Fetch ad insights from Facebook Marketing API between two dates
def fetch_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Facebook ad insights from {start_date} to {end_date}...") 
    
    # 3.2.1. Retrieve Facebook Ad Account Info
    try:
        print("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")
        logging.info("🔍 [FETCH] Retrieving Facebook ad account ID from Google Secret Manager...")   
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            account_id = response.payload.data.decode("utf-8")
            account = AdAccount(f"act_{account_id}")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook ad account information for {ACCOUNT} account due to {e}.")
        print(f"✅ [FETCH] Successfully retrieved Facebook ad account ID {account_id} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved Facebook ad account ID {account_id} for {ACCOUNT} account.")

    # 3.2.2. Define parameter(s) and field(s)
        params = {
            "level": "ad",
            "time_increment": 1,
            "time_range": {"since": start_date, "until": end_date},
        }
        print(f"🔍 [FETCH] Preparing Facebook API request with {params} param(s).")
        logging.info(f"🔍 [FETCH] Preparing Facebook API request with {params} param(s).")
        fields = [
            "account_id", "campaign_id", "adset_id",
            "ad_id", "spend", "reach", "impressions", "clicks", 
            "optimization_goal", "actions", "date_start", "date_stop"
        ]        
        print(f"🔍 [FETCH] Preparing Facebook API request with {fields} field(s).")
        logging.info(f"🔍 [FETCH] Preparing Facebook API request with {fields} field(s).")

        # 3.2.3. Make Facebook API call
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching Facebook ad insights for account_id {account_id} from {start_date} to {end_date} for {attempt + 1} attempt(s).")
                logging.info(f"🔍 [FETCH] Fetching Facebook ad insights for account_id {account_id} from {start_date} to {end_date} for {attempt + 1} attempt(s).")
                insights = account.get_insights(fields=fields, params=params)
                records = [dict(record) for record in insights]
                if not records:
                    print("⚠️ [FETCH] No data returned from Facebook API.")
                    logging.warning("⚠️ [FETCH] No data returned from Facebook API..")                    
                    return pd.DataFrame()
                df = pd.DataFrame(records)                
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for Facebook ad insights from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for Facebook ad insights from {start_date} to {end_date}.")
        
        # 3.2.4. Enforce schema
                print(f"🔄 [FETCH] Enforcing schema for Facebook ad insights from {start_date} to {end_date}...")
                logging.info(f"🔄 [FETCH] Enforcing schema for Facebook ad insights from {start_date} to {end_date}...")                
                df = ensure_table_schema(df, "fetch_ad_insights")                
                print(f"✅ [FETCH] Successfully enforced Facebook ad insights schema with {len(df)} row(s) from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully enforced Facebook ad insights schema with {len(df)} row(s) from {start_date} to {end_date}.")                
                return df
            except FacebookRequestError as fb_error:
                print(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {fb_error}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {fb_error}.")                
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)
            except Exception as e_inner:
                print(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e_inner}.")
                logging.error(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} for {attempt + 1} attempt(s) due to {e_inner}.")                
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)
    except Exception as e_outer:
        logging.error(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} due to {e_outer}.")
        print(f"❌ [FETCH] Failed to fetch Facebook ad insights from {start_date} to {end_date} due to {e_outer}.")
        return pd.DataFrame()