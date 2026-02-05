import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.session import FacebookSession
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

def extract_ad_creative(
    access_token: str,
    account_id: str,
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads ad creative
    ---------
    Workflow:
        1. Validate input ad_ids
        2. Loop each ad_id
        3. Make API call for Ad(ad_id) endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
        1. DataFrame:
            Flattened ad creative records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_ad_ids: list[str] = []
    retryable = False

    # Validate input    
    if not ad_ids:
        msg = (
            "⚠️ [EXTRACT] No input ad_ids for Facebook Ads account_id "
            f"{account_id} then empty DataFrame returned."
        )
        print(msg)
        logging.warning(msg)

        df = pd.DataFrame(
            columns=[
                "account_id",
                "ad_id",
                "creative_id",
                "thumbnail_url",
            ]
        )
        df.failed_ad_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    # Initialize Facebook Ads SDK client
    try:
        msg = (
            "🔍 [EXTRACT] Initializing Facebook Ads SDK client with account_id "
            f"{account_id} for ad creative extraction..."
        )
        print(msg)
        logging.info(msg)

        session = FacebookSession(
            access_token=access_token,
            timeout=180,
        )

        api = FacebookAdsApi(session)
        FacebookAdsApi.set_default_api(api)

        msg = (
            "✅ [EXTRACT] Successfully initialized Facebook Ads SDK client for account_id "
            f"{account_id} for ad creative extraction."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to initialize Facebook Ads SDK client for account_id "
            f"{account_id} for ad creative extraction due to "
            f"{e}."
        ) from e

    # Make Facebook Ads API call for ad creative
    msg = (
        "🔍 [EXTRACT] Extracting Facebook Ads ad creative for account_id "
        f"{account_id} with "
        f"{len(ad_ids)} ad_id(s)..."
    )
    print(msg)
    logging.info(msg)
        
    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(fields=["creative"])
            creative_id = ad.get("creative", {}).get("id")

            if not creative_id:
                rows.append(
                    {
                        "account_id": account_id,
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                    }
                )
                continue

            creative = AdCreative(creative_id).api_get(
                fields=["thumbnail_url"]
            )

            rows.append(
                {
                    "account_id": account_id,
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": creative.get("thumbnail_url"),
                }
            )

        except FacebookRequestError as e:
            api_error_code = None
            http_status = None

            try:
                api_error_code = e.api_error_code()
                http_status = e.http_status()
            except Exception:
                pass
            
        # Expired token error
            if api_error_code == 190:
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract Facebook Ads ad creative for account_id "
                    f"{account_id} due to expired or invalid access token then manual token refresh is required."
                )

        # Unexpected retryable API error
            if (
                (http_status and http_status >= 500)
                or api_error_code in {
                    1, 
                    2, 
                    4, 
                    17, 
                    80000
                }
            ):
                failed_ad_ids.append(ad_id)
                retryable = True

                msg = (
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                    f"{ad_id} due to API error "
                    f"{e} then this request is eligible to retry."
                )
                print(msg)
                logging.warning(msg)

                rows.append(
                    {
                        "account_id": account_id,
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                    }
                )
                continue
            
        # Unexpected non-retryable API error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                f"{ad_id} due to API error "
                f"{e} then this request is no eligible to retry."
            ) from e

        # Unknown non-retryable error
        except Exception as e:           
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads creative for ad_id "
                f"{ad_id} due to "
                f"{e}."
            ) from e

    df = pd.DataFrame(rows)

    msg = (
        "✅ [EXTRACT] Successfully extracted "
        f"{len(df)}/{len(ad_ids)} row(s) of Facebook Ads ad creative."
    )
    print(msg)
    logging.info(msg)   
    
    df.failed_ad_ids = failed_ad_ids
    df.retryable = retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = len(ad_ids)
    df.rows_output = len(df)

    return df