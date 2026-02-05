import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.session import FacebookSession
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_campaign_insights(
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Extract Facebook Ads campaign insights
    ---------
    Workflow:
        1. Validate input account_id
        2. Validate input start_date and end_date
        3. Make API call for AdAccount(account_id).get_insights endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign insights records
    """

    start_time = time.time()

    fields = [        
        "account_id", 
        "campaign_id", 
        "optimization_goal",
        "spend", 
        "impressions", 
        "clicks", 
        "actions",
        "date_start", 
        "date_stop"
    ]

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "campaign",
    }

    # Initialize Facebook Ads SDK client
    try:
        msg = (
            "🔍 [EXTRACT] Initializing Facebook Ads SDK client with account_id "
            f"{account_id} for campaign insights extraction..."
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
            f"{account_id} for campaign insights extraction."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to initialize Facebook Ads SDK client for account_id "
            f"{account_id} for campaign insights extraction due to "
            f"{e}."
        ) from e

    # Make Facebook Ads API call for campaign insights
    try:
        msg = (
            "🔍 [EXTRACT] Extracting Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date}..."
        )
        print(msg)
        logging.info(msg)

        account_id_prefixed = (
            account_id if account_id.startswith("act_")
            else f"act_{account_id}"
        )

        insights = AdAccount(account_id_prefixed).get_insights(
            fields=fields,
            params=params,
        )

        rows = [dict(row) for row in insights]
        df = pd.DataFrame(rows)

        msg = (
            "✅ [EXTRACT] Successfully extracted "
            f"{len(df)} row(s) of Facebook Ads campaign insights for account_id "           
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date}."
        )
        print(msg)
        logging.info(msg)        

        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = None
        df.rows_output = len(df)

        return df

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
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
                f"{account_id} from "
                f"{start_date} to "
                f"{end_date} due to expired or invalid access token then manual token refresh is required."
            ) from e

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
            retryable = True
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
                f"{account_id} from "
                f"{start_date} to "
                f"{end_date} due to API error "
                f"{e} then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable API error
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to API error "
            f"{e} then this request is not eligible to retry."
        ) from e
       
        # Unknown non-retryable error      
    except Exception as e:
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to "
            f"{e}."
        ) from e