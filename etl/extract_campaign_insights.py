import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

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
    ---
    Principles:
        1. Initialize Facebook Ads client
        2. Validate input start_date and end_date
        3. Make API call for AdAccount(account_id).get_insights endpoint (level=campaign)
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        1. pandas.DataFrame:
            Flattened Facebook Ads campaign insights records
    """

    # Initialize Facebook Ads client
    try:
        
        print(
            "🔍 [EXTRACT] Initializing Facebook Ads client for account_id "
            f"{account_id}..."
        )

        campaign_insights_session = FacebookSession(
            access_token=access_token,
            timeout=180,
        )

        campaign_insights_api = FacebookAdsApi(campaign_insights_session)

        print(
            "✅ [EXTRACT] Successfully initialized Facebook Ads client for account_id "
            f"{account_id}."
        )

    except Exception as e:
        
        error = RuntimeError(
            "❌ [EXTRACT] Failed to initialize Facebook Ads client for account_id "
            f"{account_id} due to "
            f"{e}."
        )
        
        error.retryable = False
        
        raise error from e

    # Make Facebook Ads API call for campaign insights
    fields = [
        "account_id",
        "campaign_id",
        "optimization_goal",
        "spend",
        "impressions",
        "clicks",
        "actions",
        "date_start",
        "date_stop",
    ]

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "campaign",
    }    
    
    try:
        
        print(
            "🔍 [EXTRACT] Extracting Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date}..."
        )

        account_id_prefixed = (
            account_id if account_id.startswith("act_")
            else f"act_{account_id}"
        )

        insights = AdAccount(
            account_id_prefixed,
            api=campaign_insights_api,
        ).get_insights(
            fields=fields,
            params=params,
        )

        rows = [dict(row) for row in insights]
        
        df = pd.DataFrame(rows)

        print(
            "✅ [EXTRACT] Successfully extracted Facebook Ads campaign insights with "
            f"{len(df)} row(s) for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date}."
        )

        return df

    except FacebookRequestError as e:
        
        api_error_code = None
        
        http_status = None

        try:
        
            api_error_code = e.api_error_code()
        
            http_status = e.http_status()
        
        except Exception:
        
            pass

        # Expired token
        if api_error_code == 190:
        
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
                f"{account_id} due to expired or invalid access token then manual re-authentication is required."
            )
            error.retryable = False
            raise error from e

        # Retryable API error
        if (
            (http_status and http_status >= 500)
            or api_error_code in {
                1,
                2,
                4,
                17,
                80000,
            }
        ):
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
                f"{account_id} from "
                f"{start_date} to "
                f"{end_date} due to API error "
                f"{e} then this request is eligible to retry."
            )
            error.retryable = True
            raise error from e

        # Non-retryable API error
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to API error "
            f"{e} then this request is not eligible to retry."
        )
        error.retryable = False
        raise error from e

        # Unknown non-retryable error
    except Exception as e:
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to "
            f"{e}."
        )
        error.retryable = False
        raise error from e