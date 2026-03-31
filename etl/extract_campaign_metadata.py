import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.session import FacebookSession
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_campaign_metadata(
    access_token: str,
    account_id: str,
    campaign_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads campaign metadata
    ---
    Principles:
        1. Initialize Facebook Ads client
        2. Make API call for AdAccount endpoint
        3. Make API call for Campaign(campaign_id) endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        1. DataFrame:
            Flattened campaign metadata records
    """

    # Validate input
    if not campaign_ids:
        
        print(
            "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign metadata for account_id "
            f"{account_id} due to no input campaign_ids then extraction will be suspended."
        )
        
        return pd.DataFrame()

    # Initialize Facebook Ads client
    try:
        
        print(
            "🔍 [EXTRACT] Initializing Facebook Ads client for account_id "
            f"{account_id}..."
        )

        campaign_metadata_session = FacebookSession(
            access_token=access_token,
            timeout=180,
        )

        campaign_metadata_api = FacebookAdsApi(campaign_metadata_session)

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

    # Make Facebook Ads API call for ad account information
    try:
        
        print(
            "🔍 [EXTRACT] Extracting Facebook Ads account_name for account_id "
            f"{account_id}..."
        )

        account_id_prefixed = (
            account_id if account_id.startswith("act_")
            else f"act_{account_id}"
        )

        account_info = AdAccount(
            account_id_prefixed,
            api=campaign_metadata_api,
        ).api_get(fields=["name"])

        account_name = account_info.get("name")

        print(
            "✅ [EXTRACT] Successfully extracted Facebook Ads account_name "
            f"{account_name} for account_id "
            f"{account_id}."
        )

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
                "❌ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
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
                80000
            }
        ):
            
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
                f"{account_id} due to API error "
                f"{e} then this request is eligible to retry."
            )
            
            error.retryable = True
            
            raise error from e

        # Non-retryable API error
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
            f"{account_id} due to API error "
            f"{e} then this request is not eligible to retry."
        )
        
        error.retryable = False
        
        raise error from e

        # Unknown non-retryable error
    except Exception as e:
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
            f"{account_id} due to "
            f"{e}."
        )
        
        error.retryable = False
        
        raise error from e

    # Make Facebook Ads API call for campaign metadata
    rows: list[dict] = []

    print(
        "🔍 [EXTRACT] Extracting Facebook Ads campaign metadata for account_id "
        f"{account_id} with "
        f"{len(campaign_ids)} campaign_id(s)..."
    )

    for campaign_id in campaign_ids:

        try:

            campaign = Campaign(
                campaign_id,
                api=campaign_metadata_api,
            ).api_get(
                fields=[
                    "id",
                    "name",
                    "status",
                    "account_id",
                ]
            )

            rows.append(
                {
                    "campaign_id": campaign.get("id"),
                    "campaign_name": campaign.get("name"),
                    "status": campaign.get("status"),
                    "account_id": account_id,
                    "account_name": account_name,
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

        # Expired token
            if api_error_code == 190:
                
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for account_id "
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
                    80000
                }
            ):
                
                error = RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                    f"{campaign_id} due to API error "
                    f"{e} then this request is eligible to retry."
                )
                
                error.retryable = True
                
                raise error from e

        # Non-retryable API error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                f"{campaign_id} due to API error "
                f"{e} then this request is not eligible to retry."
            )
            
            error.retryable = False
            
            raise error from e

        # Unknown non-retryable error
        except Exception as e:
            
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                f"{campaign_id} due to "
                f"{e}."
            )
            
            error.retryable = False
            
            raise error from e

    df = pd.DataFrame(rows)

    print(
        "✅ [EXTRACT] Successfully extracted Facebook Ads campaign metadata for account_id "
        f"{account_id} with "
        f"{len(df)}/{len(campaign_ids)} row(s)."
    )
    
    return df