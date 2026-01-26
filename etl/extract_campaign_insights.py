import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_campaign_insights(
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
        "date_start",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
    ]

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "campaign",
    }

    msg = (
        "🔍 [EXTRACT] Extracting Facebook Ads campaign insights for account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

    try:
        insights = AdAccount(account_id).get_insights(
            fields=fields,
            params=params,
        )

        rows = [dict(row) for row in insights]
        df = pd.DataFrame(rows)

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
            retryable = False
            raise RuntimeError("❌ [EXTRACT] Failed to extract Facebook Ads campaign insights due to token expired or invalid then manual token refresh is required.") from e

        # Unexpected non-retryable error
        if (
            (http_status and http_status >= 500)
            or api_error_code in {1, 2, 4, 17, 80000}
        ):
            retryable = True
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
                f"{account_id} from "
                f"{start_date} to "
                f"{end_date} due to API error then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable error
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to unexpected API error "
            f"{e} then this request is not eligible to retry."
        ) from e

        # Unknown non-retryable error
    except Exception as e:
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads campaign insights for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to unknown error "
            f"{e}."
        ) from e