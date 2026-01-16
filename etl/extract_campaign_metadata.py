from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi


def extract_campaign_metadata(
    campaign_ids: list[str],
    account_id: str,
) -> list[dict]:

    if not campaign_ids:
        return []

    try:
        account = AdAccount(f"act_{account_id}")
        account_info = account.api_get(fields=["name"])
        ad_account_name = account_info.get("name")
    except Exception:
        ad_account_name = None

    fields = [
        "id",
        "name",
        "status",
        "effective_status",
        "objective",
        "configured_status",
    ]

    results: list[dict] = []

    for campaign_id in campaign_ids:
        try:
            campaign = Campaign(campaign_id).api_get(fields=fields)

            row = {
                "campaign_id": campaign.get("id"),
                "campaign_name": campaign.get("name"),
                "status": campaign.get("status"),
                "effective_status": campaign.get("effective_status"),
                "objective": campaign.get("objective"),
                "configured_status": campaign.get("configured_status"),
                "ad_account_id": account_id,
                "ad_account_name": ad_account_name,
            }

            results.append(row)

        except Exception:

            continue

    return results