from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount

def extract_ad_metadata(
    ad_ids: list[str],
    account_id: str,
) -> list[dict]:

    if not ad_ids:
        return []

    try:
        account = AdAccount(f"act_{account_id}")
        account_info = account.api_get(fields=["name"])
        account_name = account_info.get("name")
    except Exception:
        account_name = None

    fields = [
        "id",
        "name",
        "adset_id",
        "campaign_id",
        "status",
        "effective_status",
    ]

    results: list[dict] = []

    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(fields=fields)

            row = {
                "ad_id": ad.get("id"),
                "ad_name": ad.get("name"),
                "adset_id": ad.get("adset_id"),
                "campaign_id": ad.get("campaign_id"),
                "status": ad.get("status"),
                "effective_status": ad.get("effective_status"),
                "account_id": account_id,
                "account_name": account_name,
            }

            results.append(row)

        except Exception:

            continue

    return results