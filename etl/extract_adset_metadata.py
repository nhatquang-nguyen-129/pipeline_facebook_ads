from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccount import AdAccount

def extract_adset_metadata(
    adset_ids: list[str],
    account_id: str,
) -> list[dict]:

    if not adset_ids:
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
        "status",
        "effective_status",
        "campaign_id",
    ]

    results: list[dict] = []

    for adset_id in adset_ids:
        try:
            adset = AdSet(adset_id).api_get(fields=fields)

            row = {
                "adset_id": adset.get("id"),
                "adset_name": adset.get("name"),
                "status": adset.get("status"),
                "effective_status": adset.get("effective_status"),
                "campaign_id": adset.get("campaign_id"),
                "account_id": account_id,
                "account_name": account_name,
            }

            results.append(row)

        except Exception:
        
            continue

    return results
