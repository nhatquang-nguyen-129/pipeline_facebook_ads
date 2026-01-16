from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative

def extract_ad_creative(ad_ids: list[str]) -> list[dict]:

    if not ad_ids:
        return []

    results: list[dict] = []

    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(fields=["creative"])
            creative_id = ad.get("creative", {}).get("id")

            if not creative_id:
                continue

            creative = AdCreative(creative_id).api_get(
                fields=["thumbnail_url"]
            )

            row = {
                "ad_id": ad_id,
                "creative_id": creative_id,
                "thumbnail_url": creative.get("thumbnail_url"),
            }

            results.append(row)

        except Exception:

            continue

    return results