from facebook_business.adobjects.adaccount import AdAccount

def extract_ad_insights(
    account_id: str,
    since: str,
    until: str
) -> list[dict]:

    fields = [
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "impressions",
        "clicks",
        "spend",
        "optimization_goal",
        "actions",
        "date_start",
        "date_stop",
    ]

    params = {
        "level": "ad",
        "time_increment": 1,
        "time_range": {"since": since, "until": until},
    }

    insights = AdAccount(account_id).get_insights(
        fields=fields,
        params=params
    )

    return [dict(row) for row in insights]    