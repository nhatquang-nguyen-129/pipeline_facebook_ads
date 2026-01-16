from facebook_business.adobjects.adaccount import AdAccount

def extract_campaign_insights(
    account_id: str,
    start_date: str,
    end_date: str
) -> list[dict]:

    fields = [
        "date_start",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "actions"
    ]

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "campaign"
    }

    insights = AdAccount(account_id).get_insights(
        fields=fields,
        params=params
    )

    return [dict(row) for row in insights]
