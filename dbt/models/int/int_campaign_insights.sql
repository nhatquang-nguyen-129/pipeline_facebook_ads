{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'facebook', 'campaign']
  ) 
}}

select
    date(insights.date) as date,

    insights.department,
    insights.account,
    insights.account_id,
    insights.campaign_id,

    campaign.campaign_name,

    case
        when campaign.campaign_status = 'ACTIVE'                    then '🟢'
        when campaign.campaign_status = 'PAUSED'                    then '⚪'
        when campaign.campaign_status in ('ARCHIVED', 'DELETED')    then '🔴'
        else '❓'
    end as campaign_status,

    campaign.platform,
    campaign.objective,
    campaign.budget_group,
    campaign.region,
    campaign.category_level_1,
    campaign.track_group,
    campaign.pillar_group,
    campaign.content_group,

    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,
    insights.result_type,

    insights.messaging_conversations_started,
    insights.purchase

from {{ ref('stg_campaign_insights') }} i
left join `{{ target.project }}.{{ var('company') }}_dataset_google_api_raw.{{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` m
    on insights.account_id = campaign.account_id
   and insights.campaign_id = campaign.campaign_id