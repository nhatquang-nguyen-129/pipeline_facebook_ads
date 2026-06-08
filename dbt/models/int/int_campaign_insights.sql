{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'facebook', 'campaign']
  ) 
}}

select
    date,
    month,
    year,

    insights.department,
    insights.account,
    insights.account_id,
    insights.campaign_id,

    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,
    insights.result_type,

    insights.messaging_conversations_started,
    insights.purchase,    

    campaign.campaign_name,
    campaign.account_name,

    case
        when campaign.status = 'ACTIVE'                    then '🟢'
        when campaign.status = 'PAUSED'                    then '⚪'
        when campaign.status in ('ARCHIVED', 'DELETED')    then '🔴'
        else '❓'
    end as campaign_status,

    campaign.platform,
    campaign.objective,
    campaign.budget_group,
    campaign.region,
    campaign.category_level_1,
    campaign.optimization,
    campaign.track,
    campaign.pillar,
    campaign.`group`

from {{ ref('stg_campaign_insights') }} insights
left join `{{ target.project }}.{{ var('company') }}_dataset_facebook_api_raw.{{ var('company') }}_table_facebook_{{ var('department') }}_{{ var('account') }}_campaign_metadata` campaign
    on insights.account_id = campaign.account_id
   and insights.campaign_id = campaign.campaign_id