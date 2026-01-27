{{ config(materialized='ephemeral') }}

select
    date(i.date) as date,

    i.account_id,
    i.campaign_id,

    m.campaign_name,

    case
        when m.campaign_status = 'ACTIVE'   then '🟢'
        when m.campaign_status = 'PAUSED'   then '⚪'
        when m.campaign_status in ('ARCHIVED', 'DELETED') then '🔴'
        else '❓'
    end as campaign_status,

    m.platform,
    m.objective,
    m.budget_group,
    m.region,
    m.category_level_1,
    m.track_group,
    m.pillar_group,
    m.content_group,

    i.impressions,
    i.clicks,
    i.spend,

    i.result,
    i.result_type,

    i.messaging_conversations_started,
    i.purchase

from {{ ref('stg_campaign_insights') }} i
left join `{{ target.project }}.{{ var('company') }}_dataset_google_api_raw.{{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` m
    on i.account_id = m.account_id
   and i.campaign_id = m.campaign_id