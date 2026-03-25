with events as (
    select * from {{ ref('stg_raw_events') }}
),

funnel as (
    select
        page_url,
        event_type,
        count(distinct user_id)         as unique_users,
        count(distinct session_id)      as unique_sessions,
        count(*)                        as total_events,
        sum(case when is_conversion
            then 1 else 0 end)          as total_conversions,
        round(
            sum(case when is_conversion then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100, 2
        )                               as conversion_rate_pct,
        round(avg(time_spent_seconds), 2) as avg_time_spent_seconds
    from events
    group by page_url, event_type
)

select * from funnel
order by total_events desc