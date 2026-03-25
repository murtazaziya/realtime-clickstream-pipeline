with device_analytics as (
    select * from {{ ref('stg_device_analytics') }}
),

aggregated as (
    select
        device_type,
        browser,
        sum(event_count)                        as total_events,
        sum(conversion_count)                   as total_conversions,
        round(avg(avg_time_spent_seconds), 2)   as avg_time_spent_seconds,
        round(
            sum(conversion_count)::numeric
            / nullif(sum(event_count), 0) * 100, 2
        )                                       as conversion_rate_pct,
        max(created_at)                         as last_updated
    from device_analytics
    group by device_type, browser
)

select * from aggregated
order by total_events desc