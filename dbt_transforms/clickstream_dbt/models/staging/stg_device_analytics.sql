with source as (
    select * from public.device_analytics
),

cleaned as (
    select
        id,
        device_type,
        browser,
        event_count,
        conversion_count,
        round(avg_time_spent::numeric, 2) as avg_time_spent_seconds,
        case
            when event_count > 0
            then round((conversion_count::numeric / event_count) * 100, 2)
            else 0
        end as conversion_rate_pct,
        window_start,
        window_end,
        created_at
    from source
    where device_type is not null
)

select * from cleaned