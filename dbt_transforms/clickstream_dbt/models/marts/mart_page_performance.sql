with page_traffic as (
    select * from {{ ref('stg_page_traffic') }}
),

aggregated as (
    select
        page_url,
        event_type,
        device_type,
        sum(event_count)                        as total_events,
        round(avg(avg_time_spent_seconds), 2)   as avg_time_spent_seconds,
        max(created_at)                         as last_updated
    from page_traffic
    group by page_url, event_type, device_type
)

select * from aggregated
order by total_events desc