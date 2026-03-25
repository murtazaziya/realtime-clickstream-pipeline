with source as (
    select * from public.page_traffic
),

cleaned as (
    select
        id,
        page_url,
        event_type,
        device_type,
        event_count,
        round(avg_time_spent::numeric, 2) as avg_time_spent_seconds,
        window_start,
        window_end,
        created_at
    from source
    where page_url is not null
)

select * from cleaned