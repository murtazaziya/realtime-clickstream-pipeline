with source as (
    select * from public.raw_events
),

cleaned as (
    select
        event_id,
        user_id,
        session_id,
        event_timestamp,
        page_url,
        referrer_url,
        event_type,
        device_type,
        browser,
        country,
        city,
        product_id,
        product_category,
        time_spent_seconds,
        is_product_page,
        is_conversion,
        hour_of_day,
        day_of_week,
        ingested_at,
        case day_of_week
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as day_name
    from source
    where event_id is not null
      and event_timestamp is not null
)

select * from cleaned