with source as (
    select * from public.country_traffic
),

cleaned as (
    select
        id,
        country,
        city,
        event_count,
        conversion_count,
        case
            when event_count > 0
            then round((conversion_count::numeric / event_count) * 100, 2)
            else 0
        end as conversion_rate_pct,
        window_start,
        window_end,
        created_at
    from source
    where country is not null
)

select * from cleaned