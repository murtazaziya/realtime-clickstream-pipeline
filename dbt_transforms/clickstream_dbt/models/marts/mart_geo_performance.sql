with country_traffic as (
    select * from {{ ref('stg_country_traffic') }}
),

aggregated as (
    select
        country,
        city,
        sum(event_count)                        as total_events,
        sum(conversion_count)                   as total_conversions,
        round(
            sum(conversion_count)::numeric
            / nullif(sum(event_count), 0) * 100, 2
        )                                       as conversion_rate_pct,
        max(created_at)                         as last_updated
    from country_traffic
    group by country, city
)

select * from aggregated
order by total_events desc