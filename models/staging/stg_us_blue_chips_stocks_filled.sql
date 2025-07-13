{{
    config(
        materialized='view'
    )
}}

with calendar as (
    select 
        date '2000-01-03' + interval n day as price_date
    from unnest(generate_array(0, date_diff('2024-12-31', '2000-01-03', day))) as n
),
tickers as (
    select distinct ticker from {{ ref('stg_us_blue_chips_stocks') }}
),
calendar_tickers as (
    select
        c.price_date,
        t.ticker
    from calendar c cross join tickers t
),
data as (
    select
        price_date,
        ticker,
        open_price,
        highest_price,
        lowest_price,
        closing_price,
        volume
    from {{ ref('stg_us_blue_chips_stocks') }}
),
filled as (
    select
        {{ dbt_utils.generate_surrogate_key(['ct.price_date', 'ct.ticker']) }} as stockid,
        cast(ct.price_date as date) as price_date,
        ct.ticker,
        last_value(open_price ignore nulls) over (
            partition by ct.ticker order by ct.price_date
            rows between unbounded preceding and current row
        ) as open_price,
        last_value(highest_price ignore nulls) over (
            partition by ct.ticker order by ct.price_date
            rows between unbounded preceding and current row
        ) as highest_price,
        last_value(lowest_price ignore nulls) over (
            partition by ct.ticker order by ct.price_date
            rows between unbounded preceding and current row
        ) as lowest_price,
        last_value(closing_price ignore nulls) over (
            partition by ct.ticker order by ct.price_date
            rows between unbounded preceding and current row
        ) as closing_price,
        last_value(volume ignore nulls) over (
            partition by ct.ticker order by ct.price_date
            rows between unbounded preceding and current row
        ) as volume,
    from calendar_tickers ct left join data d
    on ct.ticker=d.ticker and ct.price_date=d.price_date
)
select * from filled








