{{
    config(
        materialized='view'
    )
}}

with wth_month_data as (
    select *,
        extract(year from price_date) as year,
        extract(month from price_date) as month
    from {{ ref('fact_us_blue_chips') }}
    where extract(year from price_date) >= 2020 and price_date is not null
),
avg_monthly_data as (
    select ticker,
        year, month, 
        avg(closing_price) as avg_monthly
    from wth_month_data
    group by 1, 2, 3
),
wth_prev_month as (
    select ticker, year, month, avg_monthly,
        lag(avg_monthly, 1) over (partition by ticker order by year ASC, month ASC) as avg_prev_month
    from avg_monthly_data
),
gained_monthly as (
    select ticker, year, month, avg_monthly, avg_prev_month,
        case
            when avg_prev_month is null then null
            else ((avg_monthly - avg_prev_month)/avg_prev_month) * 100
        end as monthly_gain
    from wth_prev_month
)

select 
    year, month, ticker, avg_monthly, avg_prev_month, monthly_gain
from gained_monthly
order by year desc, month desc, monthly_gain desc