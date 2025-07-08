{{
    config(
        materialized='view'
    )
}}

with yrly_data as (
    select *, 
        extract(year from price_date) as year
    from {{ ref('fact_us_blue_chips') }}
    where extract(year from price_date) >= 2020 and price_date is not null
),
avg_yrly_data as (
    select ticker, year,
        avg(closing_price) as avg_yearly
    from yrly_data
    group by 1, 2
),
prev_year as (
    select ticker, year, avg_yearly,
        lag(avg_yearly, 1) over (partition by ticker order by year ASC) as avg_prev_year
    from avg_yrly_data
),
gained_yearly as (
    select ticker, year, avg_yearly, avg_prev_year,
        case
            when avg_prev_year is null then null
            else ((avg_yearly - avg_prev_year) / avg_prev_year) * 100
        end as yearly_gain
    from prev_year
)

select year, ticker, avg_yearly, avg_prev_year, yearly_gain
from gained_yearly
order by year desc, yearly_gain desc