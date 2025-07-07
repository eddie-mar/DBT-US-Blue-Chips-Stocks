{{
    config(
        materialized='view'
    )
}}

with yrly_data as (
    select *,
        avg(avg_monthly) as avg_yearly
    from {{ ref('monthly_analysis') }}
    group by ticker, year
),
prev_year as (
    select ticker, year, avg_yearly,
        lag(avg_yearly, 1) over (partition by ticker order by year ASC) as avg_prev_year
    from yrly_data
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
order by year desc, avg_yearly desc