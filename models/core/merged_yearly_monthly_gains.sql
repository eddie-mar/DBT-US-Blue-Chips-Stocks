{{
    config(
        materialized='table'
    )
}}

with monthly as (
    select * from monthly_analysis
),
yearly as (
    select * from yearly_analysis
)

select monthly.year,
    monthly.month,
    monthly.ticker,
    monthly.avg_monthly,
    monthly.avg_prev_month,
    monthly.monthly_gain,
    yearly.avg_yearly,
    yearly.avg_prev_year,
    yearly.yearly_gain

from monthly inner join yearly
on monthly.year = yearly.year
order by year desc, month desc, monthly_gain desc