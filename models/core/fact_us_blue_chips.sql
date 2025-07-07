{{
    config(
        materialized='table'
    )
}}

with stock_data as (
    select *,
    lag(closing_price) over (partition by ticker order by price_date) as prev_close
    from {{ ref('stg_us_blue_chips_stocks') }}
),
company_data as (
    select * from {{ ref('dim_companies') }}
)

select stock_data.stockid,
    stock_data.price_date,
    stock_data.ticker,
    company_data.name as company,
    stock_data.open_price,
    stock_data.highest_price,
    stock_data.lowest_price,
    stock_data.closing_price,
    stock_data.prev_close as prev_closing,
    stock_data.volume,
    (stock_data.volume * stock_data.closing_price) as value_traded,
    ((stock_data.closing_price - stock_data.prev_close)/ stock_data.prev_close) * 100 as daily_change,
    avg(stock_data.closing_price) over (partition by stock_data.ticker order by stock_data.price_date rows between 6 preceding and current row) as moving_avg_7d,
    avg(stock_data.closing_price) over (partition by stock_data.ticker order by stock_data.price_date rows between 29 preceding and current row) as moving_avg_30d,
    stddev_pop(stock_data.closing_price) over (partition by stock_data.ticker order by stock_data.price_date rows between 6 preceding and current row) as volatility_7d,
    stddev_pop(stock_data.closing_price) over (partition by stock_data.ticker order by stock_data.price_date rows between 29 preceding and current row) as volatility_30d

from stock_data join company_data
on stock_data.ticker = company_data.ticker
