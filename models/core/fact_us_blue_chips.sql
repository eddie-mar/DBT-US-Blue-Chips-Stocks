{{
    config(
        materialized='table'
    )
}}

with stock_data as (
    select *,
    lag(close) over (partition by ticker order by price_date) as prev_close
    from {{ ref('stg_us_blue_chips_stocks') }}
),
company_data as (
    select * from {{ ref('blue_chips_company_ticker') }}
)
..