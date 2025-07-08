{{
    config(
        materialized='view'
    )
}}

select ticker,
    price_date,
    closing_price,
    moving_avg_7d,
    case
        when closing_price > moving_avg_7d then true
        else false
    end as above_ma_7,
    moving_avg_30d,
    case when closing_price > moving_avg_30d then true
        else false
    end as above_ma_30

from {{ ref('fact_us_blue_chips') }}
where extract(year from price_date) >= 2020 and price_date is not null
order by ticker, price_date