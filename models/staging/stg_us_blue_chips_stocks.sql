{{
    config(
        materialized='view'
    )
}}

with data as (
    select *,
    row_number() over (partition by Symbol, Date) as rn
    from {{ source('staging', 'stg_us_blue_chips') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['Date', 'Symbol'])}} as stockid,
    cast(Date as date) as price_date,
    Symbol as ticker,
    cast(Open as numeric) as open_price,
    cast(High as numeric) as highest_price,
    cast(Low as numeric) as lowest_price,
    cast(Close as numeric) as closing_price,
    {{dbt.safe_cast('Volume', api.Column.translate_type('bigint'))}} as volume

from data
where rn = 1 and Open > 0 and High > 0 and Low > 0 and Close > 0