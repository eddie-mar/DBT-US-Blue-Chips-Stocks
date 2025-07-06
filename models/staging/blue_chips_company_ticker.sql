{{
    config(
        materialized='table'
    )
}}

select * from {{ source('staging', 'blue_chips_company') }}