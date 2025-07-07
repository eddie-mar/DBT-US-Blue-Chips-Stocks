{{
    config(
        materialized='table'
    )
}}
select ticker, name from {{ ref('company_metadata') }}
