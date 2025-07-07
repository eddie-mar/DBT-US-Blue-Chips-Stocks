{#
    This macro returns month's name
#}

{% macro get_month(date) %}
    case extract(month from {{date}})
        when 1 then 'JAN'
        when 2 then 'FEB'
        when 3 then 'MAR'
        when 4 then 'APR'
        when 5 then 'MAY'
        when 6 then 'JUNE'
        when 7 then 'JUL'
        when 8 then 'AUG'
        when 9 then 'SEP'
        when 10 then 'OCT'
        when 11 then 'NOV'
        when 12 then 'DEC'
        else NULL
    end

{%- endmacro%}
