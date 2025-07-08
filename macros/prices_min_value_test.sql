{#
    This generates min_value test for prices
#}

{% macro gen_prices_test(list)%}
    
{% for price in list %}
- name: {{ price }}
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 1000000
        strictly: true
{% endfor %}

{%- endmacro %}