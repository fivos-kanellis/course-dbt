
{% macro get_order_status() %}
{ % SET agg_order_status_count % }

SELECT DISTINCT order_status
FROM {{ ref('STG_POSTGRES_ORDERS') }}
ORDER BY 1 { % endset % }

{% set results = run_query(agg_order_status_count) %}

{% if execute %}
-- Return the first column 
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}

