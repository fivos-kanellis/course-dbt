
{% macro get_event_types() %}
{ % SET agg_event_types % }

SELECT DISTINCT event_type
FROM {{ ref('STG_POSTGRES_EVENTS') }}
ORDER BY 1 { % endset % }

{% set results = run_query(agg_event_types) %}

{% if execute %}
-- Return the first column 
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}