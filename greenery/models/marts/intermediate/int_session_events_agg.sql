{{
  config(
    materialized='table'
  )
}}

{%-
  set event_types = dbt_utils.get_column_values(
    table = ref('stg_postgres_events')
    , column = 'event_type'
    , order_by = 'event_type asc')
  
  -%}


with tmp_events as 
(SELECT * 
FROM {{ ref('stg_postgres_events') }}
)

,converted_sessions as 
(SELECT DISTINCT session_id
,order_id
,TRUE as is_converted_session
FROM tmp_events e
WHERE order_id IS NOT NULL
)

SELECT e.user_id
    ,e.session_id
    ,coalesce(c.is_converted_session, FALSE) as is_converted_session
    ,c.order_id as converted_session_order_id
    {%- for event_type in event_types %}
    , sum(case when event_type = '{{ event_type }}' then 1 else 0 end) as cnt_{{ event_type }}
     {%- endfor %}
    --,sum(case when e.event_type = 'checkout' then 1 else 0 end) as cnt_checkout
    --,sum(case when e.event_type = 'package_shipped' then 1 else 0 end) as cnt_package_shipped
    --,sum(case when e.event_type = 'add_to_cart' then 1 else 0 end) as cnt_add_to_cart
    --,sum(case when e.event_type = 'page_view' then 1 else 0 end) as cnt_page_views
    ,min(e.created_at) as first_session_event_at
    ,max(e.created_at) as last_session_event_at
    ,datediff('minutes', min(e.created_at),max(e.created_at)) as session_duration_minutes
    
FROM tmp_events e
LEFT JOIN converted_sessions c on c.session_id = e.session_id
GROUP BY 1,2,3,4

 