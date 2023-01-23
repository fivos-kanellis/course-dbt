{{
  config(
    materialized='table'
  )
}}

SELECT user_id
    ,session_id
    ,sum(case when event_type = 'checkout' then 1 else 0 end) as cnt_checkout
    ,sum(case when event_type = 'package_shipped' then 1 else 0 end) as cnt_package_shipped
    ,sum(case when event_type = 'add_to_cart' then 1 else 0 end) as cnt_add_to_cart
    ,sum(case when event_type = 'page_view' then 1 else 0 end) as cnt_page_views
    ,min(created_at) as first_session_event_at
    ,max(created_at) as last_session_event_at
    
FROM {{ source('postgres', 'events') }}
GROUP BY 1,2