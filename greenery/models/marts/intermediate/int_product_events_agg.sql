
{{
  config(
    materialized='table'
  )
}}


SELECT product_id
      ,count(distinct case when event_type = 'page_view' then event_id end) as page_views
      ,count(distinct case when event_type = 'page_view' then user_id end) as unique_user_page_views
      ,count(distinct case when event_type = 'add_to_cart' then event_id end) as added_to_cart
      ,count(distinct case when event_type = 'add_to_cart' then user_id end) as unique_users_added_to_cart
FROM {{ source('postgres', 'events') }}
GROUP BY 1