{{
  config(
    materialized='table'
  )
}}

with user_sessions_agg as (
  SELECT user_id
    ,count(distinct session_id) as unique_sessions
    ,sum(cnt_checkout) as cnt_checkout
    ,sum(cnt_package_shipped) as cnt_package_shipped
    ,sum(cnt_add_to_cart) as cnt_add_to_cart
    ,sum(cnt_page_views) as cnt_page_views
    ,min(first_session_event_at) as first_session_at
    ,max(last_session_event_at) as last_session_at
  from {{ ref('int_session_events_agg')}}
  GROUP BY 1
)

,user_orders_agg as (
  select * from {{ ref('int_user_level_orders')}}
)

,users as (
  select * from {{ ref('stg_postgres_users')}}
)

SELECT u.user_id
    ,s.unique_sessions
    ,s.cnt_checkout
    ,s.cnt_package_shipped
    ,s.cnt_add_to_cart
    ,s.cnt_page_views
    ,s.first_session_at
    ,s.last_session_at
    ,o.total_user_address_cnt
    ,o.first_order_dt
    ,o.last_order_dt
    ,o.last_delivered_order_dt
    ,o.last_shipped_order_dt
    ,o.orders_total
    ,o.has_ordered
    ,o.repeat_customer
    ,o.orders_at_user_address
    ,o.orders_at_other_address
    ,o.orders_delivered
    ,o.orders_shipped
    ,o.orders_preparing
    ,o.orders_shipped_est_late
    ,o.orders_delivered_late
    ,o.orders_via_ups_total
    ,o.orders_via_ups_delivered
    ,o.orders_via_ups_shipped
    ,o.orders_via_fedex_total
    ,o.orders_via_fedex_delivered
    ,o.orders_via_fedex_shipped
    ,o.orders_via_usps_total
    ,o.orders_via_usps_delivered
    ,o.orders_via_usps_shipped
    ,o.orders_via_dhl_total
    ,o.orders_via_dhl_delivered
    ,o.orders_via_dhl_shipped
    ,o.total_order_cost_amt
    ,o.total_shipping_cost_amt
    ,o.total_order_amt
    ,o.aov
    ,o.unique_products_ordered
    
FROM users u
left join user_sessions_agg s on s.user_id = u.user_id
left join user_orders_agg o on o.user_id = u.user_id