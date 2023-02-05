{{
  config(
    materialized='table'
  )
}}


{%-
  set order_status = dbt_utils.get_column_values(
    table = ref('stg_postgres_orders')
    , column = 'order_status'
    , order_by = 'order_status asc')
  
  -%}



WITH tmp_users as
(SELECT *
 FROM {{ ref('stg_postgres_users') }}
)
,tmp_order_items as 
(SELECT *
 FROM {{ ref('stg_postgres_order_items') }}
)
,tmp_orders as 
(SELECT *
 FROM {{ ref('stg_postgres_orders') }}
)

SELECT
    u.user_id
    ,count(distinct o.address_id) as total_user_address_cnt
    ,min(o.created_at) as first_order_dt
    ,max(o.created_at) as last_order_dt
    ,max(case when o.order_status = 'delivered' then o.created_at end) as last_delivered_order_dt
    ,max(case when o.order_status = 'shipped' then o.created_at end) as last_shipped_order_dt
    ,count(distinct o.order_id) as orders_total
    ,orders_total>0 as has_ordered
    ,orders_total>1 as repeat_customer
    ,count(distinct case when o.address_id = u.address_id then o.order_id end) as orders_at_user_address
    ,count(distinct case when o.address_id <> u.address_id then o.order_id end) as orders_at_other_address  
    {%- for event_type in event_types %}
    , count(distinct case when o.order_status = '{{ order_status }}' o.order_id end) as orders_{{ order_type }}
     {%- endfor %}
    --,count(distinct case when o.order_status = 'delivered' then o.order_id end) as orders_delivered
    --,count(distinct case when o.order_status = 'shipped' then o.order_id end) as orders_shipped
    --,count(distinct case when o.order_status = 'preparing' then o.order_id end) as orders_preparing
    ,count(distinct case when o.order_status = 'shipped' and estimated_delivery_at < delivered_at then o.order_id end) as orders_shipped_est_late
    ,count(distinct case when o.order_status = 'delivered' and estimated_delivery_at < delivered_at then o.order_id end) as orders_delivered_late
    ,count(distinct case when o.shipping_service = 'ups' then o.order_id end) as orders_via_ups_total
    ,count(distinct case when o.shipping_service = 'ups' and o.order_status = 'delivered' then o.order_id end) as orders_via_ups_delivered
    ,count(distinct case when o.shipping_service = 'ups' and o.order_status = 'shipped' then o.order_id end) as orders_via_ups_shipped
    ,count(distinct case when o.shipping_service = 'fedex' then o.order_id end) as orders_via_fedex_total
    ,count(distinct case when o.shipping_service = 'fedex' and o.order_status = 'delivered' then o.order_id end) as orders_via_fedex_delivered
    ,count(distinct case when o.shipping_service = 'fedex' and o.order_status = 'shipped' then o.order_id end) as orders_via_fedex_shipped
    ,count(distinct case when o.shipping_service = 'usps' then o.order_id end) as orders_via_usps_total
    ,count(distinct case when o.shipping_service = 'usps' and o.order_status = 'delivered' then o.order_id end) as orders_via_usps_delivered
    ,count(distinct case when o.shipping_service = 'usps' and o.order_status = 'shipped' then o.order_id end) as orders_via_usps_shipped
    ,count(distinct case when o.shipping_service = 'dhl' then o.order_id end) as orders_via_dhl_total
    ,count(distinct case when o.shipping_service = 'dhl' and o.order_status = 'delivered' then o.order_id end) as orders_via_dhl_delivered
    ,count(distinct case when o.shipping_service = 'dhl' and o.order_status = 'shipped' then o.order_id end) as orders_via_dhl_shipped
    ,sum(o.order_cost) as total_order_cost_amt
    ,sum(o.shipping_cost) as total_shipping_cost_amt
    ,sum(o.order_total) as total_order_amt
    ,sum(o.order_total)/count(distinct o.order_id) as aov
    ,count(distinct oi.product_id) as unique_products_ordered

  FROM tmp_users u
  LEFT JOIN tmp_orders o on o.user_id = u.user_id
  LEFT JOIN tmp_order_items oi on oi.order_id = o.order_id 


GROUP BY u.user_id




