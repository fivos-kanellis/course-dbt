{{
  config(
    materialized='table'
  )
}}


SELECT
    u.user_id
    ,count(distinct o.address_id) as total_user_address_cnt
    ,min(o.created_at) as first_order_dt
    ,max(o.created_at) as last_order_dt
    ,max(case when o.status = 'delivered' then o.created_at end) as last_delivered_order_dt
    ,max(case when o.status = 'shipped' then o.created_at end) as last_shipped_order_dt
    ,count(distinct o.order_id) as orders_total
    ,orders_total>0 as has_ordered
    ,orders_total>1 as repeat_customer
    ,count(distinct case when o.address_id = u.address_id then o.order_id end) as orders_at_user_address
    ,count(distinct case when o.address_id <> u.address_id then o.order_id end) as orders_at_other_address  
    ,count(distinct case when o.status = 'delivered' then o.order_id end) as orders_delivered
    ,count(distinct case when o.status = 'shipped' then o.order_id end) as orders_shipped
    ,count(distinct case when o.status = 'preparing' then o.order_id end) as orders_preparing
    ,count(distinct case when o.status = 'shipped' and estimated_delivery_at < delivered_at then o.order_id end) as orders_shipped_est_late
    ,count(distinct case when o.status = 'delivered' and estimated_delivery_at < delivered_at then o.order_id end) as orders_delivered_late
    ,count(distinct case when o.shipping_service = 'ups' then o.order_id end) as orders_via_ups_total
    ,count(distinct case when o.shipping_service = 'ups' and o.status = 'delivered' then o.order_id end) as orders_via_ups_delivered
    ,count(distinct case when o.shipping_service = 'ups' and o.status = 'shipped' then o.order_id end) as orders_via_ups_shipped
    ,count(distinct case when o.shipping_service = 'fedex' then o.order_id end) as orders_via_fedex_total
    ,count(distinct case when o.shipping_service = 'fedex' and o.status = 'delivered' then o.order_id end) as orders_via_fedex_delivered
    ,count(distinct case when o.shipping_service = 'fedex' and o.status = 'shipped' then o.order_id end) as orders_via_fedex_shipped
    ,count(distinct case when o.shipping_service = 'usps' then o.order_id end) as orders_via_usps_total
    ,count(distinct case when o.shipping_service = 'usps' and o.status = 'delivered' then o.order_id end) as orders_via_usps_delivered
    ,count(distinct case when o.shipping_service = 'usps' and o.status = 'shipped' then o.order_id end) as orders_via_usps_shipped
    ,count(distinct case when o.shipping_service = 'dhl' then o.order_id end) as orders_via_dhl_total
    ,count(distinct case when o.shipping_service = 'dhl' and o.status = 'delivered' then o.order_id end) as orders_via_dhl_delivered
    ,count(distinct case when o.shipping_service = 'dhl' and o.status = 'shipped' then o.order_id end) as orders_via_dhl_shipped
    ,sum(o.order_cost) as total_order_cost_amt
    ,sum(o.shipping_cost) as total_shipping_cost_amt
    ,sum(o.order_total) as total_order_amt
    ,sum(o.order_total)/count(distinct o.order_id) as aov
    ,count(distinct oi.product_id) as unique_products_ordered

  FROM {{ source('postgres', 'users') }} u
  LEFT JOIN {{ source('postgres', 'orders') }} o on o.user_id = u.user_id
  LEFT JOIN {{ source('postgres', 'order_items') }} oi on oi.order_id = o.order_id 


GROUP BY u.user_id